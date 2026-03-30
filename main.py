import telebot
from telebot import types
import pandas as pd
import time
import os
import threading
import requests
import io
import re
from neo_api_client import NeoAPI
from datetime import datetime, timedelta
from pymongo import MongoClient
import google.generativeai as genai
from http.server import BaseHTTPRequestHandler, HTTPServer

# =========================================
# --- CONFIGURATION & MONGODB & AI ---
# =========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel('gemini-2.5-flash') 
else:
    print("⚠️ GEMINI_API_KEY not found! AI Analysis will not work.")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["tradingbot"]

users_col = db["users"]
trades_col = db["trades"]
fo_master_col = db["fo_master"]
analysis_col = db["analysis_history"]

INDICES_CONFIG = {
    "NIFTY": {
        "Exchange": "nse_fo", "SpotExchange": "nse_cm", "SpotToken": "256265", 
        "LotSize": 25, "StrikeGap": 50
    },
    "SENSEX": {
        "Exchange": "bse_fo", "SpotExchange": "bse_cm", "SpotToken": "1", 
        "LotSize": 10, "StrikeGap": 100
    }
}

USER_SESSIONS, USER_DETAILS, USER_SETTINGS = {}, {}, {}
USER_STATE, PENDING_TRADE, ACTIVE_TOKENS, TEMP_REG_DATA = {}, {}, {}, {}

bot = telebot.TeleBot(BOT_TOKEN)

# =========================================
# --- HELPER FUNCTIONS ---
# =========================================
def load_users():
    try:
        USER_DETAILS.clear()
        for row in users_col.find():
            cid = int(row.get('ChatID', 0))
            if cid == 0: continue
            USER_DETAILS[cid] = {
                "Name": row.get('Name', ''), "Key": row.get('Key', row.get('ConsumerKey', '')), 
                "Mobile": row.get('Mobile', ''), "UCC": row.get('UCC', ''), "MPIN": row.get('MPIN', '')
            }
            if cid not in USER_SETTINGS: 
                USER_SETTINGS[cid] = {"Index": "NIFTY", "ATM": None}
    except Exception as e: print(f"Load Error: {e}")
load_users()

def save_new_user(cid, data):
    new_row = {"ChatID": str(cid), "Name": data.get("Name", ""), "Key": data.get("Key", ""), "Mobile": data.get("Mobile", ""), "UCC": data.get("UCC", ""), "MPIN": data.get("MPIN", "")}
    try:
        users_col.insert_one(new_row)
        USER_DETAILS[cid] = new_row
        USER_SETTINGS[cid] = {"Index": "NIFTY", "ATM": None}
        return True
    except: return False

def log_trade(cid, idx_name, trade_symbol, token, opt_type, side, qty, price, order_id):
    new_row = {
        "ChatID": str(cid), "Index": idx_name, "Date": datetime.now().strftime("%Y-%m-%d"), "Time": datetime.now().strftime("%H:%M:%S"),
        "TradeSymbol": trade_symbol, "Token": token, "Type": opt_type, "Side": side,
        "Qty": int(qty), "EntryPrice": price, "ExitPrice": 0, "Status": "OPEN", "OrderID": str(order_id), "SLOrderID": "", "SLPrice": 0
    }
    try: trades_col.insert_one(new_row)
    except: pass

def format_crore_lakh(number):
    val = abs(number)
    if val >= 10000000: return f"{number / 10000000:.2f} Cr"
    elif val >= 100000: return f"{number / 100000:.2f} L"
    else: return f"{number:,.0f}"

def place_marketable_limit(client, conf, qty, symbol, side, ltp):
    """SEBI compliant Marketable Limit Order wrapper"""
    buffer = 5.0
    if side.upper() in ["B", "BUY"]:
        limit_prc = round(ltp + buffer, 1)
        t_type = "B"
    else:
        limit_prc = round(max(0.1, ltp - buffer), 1)
        t_type = "S"
    return client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(limit_prc), order_type="L", quantity=str(qty), validity="DAY", trading_symbol=symbol, transaction_type=t_type, amo="NO")

# =========================================
# --- AI & DATA ENGINE ---
# =========================================
def get_ai_analysis(cid):
    if not GEMINI_API_KEY: return "❌ Gemini API Key missing."
    if cid not in ACTIVE_TOKENS or not ACTIVE_TOKENS[cid]: return "❌ Data not loaded. Refresh first."
        
    try:
        idx_name = USER_SETTINGS[cid]["Index"]
        atm = float(USER_SETTINGS[cid].get("ATM", 0))
        df = pd.DataFrame(ACTIVE_TOKENS[cid])
        
        df['OI'] = df['OI'].fillna(0).astype(int)
        df['OI_Change'] = df['OI_Change'].fillna(0).astype(int)
        
        overall_ce_oi = int(df[df['Type'] == 'CE']['OI'].sum())
        overall_pe_oi = int(df[df['Type'] == 'PE']['OI'].sum())
        overall_pcr = round(overall_pe_oi / overall_ce_oi, 2) if overall_ce_oi > 0 else 0
        
        gap = INDICES_CONFIG[idx_name]["StrikeGap"]
        ce_otm_strikes = [atm + (i * gap) for i in range(5)]
        pe_otm_strikes = [atm - (i * gap) for i in range(5)]
        
        ce_otm_df = df[(df['Type'] == 'CE') & (df['Strike'].isin(ce_otm_strikes))]
        pe_otm_df = df[(df['Type'] == 'PE') & (df['Strike'].isin(pe_otm_strikes))]
        
        otm_ce_oi = int(ce_otm_df['OI'].sum())
        otm_pe_oi = int(pe_otm_df['OI'].sum())
        otm_ce_chg = int(ce_otm_df['OI_Change'].sum())
        otm_pe_chg = int(pe_otm_df['OI_Change'].sum())
        otm_pcr = round(otm_pe_oi / otm_ce_oi, 2) if otm_ce_oi > 0 else 0
        
        safe_side = "CE (Resistance strong, Call sell safe)" if otm_ce_oi > otm_pe_oi else "PE (Support strong, Put sell safe)"

        prompt = f"""
        Aap ek ruthless Institutional Quant Analyst hain jo Option Seller ko commands deta hai.
        
        **Live Data for {idx_name}:**
        - Current ATM: {atm}
        - MACRO Trend (Full Day PCR): {overall_pcr} | MICRO Trend (ATM+4 OTM PCR): {otm_pcr}
        
        **Active Zone (ATM + 4 OTM):**
        - Total OTM Call OI: {otm_ce_oi} | Fresh Call OI Change (Today): {otm_ce_chg}
        - Total OTM Put OI: {otm_pe_oi} | Fresh Put OI Change (Today): {otm_pe_chg}
        - Safest Side to Sell Options based on higher OI: {safe_side}
        
        **CRITICAL INSTRUCTIONS:**
        1. Pick ONE primary bias. No diplomatic answers.
        2. Strictly use the rule: "Jis side jyada OI aur OI Change buildup hai, wo side sell karna safe hota hai". 
        3. Formatted strictly in Hinglish.
        
        🧠 **Quant Reasoning & Trap Zone:** [Explain PCR, OTM shifts, and OI Change Smart Money movement]
        🎯 **Definitive Bias:** [Strong Bearish / Strong Bullish / Pure Sideways]
        ⚡ **Trade Execution Command:** [Clear command what to short]
        """
        response = ai_model.generate_content(prompt)
        return response.text
    except Exception as e: return f"❌ AI Analysis failed: {e}"

def auto_generate_chain(cid):
    idx_name = USER_SETTINGS[cid]["Index"]
    conf = INDICES_CONFIG[idx_name]
    if cid not in USER_SESSIONS: return False, "❌ No Session Active."
    client = USER_SESSIONS[cid]
    now = datetime.now()
    
    try:
        spot_token = conf["SpotToken"]
        spot_exch = conf["SpotExchange"]
        q = client.quotes(instrument_tokens=[{"instrument_token": spot_token, "exchange_segment": spot_exch}], quote_type="all")
        
        base_ltp = 0
        if q:
            item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
            base_ltp = float(item.get('ltp', item.get('lastPrice', 0)))

        # Fallback to Futures if Spot is 0 (Fixes API Spot Issues)
        if base_ltp == 0:
            fut_cursor = fo_master_col.find({"IndexName": idx_name, "5": {"$regex": "FUT"}})
            fut_list = list(fut_cursor)
            if fut_list:
                f_tok = str(int(float(fut_list[0]["0"])))
                q_fut = client.quotes(instrument_tokens=[{"instrument_token": f_tok, "exchange_segment": conf["Exchange"]}], quote_type="all")
                if q_fut:
                    f_item = q_fut[0] if isinstance(q_fut, list) else q_fut.get('data', [{}])[0]
                    base_ltp = float(f_item.get('ltp', f_item.get('lastPrice', 0)))

        if base_ltp == 0: return False, "❌ Spot & Future Price 0 (Market Closed / API error)"
            
        atm = int(round(base_ltp / conf["StrikeGap"]) * conf["StrikeGap"])
        USER_SETTINGS[cid]["ATM"] = f"{atm}"
        
        cursor = fo_master_col.find({"IndexName": idx_name})
        df = pd.DataFrame(list(cursor))
        if df.empty: return False, "❌ Master Data Empty in MongoDB."
        
        df.columns = df.columns.astype(str)
        relevant_opts = df[df["5"].str.contains("CE|PE", regex=True)]
        if relevant_opts.empty: return False, "❌ No Options in DB."
        
        relevant_opts['Exp'] = relevant_opts['5'].str.extract(r'([0-9]{2}[A-Z]{3}[0-9]{2})')
        expiry_date_str = relevant_opts['Exp'].dropna().iloc[0]
        
        prefix = f"{idx_name}{expiry_date_str}"
        relevant = df[df["7"].str.startswith(prefix, na=False)]
        
        # Deep Chain: ±40 Strikes to catch 1Rs/5Rs premiums
        strikes = [atm + (i * conf["StrikeGap"]) for i in range(-40, 41)] 
        new_list = []
        
        for index, r in relevant.iterrows():
            ref_key = str(r["7"]).strip()
            trd_sym = str(r["5"]).strip()
            token = str(int(float(r["0"])))
            for stk in strikes:
                if f"{stk}.00CE" in ref_key or f"{stk}CE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "Token": token, "Type": "CE", "Strike": stk, "LTP": 0.0, "OI": 0, "OI_Change": 0})
                elif f"{stk}.00PE" in ref_key or f"{stk}PE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "Token": token, "Type": "PE", "Strike": stk, "LTP": 0.0, "OI": 0, "OI_Change": 0})
                     
        if not new_list: return False, "❌ Strikes list empty."
        ACTIVE_TOKENS[cid] = new_list
        return True, f"🎯 Price: {base_ltp} | ATM: {atm} | Exp: {expiry_date_str}"
    except Exception as e: return False, f"❌ Chain Gen Error: {e}"

def fetch_data_for_user(cid):
    if cid not in USER_SESSIONS: return False, "❌ No Session"
    if cid not in ACTIVE_TOKENS or not ACTIVE_TOKENS[cid]: 
        success, msg = auto_generate_chain(cid)
        if not success: return False, f"{msg}"
        
    client = USER_SESSIONS[cid]
    conf = INDICES_CONFIG[USER_SETTINGS[cid]["Index"]]
    try:
        all_tokens = ACTIVE_TOKENS[cid]
        live_map = {}
        for i in range(0, len(all_tokens), 50):
            batch = all_tokens[i : i + 50]
            tokens = [{"instrument_token": x['Token'], "exchange_segment": conf["Exchange"]} for x in batch]
            q = client.quotes(instrument_tokens=tokens, quote_type="all")
            if q:
                raw = q if isinstance(q, list) else q.get('data', [])
                for item in raw:
                    tk = str(item.get('exchange_token') or item.get('tk'))
                    live_map[tk] = {
                        'ltp': float(item.get('ltp', item.get('lastPrice', 0))),
                        'oi': int(item.get('open_int', item.get('openInterest', item.get('oi', 0)))),
                        'oi_chg': int(item.get('chng_in_oi', item.get('netChange', 0)))
                    }
        for item in all_tokens:
            d = live_map.get(item['Token'], {'ltp': 0, 'oi': 0, 'oi_chg': 0})
            item['LTP'] = d['ltp']; item['OI'] = d['oi']; item['OI_Change'] = d['oi_chg']
        ACTIVE_TOKENS[cid] = all_tokens 
        return True, "Success"
    except Exception as e: return False, f"❌ Fetch Error: {e}"

# =========================================
# --- BACKGROUND THREADS ---
# =========================================
def sl_monitor_thread():
    while True:
        try:
            open_sl_trades = list(trades_col.find({"Status": "OPEN", "SLOrderID": {"$nin": ["", "nan", None]}}))
            for row in open_sl_trades:
                sl_id = str(row.get('SLOrderID', ""))
                cid = int(row['ChatID'])
                if cid in USER_SESSIONS:
                    client = USER_SESSIONS[cid]
                    order_hist = client.order_history(order_id=sl_id)
                    if order_hist and isinstance(order_hist, list):
                        status = order_hist[0].get('status', '').upper()
                        if status in ['COMPLETE', 'FILLED']:
                            trades_col.update_one({"_id": row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": row['SLPrice']}})
                            bot.send_message(cid, f"🎯 **SL HIT:** {row['TradeSymbol']}\nClosing Hedge (if any)...")
                            
                            hedge_pos = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN", "Side": "BUY", "Index": row['Index']}))
                            for h_row in hedge_pos:
                                conf = INDICES_CONFIG[h_row['Index']]
                                h_ltp = 0
                                try:
                                    hq = client.quotes(instrument_tokens=[{"instrument_token": str(h_row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                                    h_item = hq[0] if isinstance(hq, list) else hq.get('data', [{}])[0]
                                    h_ltp = float(h_item.get('ltp', h_item.get('lastPrice', 0)))
                                except: pass
                                place_marketable_limit(client, conf, int(h_row['Qty']), h_row['TradeSymbol'], "S", h_ltp)
                                trades_col.update_one({"_id": h_row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": h_ltp}})
                                
                        elif status in ['REJECTED', 'CANCELLED']:
                            trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})
        except: pass 
        time.sleep(5) 
threading.Thread(target=sl_monitor_thread, daemon=True).start()

def auto_updater():
    while True:
        try:
            for cid in list(USER_SESSIONS.keys()): fetch_data_for_user(cid)
        except: pass
        time.sleep(180) 
threading.Thread(target=auto_updater, daemon=True).start()

# =========================================
# --- MENUS ---
# =========================================
def get_main_menu(cid):
    idx = USER_SETTINGS[cid]["Index"]
    mk = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    mk.add(types.KeyboardButton("🤖 AI Market Analysis"))
    mk.add(types.KeyboardButton("🔄 Refresh Data"))
    mk.add(types.KeyboardButton(f"🚀 New Trade ({idx})"), types.KeyboardButton("💰 P&L"))
    mk.add(types.KeyboardButton("⚡ Auto Strangle"))
    mk.add(types.KeyboardButton("📊 OI Data"), types.KeyboardButton("📋 API Orders"))
    mk.add(types.KeyboardButton("🛑 Stop Loss (SL)"), types.KeyboardButton("🗑️ DB Orders"))
    mk.add(types.KeyboardButton("🚪 Logout"), types.KeyboardButton("🚨 EXIT ALL"))
    mk.add(types.KeyboardButton(f"Index: {idx} 🔀"))
    return mk

def get_login_btn(): return types.ReplyKeyboardMarkup(resize_keyboard=True).add(types.KeyboardButton("🔐 Login Now"))

# =========================================
# --- COMMAND HANDLERS ---
# =========================================
@bot.message_handler(commands=['logout'])
def cmd_logout(message):
    cid = message.chat.id
    if cid in USER_SESSIONS: del USER_SESSIONS[cid]
    bot.send_message(cid, "👋 Logged Out.", reply_markup=get_login_btn())

@bot.message_handler(commands=['login'])
def cmd_login_command(message):
    cid = message.chat.id
    if cid in USER_DETAILS:
        USER_STATE[cid] = "WAIT_TOTP"
        bot.send_message(cid, f"🔐 Enter **TOTP**:", reply_markup=types.ReplyKeyboardRemove())
    else: bot.send_message(cid, "❌ User not found. Type /start to register.")

@bot.message_handler(commands=['start'])
def cmd_start(message):
    cid = message.chat.id
    load_users()
    if cid in USER_DETAILS:
        if cid in USER_SESSIONS: bot.send_message(cid, f"👋 Ready! Index: {USER_SETTINGS[cid]['Index']}", reply_markup=get_main_menu(cid))
        else: bot.send_message(cid, "👋 Welcome back!", reply_markup=get_login_btn())
    else:
        USER_STATE[cid] = "REG_NAME"; TEMP_REG_DATA[cid] = {}
        bot.send_message(cid, "🆕 **Registration**\nEnter Name:")

@bot.message_handler(func=lambda m: (USER_STATE.get(m.chat.id) or "").startswith("REG_"))
def reg_flow(m):
    cid, text = m.chat.id, m.text.strip()
    st = USER_STATE[cid]
    if st == "REG_NAME": TEMP_REG_DATA[cid]['Name'] = text; USER_STATE[cid] = "REG_KEY"; bot.send_message(cid, "Consumer Key:")
    elif st == "REG_KEY": TEMP_REG_DATA[cid]['Key'] = text; USER_STATE[cid] = "REG_MOB"; bot.send_message(cid, "Mobile (+91...):")
    elif st == "REG_MOB": TEMP_REG_DATA[cid]['Mobile'] = text; USER_STATE[cid] = "REG_UCC"; bot.send_message(cid, "UCC:")
    elif st == "REG_UCC": TEMP_REG_DATA[cid]['UCC'] = text; USER_STATE[cid] = "REG_MPIN"; bot.send_message(cid, "MPIN:")
    elif st == "REG_MPIN":
        TEMP_REG_DATA[cid]['MPIN'] = text
        if save_new_user(cid, TEMP_REG_DATA[cid]): bot.send_message(cid, "✅ Registered!", reply_markup=get_login_btn())
        USER_STATE[cid] = None

@bot.message_handler(func=lambda m: m.text == "🔐 Login Now")
def do_login_btn(m): cmd_login_command(m)

# =========================================
# --- MAIN LOGIC ---
# =========================================
@bot.message_handler(func=lambda message: True)
def main_handler(message):
    cid = message.chat.id
    text = message.text.strip()
    state = USER_STATE.get(cid)

    if state == "WAIT_TOTP":
        try:
            u = USER_DETAILS.get(cid, {})
            api_key = u.get('Key', u.get('ConsumerKey'))
            cl = NeoAPI(consumer_key=api_key, environment='prod')
            cl.totp_login(mobile_number=u.get('Mobile'), ucc=u.get('UCC'), totp=text)
            cl.totp_validate(mpin=u.get('MPIN'))
            USER_SESSIONS[cid] = cl
            USER_STATE[cid] = None
            bot.send_message(cid, f"✅ Logged In! Index: {USER_SETTINGS[cid]['Index']}", reply_markup=get_main_menu(cid))
            auto_generate_chain(cid)
        except Exception as e: bot.send_message(cid, f"❌ Login Failed: {e}", reply_markup=get_login_btn())
        return

    if cid not in USER_SESSIONS: return

    if text == "🤖 AI Market Analysis":
        bot.send_message(cid, "⏳ *AI analyzing OI & OI Change...*", parse_mode="Markdown")
        fetch_data_for_user(cid) 
        bot.send_message(cid, f"🤖 **Gemini AI:**\n\n{get_ai_analysis(cid)}")

    elif "Index:" in text:
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🔵 NIFTY", callback_data="SET_NIFTY"), types.InlineKeyboardButton("🔴 SENSEX", callback_data="SET_SENSEX"))
        bot.send_message(cid, "Select Index:", reply_markup=mk)

    elif text == "🔄 Refresh Data":
        bot.send_message(cid, "⏳ Updating...")
        success, msg = fetch_data_for_user(cid)
        bot.send_message(cid, "✅ Updated" if success else msg)

    elif text == "💰 P&L":
        try:
            client = USER_SESSIONS[cid]
            positions_resp = client.positions()
            pos_list = positions_resp.get('data', []) if isinstance(positions_resp, dict) else positions_resp
            if not pos_list: return bot.send_message(cid, "✅ No Positions in Broker.")
            
            msg = "💰 **Live P&L**\n\n"
            total_mtm = 0.0
            has_pos = False
            for p in pos_list:
                try: net_qty = int(p.get('netQty', p.get('flQty', 0)))
                except: net_qty = (int(p.get('flBuyQty', 0)) + int(p.get('cfBuyQty', 0))) - (int(p.get('flSellQty', 0)) + int(p.get('cfSellQty', 0)))
                
                if net_qty != 0:
                    has_pos = True
                    sym = p.get('trdSym', p.get('trading_symbol', 'Unknown'))
                    mtm = float(p.get('mtm', p.get('unRealizedPnL', p.get('pnl', 0.0))))
                    total_mtm += mtm
                    msg += f"{'🟢' if mtm >= 0 else '🔴'} **{sym}**\nQty: {net_qty} | MTM: **{mtm:+.2f}**\n\n"
            if not has_pos: msg = "✅ All positions squared off."
            else: msg += f"────────────────\n**Total MTM: {total_mtm:+.2f}**"
            bot.send_message(cid, msg)
        except Exception as e: bot.send_message(cid, f"❌ P&L Error: {e}")

    elif text == "📋 API Orders":
        try:
            client = USER_SESSIONS[cid]
            orders_resp = client.order_report()
            orders_list = orders_resp.get('data', []) if isinstance(orders_resp, dict) else orders_resp
            open_orders = [o for o in orders_list if str(o.get('ordSt', o.get('status', ''))).lower() in ['open', 'trigger pending', 'pending']]
            if not open_orders: return bot.send_message(cid, "✅ No Open API Orders.")
            
            msg = "📋 **Live API Orders**\n\n"
            for o in open_orders:
                sym = o.get('trdSym', o.get('trading_symbol', 'Unknown'))
                msg += f"⏳ **{sym}**\nStatus: {o.get('ordSt')} | Type: {o.get('ordTyp')}\nQty: {o.get('qty')} | Prc: {o.get('prc')} | Trg: {o.get('trgPrc')}\n\n"
            bot.send_message(cid, msg)
        except Exception as e: bot.send_message(cid, f"❌ Order Err: {e}")

    elif text == "🗑️ DB Orders":
        open_db = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"}))
        if not open_db: return bot.send_message(cid, "✅ DB is clean.")
        mk = types.InlineKeyboardMarkup(row_width=1)
        for row in open_db: mk.add(types.InlineKeyboardButton(f"❌ Clear {row['TradeSymbol']}", callback_data=f"DBCLEAR_{row['OrderID']}"))
        mk.add(types.InlineKeyboardButton("🗑️ Clear ALL DB", callback_data="DBCLEAR_ALL"), types.InlineKeyboardButton("⬅️ Close", callback_data="EXIT_CANCEL"))
        bot.send_message(cid, "🛠 **Manage DB Orders:**", reply_markup=mk)

    elif text == "📊 OI Data":
        success, msg = fetch_data_for_user(cid)
        if not success: return bot.send_message(cid, msg)
        USER_STATE[cid] = "WAIT_OI_RANGE"
        bot.send_message(cid, "🔢 **Range?** (Ex: 3)")

    elif text == "🛑 Stop Loss (SL)":
        mk = types.InlineKeyboardMarkup(row_width=1)
        mk.add(types.InlineKeyboardButton("🎯 Modify Specific SL", callback_data="SL_LIST_POSITIONS"), types.InlineKeyboardButton("🗑️ Cancel All SL", callback_data="SL_CANCEL_ALL"), types.InlineKeyboardButton("❌ Close", callback_data="EXIT_CANCEL"))
        bot.send_message(cid, "⚙️ **Manage Stop Loss:**", reply_markup=mk)

    elif "New Trade" in text:
        success, msg = fetch_data_for_user(cid)
        if not success: return bot.send_message(cid, msg)
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("📈 Call (CE)", callback_data="TRADE_CE"), types.InlineKeyboardButton("📉 Put (PE)", callback_data="TRADE_PE"))
        bot.send_message(cid, f"🚀 **Trade** Select:", reply_markup=mk)

    # --- AUTO STRANGLE (Custom Nifty < 6, Sensex < 12) ---
    elif text == "⚡ Auto Strangle":
        idx = USER_SETTINGS[cid]["Index"]
        success, msg = fetch_data_for_user(cid)
        if not success: return bot.send_message(cid, msg)
        
        target = 6.0 if idx == "NIFTY" else 12.0
        df = pd.DataFrame(ACTIVE_TOKENS[cid])
        
        try:
            ce_df = df[(df['Type'] == 'CE') & (df['LTP'] <= target) & (df['LTP'] > 0.5)]
            pe_df = df[(df['Type'] == 'PE') & (df['LTP'] <= target) & (df['LTP'] > 0.5)]
            
            if ce_df.empty or pe_df.empty: return bot.send_message(cid, f"❌ No options under ₹{target}")
                
            best_ce = ce_df.sort_values('LTP', ascending=False).iloc[0]
            best_pe = pe_df.sort_values('LTP', ascending=False).iloc[0]
            
            USER_STATE[cid] = "WAIT_STRANGLE_LOTS"
            PENDING_TRADE[cid] = {"CE": best_ce.to_dict(), "PE": best_pe.to_dict()}
            
            bot.send_message(cid, f"⚡ **Auto Strangle ({idx})**\nCE: {best_ce['TradeSymbol']} (@{best_ce['LTP']})\nPE: {best_pe['TradeSymbol']} (@{best_pe['LTP']})\n\n🔢 **Lots?**")
        except Exception as e: bot.send_message(cid, f"❌ Err: {e}")

    elif state == "WAIT_STRANGLE_LOTS":
        try:
            lots = int(text)
            idx = USER_SETTINGS[cid]["Index"]
            conf = INDICES_CONFIG[idx]
            qty = lots * conf["LotSize"]
            client = USER_SESSIONS[cid]
            ce = PENDING_TRADE[cid]["CE"]
            pe = PENDING_TRADE[cid]["PE"]
            sl_multiplier = 2.0 if idx == "NIFTY" else 3.0
            
            bot.send_message(cid, "⏳ Firing Limit Orders...")
            resp_ce = place_marketable_limit(client, conf, qty, ce['TradeSymbol'], "S", ce['LTP'])
            resp_pe = place_marketable_limit(client, conf, qty, pe['TradeSymbol'], "S", pe['LTP'])
            
            msg = "✅ **Strangle Executed:**\n"
            
            if isinstance(resp_ce, dict) and 'nOrdNo' in resp_ce:
                ce_oid = str(resp_ce['nOrdNo'])
                ce_sl_trigger = round(ce['LTP'] * sl_multiplier, 1)
                ce_sl_limit = ce_sl_trigger + 10.0
                trades_col.insert_one({"ChatID": str(cid), "Index": idx, "TradeSymbol": ce['TradeSymbol'], "Token": ce['Token'], "Side": "SELL", "Qty": qty, "EntryPrice": ce['LTP'], "Status": "OPEN", "OrderID": ce_oid})
                client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(ce_sl_limit), order_type="SL", quantity=str(qty), validity="DAY", trading_symbol=ce['TradeSymbol'], transaction_type="B", trigger_price=str(ce_sl_trigger), amo="NO")
                msg += f"🔴 CE {ce['LTP']} (SL: {ce_sl_trigger})\n"
                
            if isinstance(resp_pe, dict) and 'nOrdNo' in resp_pe:
                pe_oid = str(resp_pe['nOrdNo'])
                pe_sl_trigger = round(pe['LTP'] * sl_multiplier, 1)
                pe_sl_limit = pe_sl_trigger + 10.0
                trades_col.insert_one({"ChatID": str(cid), "Index": idx, "TradeSymbol": pe['TradeSymbol'], "Token": pe['Token'], "Side": "SELL", "Qty": qty, "EntryPrice": pe['LTP'], "Status": "OPEN", "OrderID": pe_oid})
                client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(pe_sl_limit), order_type="SL", quantity=str(qty), validity="DAY", trading_symbol=pe['TradeSymbol'], transaction_type="B", trigger_price=str(pe_sl_trigger), amo="NO")
                msg += f"🔴 PE {pe['LTP']} (SL: {pe_sl_trigger})"

            bot.send_message(cid, msg)
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ Err: {e}")

    elif text == "🚨 EXIT ALL":
        mk = types.InlineKeyboardMarkup(row_width=1).add(types.InlineKeyboardButton("🚨 EXIT ALL POSITIONS (SAFE)", callback_data="EXIT_ALL_CONFIRM"), types.InlineKeyboardButton("❌ Cancel", callback_data="EXIT_CANCEL"))
        bot.send_message(cid, "⚠️ **WARNING: Close ALL? (Buys exit first)**", reply_markup=mk)

    elif state == "WAIT_PREMIUM":
        try: PENDING_TRADE[cid]["Target"] = float(text); USER_STATE[cid] = "WAIT_LOTS"; bot.send_message(cid, "🔢 **Lots?**")
        except: bot.send_message(cid, "❌ Number only.")

    elif state == "WAIT_LOTS":
        try:
            lots = int(text)
            idx = USER_SETTINGS[cid]["Index"]
            conf = INDICES_CONFIG[idx]
            qty = int(lots * conf["LotSize"])
            PENDING_TRADE[cid]["Qty"] = qty
            fetch_data_for_user(cid)
            df = pd.DataFrame(ACTIVE_TOKENS[cid])
            target, opt_type, hedge_mode = PENDING_TRADE[cid]["Target"], PENDING_TRADE[cid]["Type"], PENDING_TRADE[cid].get("HedgeMode", "HEDGE")
            
            df = df[(df['Type'] == opt_type) & (df['LTP'] > 0)]
            if df.empty: return bot.send_message(cid, "❌ No Option Data.")
                
            main = df[df['LTP'] <= target].sort_values('LTP', ascending=False)
            main = main.iloc[0] if not main.empty else df.sort_values('LTP', ascending=True).iloc[0]
            PENDING_TRADE[cid]["Main"] = main.to_dict()
            
            if hedge_mode == "HEDGE":
                pool = df[df['Strike'] > main['Strike']].copy() if opt_type == 'CE' else df[df['Strike'] < main['Strike']].copy()
                if pool.empty: return bot.send_message(cid, "❌ Hedge not found.")
                pool['diff'] = abs(pool['LTP'] - (main['LTP'] * 0.20))
                hedge = pool.sort_values(by=['diff', 'LTP']).iloc[0]
                PENDING_TRADE[cid]["Hedge"] = hedge.to_dict()
                msg = f"⚡ **CONFIRM HEDGED**\n🔴 SELL: {main['TradeSymbol']} (@{main['LTP']})\n🟢 BUY: {hedge['TradeSymbol']} (@{hedge['LTP']})"
            else:
                PENDING_TRADE[cid]["Hedge"] = None
                msg = f"⚡ **CONFIRM DIRECT SELL**\n🔴 SELL: {main['TradeSymbol']} (@{main['LTP']})"

            mk = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🔥 FIRE", callback_data="EXECUTE_TRADE"), types.InlineKeyboardButton("❌ CANCEL", callback_data="CANCEL_TRADE"))
            bot.send_message(cid, msg, reply_markup=mk)
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ Error: {e}")

    elif state == "WAIT_OI_RANGE":
        try:
            n = int(text)
            fetch_data_for_user(cid)
            df = pd.DataFrame(ACTIVE_TOKENS[cid])
            df['OI'] = df['OI'].fillna(0).astype(int)
            ce_df = df[df['Type'] == 'CE'].sort_values('Strike').reset_index(drop=True)
            pe_df = df[df['Type'] == 'PE'].sort_values('Strike').reset_index(drop=True)
            mid = len(ce_df) // 2 
            sel_pe, sel_ce = pe_df.iloc[max(0, mid-n) : mid+1], ce_df.iloc[mid : min(len(ce_df), mid+n+1)]
            pe_oi, ce_oi = sel_pe['OI'].sum(), sel_ce['OI'].sum()
            bot.send_message(cid, f"📊 **OI Analysis (±{n})**\n🛡️ PE: {format_crore_lakh(pe_oi)}\n⚔️ CE: {format_crore_lakh(ce_oi)}\nDiff: **{format_crore_lakh(pe_oi - ce_oi)}**", reply_markup=get_main_menu(cid))
            USER_STATE[cid] = None
        except: bot.send_message(cid, "❌ Error.")

# =========================================
# --- CALLBACKS ---
# =========================================
@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    cid = call.message.chat.id
    if cid not in USER_SESSIONS: return
    
    if call.data in ["SET_NIFTY", "SET_SENSEX"]:
        USER_SETTINGS[cid]["Index"] = "NIFTY" if "NIFTY" in call.data else "SENSEX"
        ACTIVE_TOKENS[cid] = [] 
        bot.edit_message_text(f"✅ Index: {USER_SETTINGS[cid]['Index']}", cid, call.message.message_id)
        auto_generate_chain(cid)

    elif call.data in ["TRADE_CE", "TRADE_PE"]:
        PENDING_TRADE[cid] = {"Type": "CE" if "CE" in call.data else "PE"}
        mk = types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🛡️ With Hedge", callback_data="HEDGE_YES"), types.InlineKeyboardButton("⚠️ Without Hedge", callback_data="HEDGE_NO"))
        bot.edit_message_text("Select Mode:", cid, call.message.message_id, reply_markup=mk)

    elif call.data in ["HEDGE_YES", "HEDGE_NO"]:
        PENDING_TRADE[cid]["HedgeMode"] = "HEDGE" if call.data == "HEDGE_YES" else "DIRECT"
        USER_STATE[cid] = "WAIT_PREMIUM"
        bot.edit_message_text("💰 Enter Sell Premium Target:", cid, call.message.message_id)

    elif call.data == "EXECUTE_TRADE":
        try:
            bot.edit_message_text("⏳ Executing...", cid, call.message.message_id)
            t_data, idx, client = PENDING_TRADE[cid], USER_SETTINGS[cid]["Index"], USER_SESSIONS[cid]
            conf, qty = INDICES_CONFIG[idx], int(t_data["Qty"])
            
            if t_data.get("HedgeMode") == "HEDGE" and t_data.get("Hedge"):
                resp_h = place_marketable_limit(client, conf, qty, t_data["Hedge"]["TradeSymbol"], "B", t_data["Hedge"]["LTP"])
                time.sleep(0.2)
                log_trade(cid, idx, t_data["Hedge"]["TradeSymbol"], t_data["Hedge"]["Token"], t_data["Type"], "BUY", qty, t_data["Hedge"]["LTP"], str(resp_h.get('nOrdNo', '')))

            resp_m = place_marketable_limit(client, conf, qty, t_data["Main"]["TradeSymbol"], "S", t_data["Main"]["LTP"])
            if isinstance(resp_m, dict) and 'nOrdNo' in resp_m:
                m_oid, entry = str(resp_m['nOrdNo']), float(t_data["Main"]["LTP"])
                log_trade(cid, idx, t_data["Main"]["TradeSymbol"], t_data["Main"]["Token"], t_data["Type"], "SELL", qty, entry, m_oid)
                
                sl_trg = round(entry * 2.0, 1)
                sl_lim = sl_trg + 10.0
                time.sleep(0.5)
                resp_sl = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(sl_lim), order_type="SL", quantity=str(qty), validity="DAY", trading_symbol=t_data["Main"]["TradeSymbol"], transaction_type="B", trigger_price=str(sl_trg), amo="NO")
                
                if isinstance(resp_sl, dict) and 'nOrdNo' in resp_sl:
                    trades_col.update_one({"OrderID": m_oid}, {"$set": {"SLOrderID": str(resp_sl['nOrdNo']), "SLPrice": sl_trg}})
                    bot.send_message(cid, f"✅ Trade & SL Placed!\nMain: {m_oid}\nSL: {sl_trg}")
                else: bot.send_message(cid, f"⚠️ SL Failed: {resp_sl}")
        except Exception as e: bot.send_message(cid, f"❌ Execution Err: {e}")

    elif call.data == "DBCLEAR_ALL":
        trades_col.update_many({"ChatID": str(cid), "Status": "OPEN"}, {"$set": {"Status": "CLOSED", "ExitPrice": 0}})
        bot.edit_message_text("🗑️ DB Cleared.", cid, call.message.message_id)
        
    elif call.data.startswith("DBCLEAR_"):
        trades_col.update_one({"OrderID": call.data.split("_")[1]}, {"$set": {"Status": "CLOSED", "ExitPrice": 0}})
        bot.edit_message_text("🗑️ DB Order Cleared.", cid, call.message.message_id)

    elif call.data == "CANCEL_TRADE": bot.edit_message_text("🚫 Cancelled.", cid, call.message.message_id)
    elif call.data == "EXIT_CANCEL": bot.delete_message(cid, call.message.message_id)

    elif call.data == "SL_LIST_POSITIONS":
        open_sells = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN", "Side": "SELL"}))
        if not open_sells: return bot.answer_callback_query(call.id, "No Open SELLs!")
        mk = types.InlineKeyboardMarkup(row_width=1)
        for row in open_sells: mk.add(types.InlineKeyboardButton(f"{row['TradeSymbol']}", callback_data=f"SLMENU_{row['OrderID']}"))
        mk.add(types.InlineKeyboardButton("⬅️ Back", callback_data="EXIT_CANCEL"))
        bot.edit_message_text("🎯 **Select position:**", cid, call.message.message_id, reply_markup=mk)

    elif call.data.startswith("SLMENU_"):
        oid = call.data.split("_")[1]
        mk = types.InlineKeyboardMarkup(row_width=2)
        for p in [25, 50, 100]: mk.add(types.InlineKeyboardButton(f"{p}%", callback_data=f"SLSET_{oid}_{p}"))
        mk.add(types.InlineKeyboardButton("🗑️ Cancel SL", callback_data=f"SLCANCEL_{oid}"))
        bot.edit_message_text(f"🛠 **Manage SL {oid}:**", cid, call.message.message_id, reply_markup=mk)

    elif call.data.startswith("SLSET_"):
        _, oid, pct = call.data.split("_")
        row = trades_col.find_one({"OrderID": str(oid)})
        if row:
            if row.get('SLOrderID'): 
                try: USER_SESSIONS[cid].cancel_order(order_id=row['SLOrderID'])
                except: pass
            trg = round(float(row['EntryPrice']) * (1 + (float(pct) / 100)), 1)
            lim = trg + 10.0
            resp = USER_SESSIONS[cid].place_order(exchange_segment=INDICES_CONFIG[row['Index']]["Exchange"], product="NRML", price=str(lim), order_type="SL", quantity=str(row['Qty']), validity="DAY", trading_symbol=row['TradeSymbol'], transaction_type="B", trigger_price=str(trg), amo="NO")
            if 'nOrdNo' in resp:
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": str(resp['nOrdNo']), "SLPrice": trg}})
                bot.edit_message_text(f"✅ Modified SL at {trg}", cid, call.message.message_id)

    elif call.data.startswith("SLCANCEL_"):
        row = trades_col.find_one({"OrderID": call.data.split("_")[1]})
        if row and row.get('SLOrderID'):
            try: USER_SESSIONS[cid].cancel_order(order_id=row['SLOrderID'])
            except: pass
            trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})
            bot.edit_message_text("🗑️ SL Cancelled.", cid, call.message.message_id)

    elif call.data == "SL_CANCEL_ALL":
        for row in list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"})):
            if row.get('SLOrderID'):
                try: USER_SESSIONS[cid].cancel_order(order_id=row['SLOrderID'])
                except: pass
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})
        bot.edit_message_text("🗑️ All SL cancelled.", cid, call.message.message_id)

    # --- CUSTOM EXIT ALL LOGIC: BUYS EXITED BEFORE SELLS ---
    elif call.data == "EXIT_ALL_CONFIRM":
        bot.edit_message_text("🚨 **INITIATING SAFE EXIT SEQUENCE...**", cid, call.message.message_id)
        try:
            open_rows = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"}))
            if not open_rows: return bot.send_message(cid, "✅ No Open DB Positions.")
            client = USER_SESSIONS[cid]
            
            for row in open_rows:
                if row.get('SLOrderID'):
                    try: client.cancel_order(order_id=row['SLOrderID'])
                    except: pass
            
            # STEP 1: EXIT BUY POSITIONS FIRST
            buys = [r for r in open_rows if r['Side'] == 'BUY']
            for row in buys:
                conf = INDICES_CONFIG[row['Index']]
                ex_ltp = 0
                try:
                    q = client.quotes(instrument_tokens=[{"instrument_token": str(row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                    item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
                    ex_ltp = float(item.get('ltp', item.get('lastPrice', 0)))
                except: pass
                place_marketable_limit(client, conf, int(row['Qty']), row['TradeSymbol'], "S", ex_ltp)
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": ex_ltp}})
            
            time.sleep(0.5)
            
            # STEP 2: EXIT SELL POSITIONS LATER
            sells = [r for r in open_rows if r['Side'] == 'SELL']
            for row in sells:
                conf = INDICES_CONFIG[row['Index']]
                ex_ltp = 0
                try:
                    q = client.quotes(instrument_tokens=[{"instrument_token": str(row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                    item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
                    ex_ltp = float(item.get('ltp', item.get('lastPrice', 0)))
                except: pass
                place_marketable_limit(client, conf, int(row['Qty']), row['TradeSymbol'], "B", ex_ltp)
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": ex_ltp}})
                
            bot.send_message(cid, "🏁 **SAFE EXIT COMPLETE.**\nAll Buys closed before Sells.")
        except Exception as e: bot.send_message(cid, f"❌ Exit All Error: {e}")

# =========================================
# --- WEB SERVER (PREVENTS CRASH ON RENDER) ---
# =========================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is fully active!")

def keep_alive():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    server.serve_forever()

if __name__ == "__main__":
    threading.Thread(target=keep_alive, daemon=True).start()
    while True:
        try: bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e: time.sleep(10)
