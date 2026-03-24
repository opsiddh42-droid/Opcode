import telebot
from telebot import types
import pandas as pd
import time
import os
import threading
import requests
import io
from neo_api_client import NeoAPI
from datetime import datetime, timedelta
from pymongo import MongoClient
import google.generativeai as genai

# =========================================
# --- CONFIGURATION & MONGODB & AI ---
# =========================================
# Secrets fetched from Environment Variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# AI Setup
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel('gemini-2.5-flash') 
else:
    print("⚠️ GEMINI_API_KEY not found! AI Analysis will not work.")

# MongoDB Setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["tradingbot"]

# Collections
users_col = db["users"]
trades_col = db["trades"]
fo_master_col = db["fo_master"]
analysis_col = db["analysis_history"]

# UPDATED: Spot Tokens for Nifty & Sensex
INDICES_CONFIG = {
    "NIFTY": {
        "Exchange": "nse_fo", 
        "SpotExchange": "nse_cm",  
        "SpotToken": "Nifty 50",   
        "LotSize": 25,             
        "StrikeGap": 50
    },
    "SENSEX": {
        "Exchange": "bse_fo", 
        "SpotExchange": "bse_cm", 
        "SpotToken": "Sensex",     
        "LotSize": 10,             
        "StrikeGap": 100
    }
}

# --- GLOBALS ---
USER_SESSIONS = {}
USER_DETAILS = {}
USER_SETTINGS = {}
USER_STATE = {}
PENDING_TRADE = {}
ACTIVE_TOKENS = {} 
TEMP_REG_DATA = {}

# =========================================
# --- 1. SETUP, DB MANAGEMENT & AI LOGIC ---
# =========================================
print("🚀 Starting Advanced Algo Bot with Spot ATM, Direct P&L & AI Analysis...")
USER_SESSIONS.clear()
bot = telebot.TeleBot(BOT_TOKEN)

def load_users():
    try:
        USER_DETAILS.clear()
        for row in users_col.find():
            cid = int(row.get('ChatID', 0))
            if cid == 0: continue
            
            USER_DETAILS[cid] = {
                "Name": row.get('Name', ''), 
                "Key": row.get('Key', row.get('ConsumerKey', '')), 
                "Mobile": row.get('Mobile', ''), 
                "UCC": row.get('UCC', ''), 
                "MPIN": row.get('MPIN', '')
            }
            if cid not in USER_SETTINGS: 
                USER_SETTINGS[cid] = {"Index": "NIFTY", "ATM": None}
    except Exception as e: 
        print(f"Load Error: {e}")
load_users()

def save_new_user(cid, data):
    new_row = {
        "ChatID": str(cid), 
        "Name": data.get("Name", ""), 
        "Key": data.get("Key", data.get("ConsumerKey", "")), 
        "Mobile": data.get("Mobile", ""), 
        "UCC": data.get("UCC", ""), 
        "MPIN": data.get("MPIN", "")
    }
    try:
        users_col.insert_one(new_row)
        USER_DETAILS[cid] = new_row
        USER_SETTINGS[cid] = {"Index": "NIFTY", "ATM": None}
        return True
    except Exception as e: 
        print(f"MongoDB Insert Error: {e}")
        return False

def log_trade(cid, idx_name, trade_symbol, token, opt_type, side, qty, price, order_id):
    new_row = {
        "ChatID": str(cid), "Index": idx_name,
        "Date": datetime.now().strftime("%Y-%m-%d"), "Time": datetime.now().strftime("%H:%M:%S"),
        "TradeSymbol": trade_symbol, "Token": token, "Type": opt_type, "Side": side,
        "Qty": int(qty), "EntryPrice": price, "ExitPrice": 0, "Status": "OPEN", 
        "OrderID": str(order_id), "SLOrderID": "", "SLPrice": 0
    }
    try:
        trades_col.insert_one(new_row)
    except Exception as e: print(f"Log Error: {e}")

def format_crore_lakh(number):
    val = abs(number)
    if val >= 10000000: return f"{number / 10000000:.2f} Cr"
    elif val >= 100000: return f"{number / 100000:.2f} L"
    else: return f"{number:,.0f}"

# UPDATED: AI Analysis focusing on ATM + 4 OTM Strikes
def get_ai_analysis(cid):
    if not GEMINI_API_KEY:
        return "❌ Gemini API Key missing in environment."
    if cid not in ACTIVE_TOKENS or not ACTIVE_TOKENS[cid]:
        return "❌ Data not loaded. Please refresh data first."
        
    try:
        idx_name = USER_SETTINGS[cid]["Index"]
        conf = INDICES_CONFIG[idx_name]
        atm = float(USER_SETTINGS[cid].get("ATM", 0))
        df = pd.DataFrame(ACTIVE_TOKENS[cid])
        
        # Ensure data types are correct
        df['OI'] = pd.to_numeric(df['OI'], errors='coerce').fillna(0).astype(int)
        df['Strike'] = pd.to_numeric(df['Strike'], errors='coerce').astype(float)
        
        # 1. MACRO PICTURE (Overall PCR)
        overall_ce_oi = int(df[df['Type'] == 'CE']['OI'].sum())
        overall_pe_oi = int(df[df['Type'] == 'PE']['OI'].sum())
        overall_pcr = round(overall_pe_oi / overall_ce_oi, 2) if overall_ce_oi > 0 else 0
        
        # 2. MICRO PICTURE (Strictly ATM + 4 OTM Strikes)
        gap = conf["StrikeGap"]
        ce_otm_strikes = [atm + (i * gap) for i in range(0, 5)] 
        pe_otm_strikes = [atm - (i * gap) for i in range(0, 5)]
        
        ce_otm_df = df[(df['Type'] == 'CE') & (df['Strike'].isin(ce_otm_strikes))]
        pe_otm_df = df[(df['Type'] == 'PE') & (df['Strike'].isin(pe_otm_strikes))]
        
        otm_ce_oi = int(ce_otm_df['OI'].sum())
        otm_pe_oi = int(pe_otm_df['OI'].sum())
        otm_pcr = round(otm_pe_oi / otm_ce_oi, 2) if otm_ce_oi > 0 else 0
        
        otm_max_ce_strike = ce_otm_df.loc[ce_otm_df['OI'].idxmax()]['Strike'] if not ce_otm_df.empty else "N/A"
        otm_max_pe_strike = pe_otm_df.loc[pe_otm_df['OI'].idxmax()]['Strike'] if not pe_otm_df.empty else "N/A"

        # 3. HISTORY TRACKING (For catching sudden OI shifting)
        query = {"ChatID": str(cid), "Index": idx_name}
        last_record = analysis_col.find_one(query)
        
        prev_ce_oi = last_record.get("OTM_CE_OI", otm_ce_oi) if last_record else otm_ce_oi
        prev_pe_oi = last_record.get("OTM_PE_OI", otm_pe_oi) if last_record else otm_pe_oi
        prev_time = last_record.get("Time", "Day Start") if last_record else "Day Start"
        
        current_time = datetime.now().strftime("%H:%M:%S")
        analysis_col.update_one(
            query,
            {"$set": {"OTM_CE_OI": otm_ce_oi, "OTM_PE_OI": otm_pe_oi, "Time": current_time}},
            upsert=True
        )

        target_premium = 20 if idx_name == "NIFTY" else 40
        ce_change = otm_ce_oi - prev_ce_oi
        pe_change = otm_pe_oi - prev_pe_oi

        prompt = f"""
        Aap ek ruthless, highly decisive Institutional Quant Analyst hain jo ek professional Option Seller ko clear trade commands deta hai.
        
        **Live Data for {idx_name}:**
        - Current ATM: {atm}
        - MACRO Trend (Full Chain PCR): {overall_pcr}
        - MICRO Trend (ATM to 4 OTM PCR): {otm_pcr}
        
        **Immediate Smart Money Zone (ATM to 4 OTM):**
        - OTM Call OI (Resistance Strength): {otm_ce_oi}
        - OTM Put OI (Support Strength): {otm_pe_oi}
        - Highest Call Resistance in OTM Zone: {otm_max_ce_strike}
        - Highest Put Support in OTM Zone: {otm_max_pe_strike}
        
        **Recent OI Shift in OTM Zone (From {prev_time} to {current_time}):**
        - Call OI Change: {ce_change}
        - Put OI Change: {pe_change}
        
        **Trader Profile:** Option Seller, target premium to short is ~₹{target_premium}.
        
        **CRITICAL INSTRUCTIONS (DO NOT VIOLATE):**
        1. NO DIPLOMATIC ANSWERS. Pick ONE primary bias.
        2. Focus heavily on the **MICRO Trend (ATM to 4 OTM PCR)**. Agar MACRO aur MICRO diverge kar rahe hain, toh MICRO (ATM+4 OTM) ko importance do kyunki wahi par live action ho raha hai.
        3. Explain WHO IS GETTING TRAPPED based on the OI shifting ({ce_change} vs {pe_change}).
        4. Give a clear execution command.
        
        Format your response EXACTLY like this in strict Hinglish:
        
        🧠 **Quant Reasoning & Trap Zone:** [Short and sharp analysis focusing on ATM+4 OTM PCR and shifting]
        🎯 **Definitive Bias:** [Strong Bearish / Strong Bullish / Pure Sideways]
        ⚡ **Trade Execution Command:** [Clear command for Option Seller. Mention which side to short and the safe strike near {otm_max_ce_strike} or {otm_max_pe_strike}]
        """
        
        response = ai_model.generate_content(prompt)
        return response.text
    except Exception as e:
        return f"❌ AI Analysis failed: {e}"
# =========================================
# --- 2. DATA ENGINE & MONITOR ---
# =========================================
# UPDATED: Fast Option Chain with Live Spot Price (isIndex=True)
def auto_generate_chain(cid):
    idx_name = USER_SETTINGS[cid]["Index"]
    conf = INDICES_CONFIG[idx_name]
    if cid not in USER_SESSIONS: return False, "❌ No Session Active. Login again."
    client = USER_SESSIONS[cid]
    
    now = datetime.now()
    
    try:
        # 1. Fetch Spot Price with isIndex=True
        spot_token = conf["SpotToken"]
        spot_exchange = conf["SpotExchange"]
        
        inst_tokens = [{"instrument_token": spot_token, "exchange_segment": spot_exchange}]
        
        # isIndex=True is critical for NIFTY/SENSEX Spot fetching in Kotak Neo
        q = client.quotes(instrument_tokens=inst_tokens, quote_type="all", isIndex=True)
        
        ltp = 0
        if q:
            item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
            ltp = float(item.get('ltp', item.get('lastPrice', 0)))
            
        if ltp == 0: return False, "❌ Spot Price is 0 (Market Closed / API error)"
        
        # 2. Calculate ATM based on Live Spot Price
        atm = round(ltp / conf["StrikeGap"]) * conf["StrikeGap"]
        USER_SETTINGS[cid]["ATM"] = f"{atm}"
        
        # 3. Fast Expiry Matching from DB
        cursor = fo_master_col.find({"IndexName": idx_name})
        df = pd.DataFrame(list(cursor))
        
        if df.empty or "7" not in df.columns.astype(str):
            return False, "❌ Master Data Empty in MongoDB."

        df.columns = df.columns.astype(str)
        expiry_date_str = None
        all_ref_keys = set(df["7"].astype(str).values) 
        
        for i in range(0, 45):
            test_date = now + timedelta(days=i)
            d_str = f"{test_date.strftime('%d')}{test_date.strftime('%b').upper()}{test_date.strftime('%y')}"
            
            check_sym_1 = f"{idx_name}{d_str}{atm}.00CE"
            check_sym_2 = f"{idx_name}{d_str}{atm}CE" 
            
            if check_sym_1 in all_ref_keys or check_sym_2 in all_ref_keys:
                expiry_date_str = d_str
                break
                
        if not expiry_date_str: return False, f"❌ Expiry Not Found for ATM {atm}"
        
        # 4. Filter only ATM ± 10 Strikes for Super-Fast Data Fetching
        prefix = f"{idx_name}{expiry_date_str}"
        relevant = df[df["7"].str.startswith(prefix, na=False)]
        
        strikes = [atm + (i * conf["StrikeGap"]) for i in range(-10, 11)] 
        new_list = []
        
        for index, r in relevant.iterrows():
            ref_key = str(r["7"]).strip()
            trd_sym = str(r["5"]).strip()
            token = str(int(float(r["0"])))
            for stk in strikes:
                if f"{stk}.00CE" in ref_key or f"{stk}CE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "RefKey": ref_key, "Token": token, "Type": "CE", "Strike": stk, "LTP": 0.0, "OI": 0})
                elif f"{stk}.00PE" in ref_key or f"{stk}PE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "RefKey": ref_key, "Token": token, "Type": "PE", "Strike": stk, "LTP": 0.0, "OI": 0})
                     
        if not new_list:
            return False, "❌ Strikes list empty."
            
        ACTIVE_TOKENS[cid] = new_list
        return True, f"🎯 Spot: {ltp} | ATM: {atm} | Exp: {expiry_date_str}"
        
    except Exception as e: 
        return False, f"❌ Chain Gen Error: {str(e)}"

def fetch_data_for_user(cid):
    if cid not in USER_SESSIONS: return False, "❌ No Session"
    if cid not in ACTIVE_TOKENS or not ACTIVE_TOKENS[cid]: 
        success, msg = auto_generate_chain(cid)
        if not success: return False, f"{msg}"
        
    client = USER_SESSIONS[cid]
    idx_name = USER_SETTINGS[cid]["Index"]
    conf = INDICES_CONFIG[idx_name]
    try:
        all_tokens = ACTIVE_TOKENS[cid]
        if not all_tokens: return False, "❌ Tokens list empty"
            
        live_map = {}
        batch_size = 50
        for i in range(0, len(all_tokens), batch_size):
            batch = all_tokens[i : i + batch_size]
            tokens = [{"instrument_token": x['Token'], "exchange_segment": conf["Exchange"]} for x in batch]
            q = client.quotes(instrument_tokens=tokens, quote_type="all")
            if q:
                raw = q if isinstance(q, list) else q.get('data', [])
                for item in raw:
                    tk = str(item.get('exchange_token') or item.get('tk'))
                    ltp_val = float(item.get('ltp', item.get('lastPrice', 0)))
                    oi_val = int(item.get('open_int') or item.get('openInterest') or item.get('oi') or 0)
                    live_map[tk] = {'ltp': ltp_val, 'oi': oi_val}
        for item in all_tokens:
            d = live_map.get(item['Token'], {'ltp': 0, 'oi': 0})
            item['LTP'] = d['ltp']; item['OI'] = d['oi']
        ACTIVE_TOKENS[cid] = all_tokens 
        return True, "Success"
    except Exception as e: 
        return False, f"❌ Fetch Error: {str(e)}"

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
                            bot.send_message(cid, f"🎯 **SL HIT:** {row['TradeSymbol']}\nClosing Hedge Automatically (if any)...")
                            
                            # Close Hedge Position logic
                            hedge_pos = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN", "Side": "BUY", "Index": row['Index']}))
                            for h_row in hedge_pos:
                                conf = INDICES_CONFIG[h_row['Index']]
                                h_ltp = 0
                                try:
                                    hq = client.quotes(instrument_tokens=[{"instrument_token": str(h_row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                                    h_item = hq[0] if isinstance(hq, list) else hq.get('data', [{}])[0]
                                    h_ltp = float(h_item.get('ltp', h_item.get('lastPrice', 0)))
                                except: pass
                                
                                client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(int(h_row['Qty'])), validity="DAY", trading_symbol=h_row['TradeSymbol'], transaction_type="S", amo="NO")
                                trades_col.update_one({"_id": h_row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": h_ltp}})
                                
                        elif status in ['REJECTED', 'CANCELLED']:
                            trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})

        except Exception as e: 
            pass 
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
# --- 3. MENUS ---
# =========================================
def get_main_menu(cid):
    idx = USER_SETTINGS[cid]["Index"]
    mk = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    mk.add(types.KeyboardButton("🤖 AI Market Analysis"))
    mk.add(types.KeyboardButton("🔄 Refresh Data"))
    mk.add(types.KeyboardButton(f"🚀 New Trade ({idx})"), types.KeyboardButton("💰 P&L"))
    mk.add(types.KeyboardButton("📊 OI Data"), types.KeyboardButton("📋 Open Orders")) # Added Open Orders
    mk.add(types.KeyboardButton("🛑 Stop Loss (SL)"), types.KeyboardButton(f"Index: {idx} 🔀"))
    mk.add(types.KeyboardButton("🚪 Logout"), types.KeyboardButton("🚨 EXIT ALL"))
    return mk

def get_login_btn():
    return types.ReplyKeyboardMarkup(resize_keyboard=True).add(types.KeyboardButton("🔐 Login Now"))
# =========================================
# --- 4. COMMAND HANDLERS ---
# =========================================
@bot.message_handler(commands=['logout'])
def cmd_logout(message):
    cid = message.chat.id
    if cid in USER_SESSIONS: del USER_SESSIONS[cid]
    bot.send_message(cid, "👋 You are Logged Out.", reply_markup=get_login_btn())

@bot.message_handler(commands=['login'])
def cmd_login_command(message):
    cid = message.chat.id
    if cid in USER_DETAILS:
        USER_STATE[cid] = "WAIT_TOTP"
        bot.send_message(cid, f"🔐 Enter **TOTP (Authenticator Code)**:", reply_markup=types.ReplyKeyboardRemove())
    else:
        bot.send_message(cid, "❌ User not found. Type /start to register.")

@bot.message_handler(commands=['start'])
def cmd_start(message):
    cid = message.chat.id
    load_users()
    if cid in USER_DETAILS:
        if cid in USER_SESSIONS:
            bot.send_message(cid, f"👋 Ready! Index: **{USER_SETTINGS[cid]['Index']}**", reply_markup=get_main_menu(cid))
        else:
            bot.send_message(cid, f"👋 Welcome back **{USER_DETAILS[cid].get('Name', '')}**!", reply_markup=get_login_btn())
    else:
        USER_STATE[cid] = "REG_NAME"
        TEMP_REG_DATA[cid] = {}
        bot.send_message(cid, "🆕 **New User Registration**\nEnter Name:")

# =========================================
# --- 5. REGISTRATION & LOGIN ---
# =========================================
@bot.message_handler(func=lambda m: (USER_STATE.get(m.chat.id) or "").startswith("REG_"))
def reg_flow(m):
    cid, text = m.chat.id, m.text.strip()
    st = USER_STATE[cid]
    if st == "REG_NAME":
        TEMP_REG_DATA[cid]['Name'] = text
        USER_STATE[cid] = "REG_KEY"; bot.send_message(cid, "Enter Consumer Key:")
    elif st == "REG_KEY":
        TEMP_REG_DATA[cid]['Key'] = text
        USER_STATE[cid] = "REG_MOB"; bot.send_message(cid, "Enter Mobile (+91...):")
    elif st == "REG_MOB":
        TEMP_REG_DATA[cid]['Mobile'] = text
        USER_STATE[cid] = "REG_UCC"; bot.send_message(cid, "Enter UCC:")
    elif st == "REG_UCC":
        TEMP_REG_DATA[cid]['UCC'] = text
        USER_STATE[cid] = "REG_MPIN"; bot.send_message(cid, "Enter MPIN:")
    elif st == "REG_MPIN":
        TEMP_REG_DATA[cid]['MPIN'] = text
        bot.send_message(cid, "⏳ Saving to Database...")
        if save_new_user(cid, TEMP_REG_DATA[cid]):
            bot.send_message(cid, "✅ Registered! Click Login.", reply_markup=get_login_btn())
        else:
            bot.send_message(cid, "❌ Database Error! Check Render logs.")
        USER_STATE[cid] = None

@bot.message_handler(func=lambda m: m.text == "🔐 Login Now")
def do_login_btn(m):
    cmd_login_command(m)

# =========================================
# --- 6. MAIN LOGIC & HANDLERS ---
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
            
            if not api_key:
                bot.send_message(cid, "❌ API Key error. Type /start to register again.")
                USER_STATE[cid] = None
                return

            cl = NeoAPI(consumer_key=api_key, environment='prod')
            cl.totp_login(mobile_number=u.get('Mobile'), ucc=u.get('UCC'), totp=text)
            cl.totp_validate(mpin=u.get('MPIN'))
            
            USER_SESSIONS[cid] = cl
            USER_STATE[cid] = None
            idx = USER_SETTINGS[cid]["Index"]
            bot.send_message(cid, f"✅ Logged In! Index: {idx}", reply_markup=get_main_menu(cid))
            auto_generate_chain(cid)
        except Exception as e:
            bot.send_message(cid, f"❌ Login Failed: {e}", reply_markup=get_login_btn())
            USER_STATE[cid] = None
        return

    if cid not in USER_SESSIONS: return

    # --- AI ANALYSIS BUTTON ---
    if text == "🤖 AI Market Analysis":
        bot.send_message(cid, "⏳ *AI Market ko analyze kar raha hai, please wait...*", parse_mode="Markdown")
        fetch_data_for_user(cid) 
        analysis_result = get_ai_analysis(cid)
        bot.send_message(cid, f"🤖 **Gemini AI Analysis:**\n\n{analysis_result}")

    elif "Index:" in text:
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🔵 NIFTY", callback_data="SET_NIFTY"),
               types.InlineKeyboardButton("🔴 SENSEX", callback_data="SET_SENSEX"))
        bot.send_message(cid, "Select Index:", reply_markup=mk)

    elif text == "🔄 Refresh Data":
        bot.send_message(cid, "⏳ Updating Data...")
        success, msg = fetch_data_for_user(cid)
        if success: bot.send_message(cid, "✅ Data Updated")
        else: bot.send_message(cid, f"{msg}")

    elif "Change ATM" in text:
        bot.send_message(cid, "⚙️ Auto-Detecting ATM from Spot Price...")
        success, msg = auto_generate_chain(cid)
        bot.send_message(cid, f"✅ {msg}" if success else f"{msg}")

    # --- DIRECT P&L FROM BROKER ---
    elif text == "💰 P&L":
        try:
            client = USER_SESSIONS[cid]
            positions_resp = client.positions()
            
            pos_list = positions_resp if isinstance(positions_resp, list) else positions_resp.get('data', [])
            
            if not pos_list:
                bot.send_message(cid, "✅ No Positions found in Broker account.")
                return

            msg = "💰 **Live Positions & P&L (Direct from Broker)**\n\n"
            total_mtm = 0.0
            has_open_pos = False
            
            for p in pos_list:
                net_qty = int(p.get('flQty', p.get('netQty', 0)))
                
                if net_qty != 0:
                    has_open_pos = True
                    sym = p.get('trdSym', p.get('trading_symbol', 'Unknown'))
                    mtm = float(p.get('mtm', p.get('unRealizedPnL', 0.0)))
                    ltp = float(p.get('ltp', p.get('lastPrice', 0.0)))
                    
                    total_mtm += mtm
                    icon = "🟢" if mtm >= 0 else "🔴"
                    msg += f"{icon} **{sym}**\nNet Qty: {net_qty} | LTP: {ltp}\nMTM: **{mtm:+.2f}**\n\n"
            
            if not has_open_pos:
                msg = "✅ All positions are squared off. No active open positions right now."
            else:
                msg += f"────────────────\n**Total Live MTM: {total_mtm:+.2f}**"
                
            bot.send_message(cid, msg)
        except Exception as e: 
            bot.send_message(cid, f"❌ P&L Error: {e}")

    # --- DIRECT OPEN ORDERS FROM BROKER ---
    elif text == "📋 Open Orders":
        try:
            client = USER_SESSIONS[cid]
            orders_resp = client.order_report()
            
            orders_list = orders_resp if isinstance(orders_resp, list) else orders_resp.get('data', [])
            
            open_orders = [
                o for o in orders_list 
                if str(o.get('ordSt', o.get('status', ''))).lower() in ['open', 'trigger pending', 'pending']
            ]
            
            if not open_orders:
                bot.send_message(cid, "✅ No Open or Pending Orders.")
                return
            
            msg = "📋 **Live Open Orders (From Broker)**\n\n"
            for o in open_orders:
                sym = o.get('trdSym', o.get('trading_symbol', 'Unknown'))
                status = str(o.get('ordSt', o.get('status', ''))).upper()
                ord_type = str(o.get('ordTyp', o.get('order_type', ''))).upper()
                qty = int(o.get('qty', o.get('quantity', 0)))
                price = float(o.get('prc', o.get('price', 0)))
                trigger = float(o.get('trgPrc', o.get('trigger_price', 0)))
                
                price_str = f"Trigger: {trigger}" if ord_type == "SL" else f"Price: {price}"
                
                msg += f"⏳ **{sym}**\nStatus: {status} | Type: {ord_type}\nQty: {qty} | {price_str}\n\n"
                
            bot.send_message(cid, msg)
        except Exception as e: 
            bot.send_message(cid, f"❌ Order Fetch Error: {e}")

    elif text == "📊 OI Data":
        success, msg = fetch_data_for_user(cid)
        if not success:
            bot.send_message(cid, f"{msg}")
            return
        USER_STATE[cid] = "WAIT_OI_RANGE"
        bot.send_message(cid, "🔢 **Range?** (Ex: 3)")

    elif text == "🛑 Stop Loss (SL)":
        mk = types.InlineKeyboardMarkup(row_width=1)
        mk.add(types.InlineKeyboardButton("🎯 Place/Modify SL", callback_data="SL_LIST_POSITIONS"),
               types.InlineKeyboardButton("🗑️ Cancel All SL Orders", callback_data="SL_CANCEL_ALL"),
               types.InlineKeyboardButton("❌ Close", callback_data="EXIT_CANCEL"))
        bot.send_message(cid, "⚙️ **Manage Stop Loss:**\n(Applies to SELL trades only)", reply_markup=mk)

    elif "New Trade" in text:
        idx = USER_SETTINGS[cid]["Index"]
        success, msg = fetch_data_for_user(cid)
        if not success:
            bot.send_message(cid, f"❌ Cannot start trade:\n{msg}")
            return
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("📈 Call (CE)", callback_data="TRADE_CE"),
               types.InlineKeyboardButton("📉 Put (PE)", callback_data="TRADE_PE"))
        bot.send_message(cid, f"🚀 **{idx} Trade**\nSelect Strategy:", reply_markup=mk)

    elif text == "🚨 EXIT ALL":
        mk = types.InlineKeyboardMarkup(row_width=1)
        mk.add(types.InlineKeyboardButton("🚨 EXIT ALL POSITIONS (SAFE)", callback_data="EXIT_ALL_CONFIRM"),
               types.InlineKeyboardButton("❌ Cancel", callback_data="EXIT_CANCEL"))
        bot.send_message(cid, "⚠️ **WARNING: This will close ALL positions!**\nSells will be exited before Buys.", reply_markup=mk)

    elif state == "WAIT_PREMIUM":
        try:
            PENDING_TRADE[cid]["Target"] = float(text)
            USER_STATE[cid] = "WAIT_LOTS"
            idx = USER_SETTINGS[cid]["Index"]
            sz = INDICES_CONFIG[idx]["LotSize"]
            bot.send_message(cid, f"🔢 **Enter Lots:** (1 Lot = {sz} Qty)")
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
            target = PENDING_TRADE[cid]["Target"]
            opt_type = PENDING_TRADE[cid]["Type"]
            hedge_mode = PENDING_TRADE[cid].get("HedgeMode", "HEDGE")
            
            df = df[(df['Type'] == opt_type) & (df['LTP'] > 0)]
            if df.empty:
                bot.send_message(cid, "❌ No Option Data available.")
                return
                
            main = df[df['LTP'] <= target].sort_values('LTP', ascending=False)
            main = main.iloc[0] if not main.empty else df.sort_values('LTP', ascending=True).iloc[0]
            PENDING_TRADE[cid]["Main"] = main.to_dict()
            
            if hedge_mode == "HEDGE":
                if opt_type == 'CE': pool = df[df['Strike'] > main['Strike']].copy()
                else: pool = df[df['Strike'] < main['Strike']].copy()
                
                if pool.empty:
                    bot.send_message(cid, "❌ Hedge not found. Out of Strikes.")
                    return
                pool['diff'] = abs(pool['LTP'] - (main['LTP'] * 0.20))
                hedge = pool.sort_values(by=['diff', 'LTP']).iloc[0]
                PENDING_TRADE[cid]["Hedge"] = hedge.to_dict()
                
                msg = (f"⚡ **CONFIRM {idx} HEDGED TRADE**\nLots: {lots} (Qty: {qty})\n"
                       f"🔴 SELL: {main['TradeSymbol']} (@{main['LTP']})\n"
                       f"🟢 BUY: {hedge['TradeSymbol']} (@{hedge['LTP']})\nExecute?")
            else:
                PENDING_TRADE[cid]["Hedge"] = None
                msg = (f"⚡ **CONFIRM {idx} DIRECT SELL (NO HEDGE)**\nLots: {lots} (Qty: {qty})\n"
                       f"🔴 SELL: {main['TradeSymbol']} (@{main['LTP']})\n"
                       f"⚠️ Alert: You are doing a Naked Sell.\nExecute?")

            mk = types.InlineKeyboardMarkup()
            mk.add(types.InlineKeyboardButton("🔥 FIRE", callback_data="EXECUTE_TRADE"),
                   types.InlineKeyboardButton("❌ CANCEL", callback_data="CANCEL_TRADE"))
            bot.send_message(cid, msg, reply_markup=mk)
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ Error: {e}")

    elif state == "WAIT_OI_RANGE":
        try:
            n = int(text)
            fetch_data_for_user(cid)
            df = pd.DataFrame(ACTIVE_TOKENS[cid])
            if 'OI' not in df.columns: df['OI'] = 0
            df['OI'] = df['OI'].fillna(0).astype(int)
            ce_df = df[df['Type'] == 'CE'].sort_values('Strike').reset_index(drop=True)
            pe_df = df[df['Type'] == 'PE'].sort_values('Strike').reset_index(drop=True)
            mid = len(ce_df) // 2 
            sel_pe = pe_df.iloc[max(0, mid-n) : mid+1] 
            sel_ce = ce_df.iloc[mid : min(len(ce_df), mid+n+1)]
            pe_oi = sel_pe['OI'].sum()
            ce_oi = sel_ce['OI'].sum()
            diff = pe_oi - ce_oi
            msg = (f"📊 **OI Analysis (ATM ±{n})**\n"
                   f"🛡️ PE (Supp): {format_crore_lakh(pe_oi)}\n"
                   f"⚔️ CE (Res): {format_crore_lakh(ce_oi)}\n"
                   f"Diff: **{format_crore_lakh(diff)}**")
            bot.send_message(cid, msg, reply_markup=get_main_menu(cid))
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ OI Error: {e}")

# =========================================
# --- 7. CALLBACK HANDLER ---
# =========================================
@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    cid = call.message.chat.id
    if cid not in USER_SESSIONS: return
    
    if call.data == "SET_NIFTY":
        USER_SETTINGS[cid]["Index"] = "NIFTY"
        ACTIVE_TOKENS[cid] = [] 
        bot.edit_message_text("✅ Index: NIFTY", cid, call.message.message_id)
        bot.send_message(cid, "Menu Updated.", reply_markup=get_main_menu(cid))
        auto_generate_chain(cid)
    
    elif call.data == "SET_SENSEX":
        USER_SETTINGS[cid]["Index"] = "SENSEX"
        ACTIVE_TOKENS[cid] = []
        bot.edit_message_text("✅ Index: SENSEX", cid, call.message.message_id)
        bot.send_message(cid, "Menu Updated.", reply_markup=get_main_menu(cid))
        auto_generate_chain(cid)

    elif call.data in ["TRADE_CE", "TRADE_PE"]:
        PENDING_TRADE[cid] = {"Type": "CE" if "CE" in call.data else "PE"}
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🛡️ With Hedge", callback_data="HEDGE_YES"),
               types.InlineKeyboardButton("⚠️ Without Hedge", callback_data="HEDGE_NO"))
        bot.edit_message_text("Select Trade Mode:", cid, call.message.message_id, reply_markup=mk)

    elif call.data in ["HEDGE_YES", "HEDGE_NO"]:
        PENDING_TRADE[cid]["HedgeMode"] = "HEDGE" if call.data == "HEDGE_YES" else "DIRECT"
        USER_STATE[cid] = "WAIT_PREMIUM"
        mode_text = "Hedged" if call.data == "HEDGE_YES" else "Naked Sell"
        bot.edit_message_text(f"💰 Enter Sell Premium Target for {mode_text} Trade:", cid, call.message.message_id)

    elif call.data == "EXECUTE_TRADE":
        try:
            bot.edit_message_text("⏳ Executing Market Orders...", cid, call.message.message_id)
            t_data = PENDING_TRADE[cid]
            idx = USER_SETTINGS[cid]["Index"]
            conf = INDICES_CONFIG[idx]
            client = USER_SESSIONS[cid]
            qty = int(t_data["Qty"])
            
            if t_data.get("HedgeMode") == "HEDGE" and t_data.get("Hedge"):
                resp_hedge = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(qty), validity="DAY", trading_symbol=t_data["Hedge"]["TradeSymbol"], transaction_type="B", amo="NO")
                
                if not isinstance(resp_hedge, dict) or 'nOrdNo' not in resp_hedge:
                    bot.send_message(cid, f"❌ Hedge Buy Failed: {resp_hedge}")
                    return
                time.sleep(0.2)
                log_trade(cid, idx, t_data["Hedge"]["TradeSymbol"], t_data["Hedge"]["Token"], t_data["Type"], "BUY", qty, t_data["Hedge"]["LTP"], str(resp_hedge['nOrdNo']))

            resp_main = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(qty), validity="DAY", trading_symbol=t_data["Main"]["TradeSymbol"], transaction_type="S", amo="NO")
            
            if not isinstance(resp_main, dict) or 'nOrdNo' not in resp_main:
                bot.send_message(cid, f"⚠️ Main SELL failed: {resp_main}")
            else:
                log_trade(cid, idx, t_data["Main"]["TradeSymbol"], t_data["Main"]["Token"], t_data["Type"], "SELL", qty, t_data["Main"]["LTP"], str(resp_main['nOrdNo']))
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(types.InlineKeyboardButton("105% (Auto)", callback_data=f"SLSET_{resp_main['nOrdNo']}_105"),
                       types.InlineKeyboardButton("25%", callback_data=f"SLSET_{resp_main['nOrdNo']}_25"),
                       types.InlineKeyboardButton("50%", callback_data=f"SLSET_{resp_main['nOrdNo']}_50"),
                       types.InlineKeyboardButton("100%", callback_data=f"SLSET_{resp_main['nOrdNo']}_100"))
                bot.send_message(cid, f"✅ Trade Executed!\nMain Order ID: {resp_main['nOrdNo']}\n\n**Set Stop Loss?**", reply_markup=mk)
        except Exception as e: bot.send_message(cid, f"❌ Execution Err: {e}")

    elif call.data == "CANCEL_TRADE":
        bot.edit_message_text("🚫 Trade Cancelled.", cid, call.message.message_id)

    elif call.data == "EXIT_CANCEL":
        bot.delete_message(cid, call.message.message_id)

    elif call.data == "SL_LIST_POSITIONS":
        try:
            open_sells = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN", "Side": "SELL"}))
            if not open_sells:
                bot.answer_callback_query(call.id, "No Open SELL Positions!")
                return
            mk = types.InlineKeyboardMarkup(row_width=1)
            for row in open_sells:
                sl_status = f" (SL: {row.get('SLPrice', 0)})" if row.get('SLPrice', 0) > 0 else " (No SL)"
                mk.add(types.InlineKeyboardButton(f"{row['TradeSymbol']}{sl_status}", callback_data=f"SLMENU_{row['OrderID']}"))
            mk.add(types.InlineKeyboardButton("⬅️ Back", callback_data="EXIT_CANCEL"))
            bot.edit_message_text("🎯 **Select position to set SL:**", cid, call.message.message_id, reply_markup=mk)
        except Exception as e: bot.send_message(cid, f"❌ SL List Error: {e}")

    elif call.data.startswith("SLMENU_"):
        oid = call.data.split("_")[1]
        mk = types.InlineKeyboardMarkup(row_width=2)
        mk.add(types.InlineKeyboardButton("105% (Auto)", callback_data=f"SLSET_{oid}_105"),
               types.InlineKeyboardButton("25%", callback_data=f"SLSET_{oid}_25"),
               types.InlineKeyboardButton("50%", callback_data=f"SLSET_{oid}_50"),
               types.InlineKeyboardButton("100%", callback_data=f"SLSET_{oid}_100"),
               types.InlineKeyboardButton("🗑️ Cancel SL", callback_data=f"SLCANCEL_{oid}"))
        bot.edit_message_text(f"🛠 **Manage SL for Order {oid}:**", cid, call.message.message_id, reply_markup=mk)

    elif call.data.startswith("SLSET_"):
        parts = call.data.split("_")
        oid, pct = parts[1], float(parts[2])
        try:
            row = trades_col.find_one({"OrderID": str(oid)})
            if not row: return
            
            sl_id = str(row.get('SLOrderID', ""))
            if sl_id != "" and sl_id != "nan":
                try: USER_SESSIONS[cid].cancel_order(order_id=sl_id)
                except: pass
                
            entry = float(row['EntryPrice'])
            sl_trigger = round(entry + (entry * (pct / 100)), 1)
            sl_limit = sl_trigger + 10.0
            conf = INDICES_CONFIG[row['Index']]
            client = USER_SESSIONS[cid]
            resp = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(sl_limit), order_type="SL", quantity=str(int(row['Qty'])), validity="DAY", trading_symbol=row['TradeSymbol'], transaction_type="B", trigger_price=str(sl_trigger), amo="NO")
            
            if isinstance(resp, dict) and 'nOrdNo' in resp:
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": str(resp['nOrdNo']), "SLPrice": sl_trigger}})
                bot.edit_message_text(f"✅ SL Set at {sl_trigger}\nOrder ID: {resp['nOrdNo']}", cid, call.message.message_id)
            else: bot.send_message(cid, f"❌ SL Failed: {resp}")
        except Exception as e: bot.send_message(cid, f"❌ SL Set Error: {e}")

    elif call.data.startswith("SLCANCEL_"):
        oid = call.data.split("_")[1]
        try:
            row = trades_col.find_one({"OrderID": str(oid)})
            if not row: return
            sl_id = str(row.get('SLOrderID', ""))
            if sl_id != "" and sl_id != "nan":
                try: USER_SESSIONS[cid].cancel_order(order_id=sl_id)
                except: pass
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})
                bot.edit_message_text("🗑️ SL Cancelled.", cid, call.message.message_id)
        except Exception as e: bot.send_message(cid, f"❌ SL Cancel Error: {e}")

    elif call.data == "SL_CANCEL_ALL":
        try:
            open_sl = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"}))
            client = USER_SESSIONS[cid]
            for row in open_sl:
                sl_id = str(row.get('SLOrderID', ""))
                if sl_id != "" and sl_id != "nan":
                    try: client.cancel_order(order_id=sl_id)
                    except: pass
                    trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})
            bot.edit_message_text("🗑️ All active SL orders have been cancelled.", cid, call.message.message_id)
        except Exception as e: bot.send_message(cid, f"❌ Cancel All Err: {e}")

    elif call.data == "EXIT_ALL_CONFIRM":
        bot.edit_message_text("🚨 **INITIATING SAFE EXIT SEQUENCE...**", cid, call.message.message_id)
        try:
            open_rows = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"}))
            if not open_rows:
                bot.send_message(cid, "✅ No Open Positions.")
                return
            client = USER_SESSIONS[cid]
            
            for row in open_rows:
                sl_id = str(row.get('SLOrderID', ""))
                if sl_id != "" and sl_id != "nan":
                    try: client.cancel_order(order_id=sl_id)
                    except: pass
            
            sells = [r for r in open_rows if r['Side'] == 'SELL']
            for row in sells:
                conf = INDICES_CONFIG[row['Index']]
                ex_ltp = 0
                try:
                    q = client.quotes(instrument_tokens=[{"instrument_token": str(row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                    item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
                    ex_ltp = float(item.get('ltp', item.get('lastPrice', 0)))
                except: pass
                client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(int(row['Qty'])), validity="DAY", trading_symbol=row['TradeSymbol'], transaction_type="B", amo="NO")
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": ex_ltp}})
            
            time.sleep(0.5)
            
            buys = [r for r in open_rows if r['Side'] == 'BUY']
            for row in buys:
                conf = INDICES_CONFIG[row['Index']]
                ex_ltp = 0
                try:
                    q = client.quotes(instrument_tokens=[{"instrument_token": str(row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                    item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
                    ex_ltp = float(item.get('ltp', item.get('lastPrice', 0)))
                except: pass
                client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(int(row['Qty'])), validity="DAY", trading_symbol=row['TradeSymbol'], transaction_type="S", amo="NO")
                trades_col.update_one({"_id": row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": ex_ltp}})
                
            bot.send_message(cid, "🏁 **SAFE EXIT COMPLETE.**\nAll Sells closed before Buys.")
        except Exception as e: bot.send_message(cid, f"❌ Exit All Error: {e}")

# =========================================
# --- 8. RENDER CRASH PROTECTION & DUMMY SERVER ---
# =========================================
from http.server import BaseHTTPRequestHandler, HTTPServer

class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is active, AI logic ready, Spot price enabled, and polling!")

def keep_alive():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    print(f"🌐 Dummy web server running on port {port}")
    server.serve_forever()

def start_bot():
    print("🤖 Bot is polling...")
    bot.infinity_polling(timeout=10, long_polling_timeout=5)

if __name__ == "__main__":
    threading.Thread(target=keep_alive, daemon=True).start()
    
    while True:
        try:
            start_bot()
        except Exception as e:
            print(f"Bot crashed: {e}")
            time.sleep(10)
