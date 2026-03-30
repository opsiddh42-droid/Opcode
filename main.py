import telebot
from telebot import types
import pandas as pd
import time
import os
import threading
import requests
from neo_api_client import NeoAPI
from datetime import datetime, timedelta
from dotenv import load_dotenv
import google.generativeai as genai
from http.server import BaseHTTPRequestHandler, HTTPServer

# Load environment variables
load_dotenv()

# =========================================
# --- CONFIGURATION & AI ---
# =========================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    ai_model = genai.GenerativeModel('gemini-2.5-flash') 
else:
    print("⚠️ GEMINI_API_KEY not found! AI Analysis will not work.")

INDICES_CONFIG = {
    "NIFTY": {
        "Exchange": "nse_fo", "LotSize": 65, "StrikeGap": 50, # Updated LotSize to 25
        "MasterFile": "nse_fo_master.csv", "ChainFile": "nifty_chain.csv",
        "Url": "https://lapi.kotaksecurities.com/wso2-scrip-master/api/v1/scrip-master/csv/nse_fo"
    },
    "SENSEX": {
        "Exchange": "bse_fo", "LotSize": 20, "StrikeGap": 100, # Updated LotSize to 10
        "MasterFile": "bse_fo_master.csv", "ChainFile": "sensex_chain.csv",
        "Url": "https://lapi.kotaksecurities.com/wso2-scrip-master/api/v1/scrip-master/csv/bse_fo"
    }
}

FILES = {"USERS": "users.csv", "BOOK": "tradebook.csv"}

# --- GLOBALS ---
USER_SESSIONS, USER_DETAILS, USER_SETTINGS = {}, {}, {}
USER_STATE, PENDING_TRADE, ACTIVE_TOKENS, TEMP_REG_DATA = {}, {}, {}, {}

bot = telebot.TeleBot(BOT_TOKEN)

# =========================================
# --- 1. SETUP & CSV HANDLING ---
# =========================================
print("🚀 Starting Bot (CLEAN START WITH AI & STRANGLE)...")
USER_SESSIONS.clear()

def init_files():
    if not os.path.exists(FILES["USERS"]):
        pd.DataFrame(columns=["ChatID", "Name", "ConsumerKey", "Mobile", "UCC", "MPIN"]).to_csv(FILES["USERS"], index=False)
    if not os.path.exists(FILES["BOOK"]):
        cols = ["ChatID", "Index", "Date", "Time", "TradeSymbol", "Token", "Type", "Side", "Qty", "EntryPrice", "ExitPrice", "Status", "OrderID", "SLOrderID", "SLPrice", "InitialOI", "InitialOIChange", "LastAITime"]
        pd.DataFrame(columns=cols).to_csv(FILES["BOOK"], index=False)
init_files()

def load_users():
    try:
        if os.path.exists(FILES["USERS"]):
            df = pd.read_csv(FILES["USERS"], dtype=str)
            for _, row in df.iterrows():
                cid = int(row['ChatID'])
                USER_DETAILS[cid] = {
                    "Name": row['Name'], "Key": row['ConsumerKey'], 
                    "Mobile": row['Mobile'], "UCC": row['UCC'], "MPIN": row['MPIN']
                }
                if cid not in USER_SETTINGS: 
                    USER_SETTINGS[cid] = {"Index": "NIFTY", "ATM": None}
    except: pass
load_users()

def save_new_user(cid, data):
    new_row = {"ChatID": str(cid), "Name": data["Name"], "ConsumerKey": data["Key"], "Mobile": data["Mobile"], "UCC": data["UCC"], "MPIN": data["MPIN"]}
    try:
        df = pd.read_csv(FILES["USERS"], dtype=str)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(FILES["USERS"], index=False)
        USER_DETAILS[cid] = new_row
        USER_SETTINGS[cid] = {"Index": "NIFTY", "ATM": None}
        return True
    except: return False

def log_trade(cid, idx_name, trade_symbol, token, opt_type, side, qty, price, order_id, sl_id="", sl_prc=0, init_oi=0, init_oi_chg=0):
    now = datetime.now()
    new_row = {
        "ChatID": str(cid), "Index": idx_name,
        "Date": now.strftime("%Y-%m-%d"), "Time": now.strftime("%H:%M:%S"),
        "TradeSymbol": trade_symbol, "Token": token, "Type": opt_type, "Side": side,
        "Qty": qty, "EntryPrice": price, "ExitPrice": 0, "Status": "OPEN", "OrderID": str(order_id),
        "SLOrderID": str(sl_id), "SLPrice": sl_prc, "InitialOI": init_oi, "InitialOIChange": init_oi_chg, "LastAITime": now.strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        df = pd.read_csv(FILES["BOOK"])
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(FILES["BOOK"], index=False)
    except: pass

def update_db_order(order_id, updates_dict):
    try:
        df = pd.read_csv(FILES["BOOK"])
        idx = df.index[df['OrderID'] == str(order_id)].tolist()
        if idx:
            for k, v in updates_dict.items(): df.at[idx[0], k] = v
            df.to_csv(FILES["BOOK"], index=False)
    except: pass

def format_crore_lakh(number):
    val = abs(number)
    if val >= 10000000: return f"{number / 10000000:.2f} Cr"
    elif val >= 100000: return f"{number / 100000:.2f} L"
    else: return f"{number:,.0f}"

def place_marketable_limit(client, conf, qty, symbol, side, ltp):
    buffer = 5.0
    if side.upper() in ["B", "BUY"]:
        limit_prc = round(ltp + buffer, 1)
        t_type = "B"
    else:
        limit_prc = round(max(0.1, ltp - buffer), 1)
        t_type = "S"
    return client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(limit_prc), order_type="L", quantity=str(qty), validity="DAY", trading_symbol=symbol, transaction_type=t_type, amo="NO")

# =========================================
# --- 2. DATA ENGINE & AI ---
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
        
        ce_otm_df = df[(df['Type'] == 'CE') & (df['Strike'] >= atm)].head(5)
        pe_otm_df = df[(df['Type'] == 'PE') & (df['Strike'] <= atm)].tail(5)
        
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
        2. Analyze BOTH Total OI and OI Change to track Smart Money traps.
        3. Formatted strictly in Hinglish.
        
        🧠 **Quant Reasoning & Trap Zone:** [Explain PCR, Total OI, and OI Change shifts]
        🎯 **Definitive Bias:** [Strong Bearish / Strong Bullish / Sideways]
        ⚡ **Trade Execution Command:** [Clear command what to short]
        """
        response = ai_model.generate_content(prompt)
        return response.text
    except Exception as e: return f"❌ AI Analysis failed: {e}"

def check_master_files():
    for _, conf in INDICES_CONFIG.items():
        if not os.path.exists(conf["MasterFile"]):
            try:
                r = requests.get(conf["Url"])
                with open(conf["MasterFile"], 'wb') as f: f.write(r.content)
            except: pass

def auto_generate_chain(cid):
    idx_name = USER_SETTINGS[cid]["Index"]
    conf = INDICES_CONFIG[idx_name]
    client = USER_SESSIONS[cid]
    
    check_master_files()
    now = datetime.now()
    yy = now.strftime("%y")
    mon = now.strftime("%b").upper()
    search_sym = f"{idx_name}{yy}{mon}FUT"
    
    try:
        df = pd.read_csv(conf["MasterFile"], sep=',', header=None, low_memory=False)
        row = df[df[5] == search_sym]
        if row.empty: return False, "Future Not Found"
        fut_token = str(int(row.iloc[0, 0]))
        
        q = client.quotes(instrument_tokens=[{"instrument_token": fut_token, "exchange_segment": conf["Exchange"]}], quote_type="all")
        ltp = 0
        if q:
            item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
            ltp = float(item.get('ltp', item.get('lastPrice', 0)))
        if ltp == 0: return False, "Future Price 0"
        
        atm = round(ltp / conf["StrikeGap"]) * conf["StrikeGap"]
        USER_SETTINGS[cid]["ATM"] = f"{atm}"
        
        expiry_date_str = None
        all_ref_keys = set(df[7].astype(str).values) 
        
        for i in range(0, 45):
            test_date = now + timedelta(days=i)
            d_str = f"{test_date.strftime('%d')}{test_date.strftime('%b').upper()}{test_date.strftime('%y')}"
            check_sym = f"{idx_name}{d_str}{atm}.00CE"
            if check_sym in all_ref_keys:
                expiry_date_str = d_str
                break
        if not expiry_date_str: return False, "Expiry Not Found"
        
        prefix = f"{idx_name}{expiry_date_str}"
        relevant = df[df[7].str.startswith(prefix, na=False)]
        strikes = [atm + (i * conf["StrikeGap"]) for i in range(-30, 31)]
        new_list = []
        
        for index, r in relevant.iterrows():
            ref_key = str(r[7]).strip()
            trd_sym = str(r[5]).strip()
            token = str(int(r[0]))
            for stk in strikes:
                if f"{stk}.00CE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "RefKey": ref_key, "Token": token, "Type": "CE", "Strike": stk, "LTP": 0.0, "OI": 0, "OI_Change": 0})
                elif f"{stk}.00PE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "RefKey": ref_key, "Token": token, "Type": "PE", "Strike": stk, "LTP": 0.0, "OI": 0, "OI_Change": 0})

        ACTIVE_TOKENS[cid] = new_list
        return True, f"ATM: {atm} | Exp: {expiry_date_str}"
    except Exception as e: return False, f"Err: {str(e)}"

def fetch_data_for_user(cid):
    if cid not in USER_SESSIONS: return False
    if cid not in ACTIVE_TOKENS or not ACTIVE_TOKENS[cid]: auto_generate_chain(cid)
        
    client = USER_SESSIONS[cid]
    idx_name = USER_SETTINGS[cid]["Index"]
    conf = INDICES_CONFIG[idx_name]
    try:
        all_tokens = ACTIVE_TOKENS[cid]
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
                    oi_chg_val = int(item.get('chng_in_oi') or item.get('netChange') or 0)
                    live_map[tk] = {'ltp': ltp_val, 'oi': oi_val, 'oi_chg': oi_chg_val}
        
        clean_data = []
        for item in all_tokens:
            d = live_map.get(item['Token'], {'ltp': 0, 'oi': 0, 'oi_chg': 0})
            item['LTP'] = d['ltp']; item['OI'] = d['oi']; item['OI_Change'] = d['oi_chg']
            clean_data.append(item)
            
        ACTIVE_TOKENS[cid] = clean_data 
        return True
    except: return False

# =========================================
# --- 3. BACKGROUND THREADS ---
# =========================================
def background_tasks():
    while True:
        try:
            # 1. Update Market Data
            for cid in list(USER_SESSIONS.keys()): fetch_data_for_user(cid)
            
            # 2. SL Monitor (from CSV)
            if os.path.exists(FILES["BOOK"]):
                df = pd.read_csv(FILES["BOOK"])
                open_sl_df = df[(df['Status'] == 'OPEN') & (df['SLOrderID'].notna()) & (df['SLOrderID'] != '')]
                
                for _, row in open_sl_df.iterrows():
                    cid = int(row['ChatID'])
                    if cid in USER_SESSIONS:
                        client = USER_SESSIONS[cid]
                        sl_id = str(row['SLOrderID'])
                        order_hist = client.order_history(order_id=sl_id)
                        if order_hist and isinstance(order_hist, list):
                            status = order_hist[0].get('status', '').upper()
                            if status in ['COMPLETE', 'FILLED']:
                                update_db_order(row['OrderID'], {"Status": "CLOSED", "ExitPrice": float(row['SLPrice'])})
                                bot.send_message(cid, f"🎯 **SL HIT:** {row['TradeSymbol']}")
                            elif status in ['REJECTED', 'CANCELLED']:
                                update_db_order(row['OrderID'], {"SLOrderID": "", "SLPrice": 0})
            
            # 3. Hourly AI Risk Monitor
            now = datetime.now()
            if os.path.exists(FILES["BOOK"]) and GEMINI_API_KEY:
                df = pd.read_csv(FILES["BOOK"])
                open_trades = df[df['Status'] == 'OPEN']
                
                for cid_str, group in open_trades.groupby('ChatID'):
                    cid = int(cid_str)
                    if cid not in USER_SESSIONS: continue
                    
                    needs_analysis = []
                    for idx, t in group.iterrows():
                        try: last_time = datetime.strptime(str(t['LastAITime']), "%Y-%m-%d %H:%M:%S")
                        except: last_time = now - timedelta(hours=2)
                        if (now - last_time).total_seconds() >= 3600:
                            needs_analysis.append((idx, t))
                    
                    if needs_analysis:
                        fetch_data_for_user(cid)
                        tdf = pd.DataFrame(ACTIVE_TOKENS.get(cid, []))
                        if tdf.empty: continue
                        
                        prompt = f"Act as a Risk Manager assessing OPEN positions 1 hour after entry.\n**Your Open Positions:**\n"
                        for idx, t in needs_analysis:
                            tk = str(t['Token'])
                            curr_row = tdf[tdf['Token'] == tk]
                            if not curr_row.empty:
                                c_ltp = curr_row.iloc[0]['LTP']
                                c_oi = curr_row.iloc[0]['OI']
                                c_chg = curr_row.iloc[0]['OI_Change']
                                prompt += f"- {t['Side']} {t['TradeSymbol']}: Entry={t['EntryPrice']}, LTP={c_ltp} | Initial OI={t.get('InitialOI',0)} (Chg: {t.get('InitialOIChange',0)}) -> Current OI={c_oi} (Chg: {c_chg})\n"
                            
                            df = pd.read_csv(FILES["BOOK"])
                            df.at[idx, 'LastAITime'] = now.strftime("%Y-%m-%d %H:%M:%S")
                            df.to_csv(FILES["BOOK"], index=False)
                            
                        prompt += "\nHas the market turned against the seller? Are these positions safe based on OI shifts? Give a brief final verdict: '🟢 SAFE' or '🔴 DANGER - EXIT'."
                        try:
                            response = ai_model.generate_content(prompt)
                            bot.send_message(cid, f"⏳ **1-Hour Position AI Check:**\n\n{response.text}")
                        except: pass

        except Exception as e: print(f"BG Task Err: {e}")
        time.sleep(60) 
threading.Thread(target=background_tasks, daemon=True).start()

# =========================================
# --- 4. MENUS ---
# =========================================
def get_main_menu(cid):
    idx = USER_SETTINGS[cid]["Index"]
    mk = types.ReplyKeyboardMarkup(row_width=2, resize_keyboard=True)
    mk.add(types.KeyboardButton("🤖 AI Market Analysis"))
    mk.add(types.KeyboardButton("🔄 Refresh Data"))
    mk.add(types.KeyboardButton(f"🚀 New Trade ({idx})"), types.KeyboardButton("💰 P&L"))
    mk.add(types.KeyboardButton("⚡ Auto Strangle"))
    mk.add(types.KeyboardButton("📊 OI Data"), types.KeyboardButton("🔄 Change ATM (Auto)"))
    mk.add(types.KeyboardButton("🛑 Set/Clear SL"), types.KeyboardButton(f"Index: {idx} 🔀"))
    mk.add(types.KeyboardButton("🚪 Logout"), types.KeyboardButton("🚨 EXIT ALL"))
    return mk

def get_login_btn(): return types.ReplyKeyboardMarkup(resize_keyboard=True).add(types.KeyboardButton("🔐 Login Now"))

# =========================================
# --- 5. REGISTRATION & COMMANDS ---
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
    else: bot.send_message(cid, "❌ User not found. Type /start to register.")

@bot.message_handler(commands=['start'])
def cmd_start(message):
    cid = message.chat.id
    load_users()
    if cid in USER_DETAILS:
        if cid in USER_SESSIONS: bot.send_message(cid, f"👋 Ready! Index: **{USER_SETTINGS[cid]['Index']}**", reply_markup=get_main_menu(cid))
        else: bot.send_message(cid, f"👋 Welcome back **{USER_DETAILS[cid]['Name']}**!\nClick below to Login.", reply_markup=get_login_btn())
    else:
        USER_STATE[cid] = "REG_NAME"; TEMP_REG_DATA[cid] = {}
        bot.send_message(cid, "🆕 **New User Registration**\nEnter Name:")

@bot.message_handler(func=lambda m: (USER_STATE.get(m.chat.id) or "").startswith("REG_"))
def reg_flow(m):
    cid, text = m.chat.id, m.text.strip()
    st = USER_STATE[cid]
    if st == "REG_NAME": TEMP_REG_DATA[cid]['Name'] = text; USER_STATE[cid] = "REG_KEY"; bot.send_message(cid, "Enter Consumer Key:")
    elif st == "REG_KEY": TEMP_REG_DATA[cid]['Key'] = text; USER_STATE[cid] = "REG_MOB"; bot.send_message(cid, "Enter Mobile (+91...):")
    elif st == "REG_MOB": TEMP_REG_DATA[cid]['Mobile'] = text; USER_STATE[cid] = "REG_UCC"; bot.send_message(cid, "Enter UCC:")
    elif st == "REG_UCC": TEMP_REG_DATA[cid]['UCC'] = text; USER_STATE[cid] = "REG_MPIN"; bot.send_message(cid, "Enter MPIN:")
    elif st == "REG_MPIN":
        TEMP_REG_DATA[cid]['MPIN'] = text
        if save_new_user(cid, TEMP_REG_DATA[cid]): bot.send_message(cid, "✅ Registered! Click Login.", reply_markup=get_login_btn())
        USER_STATE[cid] = None

@bot.message_handler(func=lambda m: m.text == "🔐 Login Now")
def do_login_btn(m): cmd_login_command(m)

# =========================================
# --- 6. MAIN LOGIC ---
# =========================================
@bot.message_handler(func=lambda message: True)
def main_handler(message):
    cid = message.chat.id
    text = message.text.strip()
    state = USER_STATE.get(cid)

    if state == "WAIT_TOTP":
        try:
            u = USER_DETAILS[cid]
            cl = NeoAPI(consumer_key=u['Key'], environment='prod')
            cl.totp_login(mobile_number=u['Mobile'], ucc=u['UCC'], totp=text)
            cl.totp_validate(mpin=u['MPIN'])
            USER_SESSIONS[cid] = cl
            check_master_files()
            USER_STATE[cid] = None
            
            idx = USER_SETTINGS[cid]["Index"]
            bot.send_message(cid, f"✅ Logged In! Index: {idx}\n⏳ Calculating ATM...", reply_markup=get_main_menu(cid))
            
            success, msg = auto_generate_chain(cid)
            if success: bot.send_message(cid, f"✅ {msg}")
            else: bot.send_message(cid, f"⚠️ Auto ATM Error: {msg}")
        except Exception as e:
            bot.send_message(cid, f"❌ Login Failed: {e}", reply_markup=get_login_btn())
            USER_STATE[cid] = None
        return

    if cid not in USER_SESSIONS: return

    if text == "🤖 AI Market Analysis":
        bot.send_message(cid, "⏳ *AI analyzing Total OI & Live OI Change...*", parse_mode="Markdown")
        fetch_data_for_user(cid) 
        bot.send_message(cid, f"🤖 **Gemini AI:**\n\n{get_ai_analysis(cid)}")

    elif "Index:" in text:
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🔵 NIFTY", callback_data="SET_NIFTY"), types.InlineKeyboardButton("🔴 SENSEX", callback_data="SET_SENSEX"))
        bot.send_message(cid, "Select Index:", reply_markup=mk)

    elif text == "🔄 Refresh Data":
        bot.send_message(cid, "⏳ Updating Data...")
        auto_generate_chain(cid)
        if fetch_data_for_user(cid): bot.send_message(cid, f"✅ Updated: {datetime.now().strftime('%H:%M:%S')}")
        else: bot.send_message(cid, "❌ Failed.")

    elif "Change ATM" in text:
        bot.send_message(cid, "⚙️ Auto-Detecting Best ATM...")
        success, msg = auto_generate_chain(cid)
        if success:
             fetch_data_for_user(cid)
             bot.send_message(cid, f"✅ {msg}")
        else: bot.send_message(cid, f"❌ Failed: {msg}")

    elif text == "💰 P&L":
        try:
            df = pd.read_csv(FILES["BOOK"])
            df['ChatID'] = df['ChatID'].astype(str)
            my_open = df[(df['ChatID'] == str(cid)) & (df['Status'] == 'OPEN')]
            
            if my_open.empty: return bot.send_message(cid, "✅ No Open Trades.")

            tokens = []
            for _, r in my_open.iterrows():
                exch = INDICES_CONFIG[r['Index']]['Exchange']
                tokens.append({"instrument_token": str(r['Token']), "exchange_segment": exch})
            
            q = USER_SESSIONS[cid].quotes(instrument_tokens=tokens, quote_type="all")
            live_ltp = {}
            if q:
                for item in (q if isinstance(q, list) else q.get('data', [])):
                    live_ltp[str(item.get('tk') or item.get('exchange_token'))] = float(item.get('ltp', 0))
            
            msg = "💰 **P&L Report**\n"
            total = 0
            for _, r in my_open.iterrows():
                curr = live_ltp.get(str(r['Token']), 0)
                if curr == 0: continue
                qty = int(r['Qty'])
                entry = float(r['EntryPrice'])
                if r['Side'] == 'BUY': pnl = (curr - entry) * qty
                else: pnl = (entry - curr) * qty
                total += pnl
                msg += f"{r['TradeSymbol']} ({r['Side']}): {pnl:.0f}\n"
            msg += f"----------------\n**Total: {total:.0f}**"
            bot.send_message(cid, msg)
        except Exception as e: bot.send_message(cid, f"P&L Error: {e}")

    elif text == "📊 OI Data":
        if cid not in ACTIVE_TOKENS: auto_generate_chain(cid)
        fetch_data_for_user(cid)
        USER_STATE[cid] = "WAIT_OI_RANGE"
        bot.send_message(cid, "🔢 **Range?** (Ex: 3)")

    elif text == "🛑 Set/Clear SL":
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🎯 Place New SL", callback_data="SL_MENU_PLACE"), types.InlineKeyboardButton("🗑️ Clear SL", callback_data="SL_MENU_CLEAR"))
        bot.send_message(cid, "⚙️ **Manage Stop Loss:**", reply_markup=mk)

    elif "New Trade" in text:
        idx = USER_SETTINGS[cid]["Index"]
        if cid not in ACTIVE_TOKENS or not ACTIVE_TOKENS[cid]:
             bot.send_message(cid, "⏳ Checking ATM...")
             auto_generate_chain(cid)
        fetch_data_for_user(cid)
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("📈 Call (CE)", callback_data="TRADE_CE"), types.InlineKeyboardButton("📉 Put (PE)", callback_data="TRADE_PE"))
        bot.send_message(cid, f"🚀 **{idx} Trade**\nATM: {USER_SETTINGS[cid].get('ATM', 'N/A')}\nSelect:", reply_markup=mk)

    elif text == "⚡ Auto Strangle":
        idx = USER_SETTINGS[cid]["Index"]
        success = fetch_data_for_user(cid)
        if not success: return bot.send_message(cid, "❌ Failed to fetch data.")
        
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
            sl_multiplier = 4.0 if idx == "SENSEX" else 3.0
            
            bot.send_message(cid, "⏳ Firing Limit Orders...")
            resp_ce = place_marketable_limit(client, conf, qty, ce['TradeSymbol'], "S", ce['LTP'])
            resp_pe = place_marketable_limit(client, conf, qty, pe['TradeSymbol'], "S", pe['LTP'])
            
            msg = "✅ **Strangle Executed:**\n"
            
            if isinstance(resp_ce, dict) and 'nOrdNo' in resp_ce:
                ce_oid = str(resp_ce['nOrdNo'])
                ce_sl_trigger = round(ce['LTP'] * sl_multiplier, 1)
                ce_sl_limit = ce_sl_trigger + 10.0
                resp_sl = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(ce_sl_limit), order_type="SL-L", quantity=str(qty), validity="DAY", trading_symbol=ce['TradeSymbol'], transaction_type="B", trigger_price=str(ce_sl_trigger), amo="NO")
                sl_oid = str(resp_sl['nOrdNo']) if isinstance(resp_sl, dict) and 'nOrdNo' in resp_sl else ""
                log_trade(cid, idx, ce['TradeSymbol'], ce['Token'], "CE", "SELL", qty, ce['LTP'], ce_oid, sl_oid, ce_sl_trigger, ce.get('OI',0), ce.get('OI_Change',0))
                msg += f"🔴 CE {ce['LTP']} (SL: {ce_sl_trigger})\n"
                
            if isinstance(resp_pe, dict) and 'nOrdNo' in resp_pe:
                pe_oid = str(resp_pe['nOrdNo'])
                pe_sl_trigger = round(pe['LTP'] * sl_multiplier, 1)
                pe_sl_limit = pe_sl_trigger + 10.0
                resp_sl = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(pe_sl_limit), order_type="SL-L", quantity=str(qty), validity="DAY", trading_symbol=pe['TradeSymbol'], transaction_type="B", trigger_price=str(pe_sl_trigger), amo="NO")
                sl_oid = str(resp_sl['nOrdNo']) if isinstance(resp_sl, dict) and 'nOrdNo' in resp_sl else ""
                log_trade(cid, idx, pe['TradeSymbol'], pe['Token'], "PE", "SELL", qty, pe['LTP'], pe_oid, sl_oid, pe_sl_trigger, pe.get('OI',0), pe.get('OI_Change',0))
                msg += f"🔴 PE {pe['LTP']} (SL: {pe_sl_trigger})"

            bot.send_message(cid, msg)
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ Err: {e}")

    elif text == "🚨 EXIT ALL":
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("✅ YES, NUKE IT (BUY FIRST)", callback_data="EXIT_CONFIRM"), types.InlineKeyboardButton("❌ CANCEL", callback_data="EXIT_CANCEL"))
        bot.send_message(cid, f"⚠️ Close ALL positions? (Buys will close before Sells)", reply_markup=mk)

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
            qty = lots * conf["LotSize"]
            PENDING_TRADE[cid]["Qty"] = qty
            
            fetch_data_for_user(cid)
            df = pd.DataFrame(ACTIVE_TOKENS[cid])
            target = PENDING_TRADE[cid]["Target"]
            opt_type = PENDING_TRADE[cid]["Type"]
            
            df = df[(df['Type'] == opt_type) & (df['LTP'] > 0)]
            if df.empty: return bot.send_message(cid, "❌ Prices are 0 or No Data.")

            main = df[df['LTP'] <= target].sort_values('LTP', ascending=False)
            main = main.iloc[0] if not main.empty else df.sort_values('LTP', ascending=True).iloc[0]
            
            if opt_type == 'CE': pool = df[df['Strike'] > main['Strike']].copy()
            else: pool = df[df['Strike'] < main['Strike']].copy()
            
            if pool.empty: return bot.send_message(cid, "❌ Hedge not found.")
            
            pool['diff'] = abs(pool['LTP'] - (main['LTP'] * 0.20))
            hedge = pool.sort_values(by=['diff', 'LTP']).iloc[0]
            
            PENDING_TRADE[cid]["Main"] = main.to_dict()
            PENDING_TRADE[cid]["Hedge"] = hedge.to_dict()
            
            msg = (f"⚡ **CONFIRM {idx} TRADE**\n📦 Lots: {lots} (Qty: {qty})\n"
                   f"🔴 SELL: {main['TradeSymbol']} ({main['LTP']})\n"
                   f"🟢 BUY: {hedge['TradeSymbol']} ({hedge['LTP']})\nExecute?")
            mk = types.InlineKeyboardMarkup()
            mk.add(types.InlineKeyboardButton("🔥 FIRE", callback_data="EXECUTE_TRADE"), types.InlineKeyboardButton("❌ CANCEL", callback_data="CANCEL_TRADE"))
            bot.send_message(cid, msg, reply_markup=mk)
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ Error: {e}")

    elif state == "WAIT_OI_RANGE":
        try:
            n = int(text)
            fetch_data_for_user(cid)
            df = pd.DataFrame(ACTIVE_TOKENS[cid])
            df['OI'] = df['OI'].fillna(0).astype(int)
            df['OI_Change'] = df['OI_Change'].fillna(0).astype(int)

            ce_df = df[df['Type'] == 'CE'].sort_values('Strike').reset_index(drop=True)
            pe_df = df[df['Type'] == 'PE'].sort_values('Strike').reset_index(drop=True)

            mid = len(ce_df) // 2 
            sel_pe = pe_df.iloc[max(0, mid - n) : mid + 1] 
            sel_ce = ce_df.iloc[mid : min(len(ce_df), mid + n + 1)]

            pe_oi = sel_pe['OI'].sum()
            ce_oi = sel_ce['OI'].sum()
            pe_chg = sel_pe['OI_Change'].sum()
            ce_chg = sel_ce['OI_Change'].sum()
            
            msg = (f"📊 **Live OI & Change (ATM ±{n})**\n\n"
                   f"🛡️ **PE (Support):**\nTotal OI: {format_crore_lakh(pe_oi)}\nFresh Chg: {format_crore_lakh(pe_chg)}\n\n"
                   f"⚔️ **CE (Resistance):**\nTotal OI: {format_crore_lakh(ce_oi)}\nFresh Chg: {format_crore_lakh(ce_chg)}\n\n"
                   f"🔥 **Diff (PE - CE):** {format_crore_lakh(pe_oi - ce_oi)}")
                   
            bot.send_message(cid, msg, parse_mode="Markdown", reply_markup=get_main_menu(cid))
            USER_STATE[cid] = None
        except Exception as e: bot.send_message(cid, f"❌ OI Error: {e}")

# =========================================
# --- 7. CALLBACKS ---
# =========================================
@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    cid = call.message.chat.id
    if cid not in USER_SESSIONS: return
    
    if call.data == "SL_MENU_PLACE":
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("25%", callback_data="SL_25"), types.InlineKeyboardButton("50%", callback_data="SL_50"), types.InlineKeyboardButton("100%", callback_data="SL_100"))
        bot.edit_message_text("🎯 **Select Stop Loss %:**", cid, call.message.message_id, reply_markup=mk)

    elif call.data == "SL_MENU_CLEAR":
        bot.edit_message_text("🗑️ Clearing Pending SL Orders...", cid, call.message.message_id)
        try:
            df = pd.read_csv(FILES["BOOK"])
            df['ChatID'] = df['ChatID'].astype(str)
            curr_idx = USER_SETTINGS[cid]["Index"]
            open_pos = df[(df['ChatID'] == str(cid)) & (df['Status'] == 'OPEN') & (df['Index'] == curr_idx)]
            open_symbols = open_pos['TradeSymbol'].tolist()
            client = USER_SESSIONS[cid]
            order_book = client.order_report()
            
            count = 0
            if order_book and 'data' in order_book:
                for order in order_book['data']:
                    status = order.get('order_status', '').lower()
                    sym = order.get('trading_symbol', '')
                    if sym in open_symbols and status in ['pending', 'open', 'trigger_pending', 'trig']:
                        oid = order.get('nOrdNo')
                        try:
                            client.cancel_order(order_id=oid, amo="NO")
                            count += 1
                        except: pass
            
            # Clear from DB
            for _, r in open_pos.iterrows(): update_db_order(r['OrderID'], {"SLOrderID": "", "SLPrice": 0})
            bot.send_message(cid, f"✅ **Cleared {count} SL Orders!**")
        except Exception as e: bot.send_message(cid, f"Clear Failed: {e}")

    elif call.data.startswith("SL_") and "MENU" not in call.data:
        pct = int(call.data.split("_")[1])
        try:
            df = pd.read_csv(FILES["BOOK"])
            df['ChatID'] = df['ChatID'].astype(str)
            curr_idx = USER_SETTINGS[cid]["Index"]
            sells = df[(df['ChatID'] == str(cid)) & (df['Status'] == 'OPEN') & (df['Side'] == 'SELL') & (df['Index'] == curr_idx)]
            conf = INDICES_CONFIG[curr_idx]
            client = USER_SESSIONS[cid]
            count = 0
            for _, row in sells.iterrows():
                entry = float(row['EntryPrice'])
                qty = str(row['Qty'])
                trigger = entry + (entry * pct / 100)
                limit = trigger + 10.0 
                
                # Cancel old SL if exists
                if str(row['SLOrderID']) != "nan" and row['SLOrderID']:
                    try: client.cancel_order(order_id=str(row['SLOrderID']), amo="NO")
                    except: pass
                    
                try:
                    resp = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(round(limit, 1)), order_type="SL-L", trigger_price=str(round(trigger, 1)), quantity=qty, validity="DAY", trading_symbol=row['TradeSymbol'], transaction_type="B", amo="NO")
                    if isinstance(resp, dict) and 'nOrdNo' in resp:
                        update_db_order(row['OrderID'], {"SLOrderID": str(resp['nOrdNo']), "SLPrice": trigger})
                        count += 1
                except: pass
            bot.edit_message_text(f"✅ Modified SL for {count} positions!", cid, call.message.message_id)
        except Exception as e: bot.send_message(cid, f"SL Failed: {e}")

    elif call.data == "SET_NIFTY":
        USER_SETTINGS[cid]["Index"] = "NIFTY"
        ACTIVE_TOKENS[cid] = [] 
        bot.delete_message(cid, call.message.message_id)
        bot.send_message(cid, "✅ Index: NIFTY. Auto-ATM...", reply_markup=get_main_menu(cid))
        auto_generate_chain(cid)
    elif call.data == "SET_SENSEX":
        USER_SETTINGS[cid]["Index"] = "SENSEX"
        ACTIVE_TOKENS[cid] = []
        bot.delete_message(cid, call.message.message_id)
        bot.send_message(cid, "✅ Index: SENSEX. Auto-ATM...", reply_markup=get_main_menu(cid))
        auto_generate_chain(cid)

    elif call.data in ["TRADE_CE", "TRADE_PE"]:
        PENDING_TRADE[cid] = {"Type": "CE" if "CE" in call.data else "PE"}
        USER_STATE[cid] = "WAIT_PREMIUM"
        bot.send_message(cid, "💰 Enter Sell Premium Target:")

    elif call.data == "EXECUTE_TRADE":
        try:
            bot.edit_message_text("⏳ Sending Marketable Limit Orders...", cid, call.message.message_id)
            t_data, idx, client = PENDING_TRADE[cid], USER_SETTINGS[cid]["Index"], USER_SESSIONS[cid]
            conf, qty = INDICES_CONFIG[idx], int(t_data["Qty"])
            
            resp_h = place_marketable_limit(client, conf, qty, t_data["Hedge"]["TradeSymbol"], "B", t_data["Hedge"]["LTP"])
            if not isinstance(resp_h, dict) or 'nOrdNo' not in resp_h: return bot.edit_message_text("❌ Hedge Failed.", cid, call.message.message_id)
            time.sleep(0.3)
            resp_m = place_marketable_limit(client, conf, qty, t_data["Main"]["TradeSymbol"], "S", t_data["Main"]["LTP"])
            
            if isinstance(resp_m, dict) and 'nOrdNo' in resp_m:
                m_oid, entry = str(resp_m['nOrdNo']), float(t_data["Main"]["LTP"])
                log_trade(cid, idx, t_data["Hedge"]["TradeSymbol"], t_data["Hedge"]["Token"], t_data["Type"], "BUY", qty, t_data["Hedge"]["LTP"], resp_h['nOrdNo'], init_oi=t_data["Hedge"].get("OI",0), init_oi_chg=t_data["Hedge"].get("OI_Change",0))
                
                sl_trg = round(entry * 3.0, 1) 
                sl_lim = sl_trg + 10.0
                resp_sl = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price=str(sl_lim), order_type="SL-L", quantity=str(qty), validity="DAY", trading_symbol=t_data["Main"]["TradeSymbol"], transaction_type="B", trigger_price=str(sl_trg), amo="NO")
                sl_oid = str(resp_sl['nOrdNo']) if isinstance(resp_sl, dict) and 'nOrdNo' in resp_sl else ""
                
                log_trade(cid, idx, t_data["Main"]["TradeSymbol"], t_data["Main"]["Token"], t_data["Type"], "SELL", qty, entry, m_oid, sl_oid, sl_trg, t_data["Main"].get("OI",0), t_data["Main"].get("OI_Change",0))
                bot.edit_message_text(f"✅ Trade Placed!\nMain: {m_oid} | SL: {sl_trg}", cid, call.message.message_id)
            else: bot.edit_message_text("⚠️ Hedge PLACED but Main FAILED.", cid, call.message.message_id)
        except Exception as e: bot.send_message(cid, f"❌ Execution Exception: {e}")

    elif call.data == "CANCEL_TRADE": bot.edit_message_text("🚫 Cancelled.", cid, call.message.message_id)
    
    # --- EXIT ALL LOGIC: CUSTOM RULE (BUY FIRST, SELL LATER) ---
    elif call.data == "EXIT_CONFIRM":
        bot.edit_message_text("🚨 Processing Safe Exit Sequence...", cid, call.message.message_id)
        try:
            df = pd.read_csv(FILES["BOOK"])
            df['ChatID'] = df['ChatID'].astype(str)
            open_rows = df[(df['ChatID'] == str(cid)) & (df['Status'] == 'OPEN')]
            client = USER_SESSIONS[cid]
            
            for _, row in open_rows.iterrows():
                if str(row['SLOrderID']) != "nan" and row['SLOrderID']:
                    try: client.cancel_order(order_id=str(row['SLOrderID']), amo="NO")
                    except: pass
            
            # STEP 1: EXIT BUYS FIRST
            buys = open_rows[open_rows['Side'] == 'BUY']
            for idx, row in buys.iterrows():
                conf = INDICES_CONFIG[row['Index']]
                try:
                    q = client.quotes(instrument_tokens=[{"instrument_token": str(row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                    item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
                    ex_ltp = float(item.get('ltp', item.get('lastPrice', 0)))
                    place_marketable_limit(client, conf, int(row['Qty']), row['TradeSymbol'], "S", ex_ltp)
                    df.at[idx, 'Status'] = 'CLOSED'
                    df.at[idx, 'ExitPrice'] = ex_ltp
                except: pass
            
            time.sleep(0.5)
            
            # STEP 2: EXIT SELLS
            sells = open_rows[open_rows['Side'] == 'SELL']
            for idx, row in sells.iterrows():
                conf = INDICES_CONFIG[row['Index']]
                try:
                    q = client.quotes(instrument_tokens=[{"instrument_token": str(row['Token']), "exchange_segment": conf["Exchange"]}], quote_type="all")
                    item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
                    ex_ltp = float(item.get('ltp', item.get('lastPrice', 0)))
                    place_marketable_limit(client, conf, int(row['Qty']), row['TradeSymbol'], "B", ex_ltp)
                    df.at[idx, 'Status'] = 'CLOSED'
                    df.at[idx, 'ExitPrice'] = ex_ltp
                except: pass
            
            df.to_csv(FILES["BOOK"], index=False)
            bot.send_message(cid, "🏁 **SAFE EXIT COMPLETE.**\nAll Buys closed before Sells.")
        except Exception as e: bot.send_message(cid, f"Exit Err: {e}")

    elif call.data == "EXIT_CANCEL": bot.edit_message_text("✅ Exit Cancelled.", cid, call.message.message_id)

# =========================================
# --- WEB SERVER ---
# =========================================
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is fully active!")

if __name__ == "__main__":
    threading.Thread(target=lambda: HTTPServer(('0.0.0.0', int(os.environ.get("PORT", 8080))), DummyHandler).serve_forever(), daemon=True).start()
    while True:
        try: bot.polling(none_stop=True, timeout=60)
        except Exception as e: time.sleep(5)
