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

# =========================================
# --- CONFIGURATION & MONGODB ---
# =========================================
# Secrets fetched from Render Environment Variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# MongoDB Setup
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["tradingbot"]

# Collections
users_col = db["users"]
trades_col = db["trades"]
fo_master_col = db["fo_master"]

INDICES_CONFIG = {
    "NIFTY": {
        "Exchange": "nse_fo", "LotSize": 65, "StrikeGap": 50,
        "Url": "https://lapi.kotaksecurities.com/wso2-scrip-master/api/v1/scrip-master/csv/nse_fo"
    },
    "SENSEX": {
        "Exchange": "bse_fo", "LotSize": 20, "StrikeGap": 100,
        "Url": "https://lapi.kotaksecurities.com/wso2-scrip-master/api/v1/scrip-master/csv/bse_fo"
    }
}

# --- GLOBALS ---
USER_SESSIONS = {}
USER_DETAILS = {}
USER_SETTINGS = {}
USER_STATE = {}
PENDING_TRADE = {}
ACTIVE_TOKENS = {} 

# =========================================
# --- 1. SETUP & MONGODB MANAGEMENT ---
# =========================================
print("🚀 Starting Advanced Algo Bot with Auto-SL Monitoring (MongoDB Edition)...")
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
# =========================================
# --- 2. DATA ENGINE & MONITOR ---
# =========================================
def check_master_files():
    for idx_name, conf in INDICES_CONFIG.items():
        # Check if master data already exists in MongoDB for this index
        if fo_master_col.count_documents({"IndexName": idx_name}) == 0:
            try:
                r = requests.get(conf["Url"])
                # Read directly from memory, no local file saved
                df = pd.read_csv(io.StringIO(r.text), sep=',', header=None, low_memory=False)
                df["IndexName"] = idx_name
                # MongoDB requires string keys; Pandas default header=None uses integers
                df.columns = df.columns.astype(str) 
                
                records = df.to_dict("records")
                if records:
                    fo_master_col.insert_many(records)
            except: pass

def auto_generate_chain(cid):
    idx_name = USER_SETTINGS[cid]["Index"]
    conf = INDICES_CONFIG[idx_name]
    if cid not in USER_SESSIONS: return False, "No Session"
    client = USER_SESSIONS[cid]
    check_master_files()
    now = datetime.now()
    yy = now.strftime("%y")
    mon = now.strftime("%b").upper()
    search_sym = f"{idx_name}{yy}{mon}FUT"
    try:
        # Fetch Master Data from MongoDB instead of CSV
        cursor = fo_master_col.find({"IndexName": idx_name})
        df = pd.DataFrame(list(cursor))
        if df.empty: return False, "Master Data Empty"

        # Using string "5" instead of int 5 because MongoDB keys are strings
        row = df[df["5"] == search_sym]
        if row.empty: return False, "Future Not Found"
        fut_token = str(int(row.iloc[0]["0"]))
        
        q = client.quotes(instrument_tokens=[{"instrument_token": fut_token, "exchange_segment": conf["Exchange"]}], quote_type="all")
        ltp = 0
        if q:
            item = q[0] if isinstance(q, list) else q.get('data', [{}])[0]
            ltp = float(item.get('ltp', item.get('lastPrice', 0)))
        if ltp == 0: return False, "Future Price 0"
        atm = round(ltp / conf["StrikeGap"]) * conf["StrikeGap"]
        USER_SETTINGS[cid]["ATM"] = f"{atm}"
        expiry_date_str = None
        all_ref_keys = set(df["7"].astype(str).values) 
        for i in range(0, 45):
            test_date = now + timedelta(days=i)
            d_str = f"{test_date.strftime('%d')}{test_date.strftime('%b').upper()}{test_date.strftime('%y')}"
            check_sym = f"{idx_name}{d_str}{atm}.00CE"
            if check_sym in all_ref_keys:
                expiry_date_str = d_str
                break
        if not expiry_date_str: return False, "Expiry Not Found"
        prefix = f"{idx_name}{expiry_date_str}"
        relevant = df[df["7"].str.startswith(prefix, na=False)]
        strikes = [atm + (i * conf["StrikeGap"]) for i in range(-20, 21)]
        new_list = []
        for index, r in relevant.iterrows():
            ref_key = str(r["7"]).strip()
            trd_sym = str(r["5"]).strip()
            token = str(int(r["0"]))
            for stk in strikes:
                if f"{stk}.00CE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "RefKey": ref_key, "Token": token, "Type": "CE", "Strike": stk, "LTP": 0.0, "OI": 0})
                elif f"{stk}.00PE" in ref_key:
                     new_list.append({"TradeSymbol": trd_sym, "RefKey": ref_key, "Token": token, "Type": "PE", "Strike": stk, "LTP": 0.0, "OI": 0})
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
                    live_map[tk] = {'ltp': ltp_val, 'oi': oi_val}
        for item in all_tokens:
            d = live_map.get(item['Token'], {'ltp': 0, 'oi': 0})
            item['LTP'] = d['ltp']; item['OI'] = d['oi']
        ACTIVE_TOKENS[cid] = all_tokens 
        return True
    except: return False

def sl_monitor_thread():
    while True:
        try:
            # Fetch ONLY open trades with an active SL from MongoDB directly
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
                            # Update Main leg to CLOSED in DB
                            trades_col.update_one({"_id": row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": row['SLPrice']}})
                            bot.send_message(cid, f"🎯 **SL HIT:** {row['TradeSymbol']}\nClosing Hedge Automatically...")
                            
                            # Find Hedge position in DB
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
                                # Update Hedge leg to CLOSED in DB
                                trades_col.update_one({"_id": h_row["_id"]}, {"$set": {"Status": "CLOSED", "ExitPrice": h_ltp}})
                                
                        elif status in ['REJECTED', 'CANCELLED']:
                            # Clear SL details in DB
                            trades_col.update_one({"_id": row["_id"]}, {"$set": {"SLOrderID": "", "SLPrice": 0}})

        except Exception as e: print(f"SL Monitor Error: {e}")
        time.sleep(600)

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
    mk.add(types.KeyboardButton("🔄 Refresh Data"))
    mk.add(types.KeyboardButton(f"🚀 New Trade ({idx})"), types.KeyboardButton("💰 P&L"))
    mk.add(types.KeyboardButton("📊 OI Data"), types.KeyboardButton("🔄 Change ATM (Auto)"))
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
TEMP_REG_DATA = {}
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
            bot.send_message(cid, "❌ Database Error! Render logs check karein.")
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
            # Smart Key Checker updated here
            api_key = u.get('Key', u.get('ConsumerKey'))
            
            if not api_key:
                bot.send_message(cid, "❌ API Key error. Type /start to register again.")
                USER_STATE[cid] = None
                return

            cl = NeoAPI(consumer_key=api_key, environment='prod')
            cl.totp_login(mobile_number=u.get('Mobile'), ucc=u.get('UCC'), totp=text)
            cl.totp_validate(mpin=u.get('MPIN'))
            
            USER_SESSIONS[cid] = cl
            check_master_files()
            USER_STATE[cid] = None
            idx = USER_SETTINGS[cid]["Index"]
            bot.send_message(cid, f"✅ Logged In! Index: {idx}", reply_markup=get_main_menu(cid))
            auto_generate_chain(cid)
        except Exception as e:
            bot.send_message(cid, f"❌ Login Failed: {e}", reply_markup=get_login_btn())
            USER_STATE[cid] = None
        return

    if cid not in USER_SESSIONS: return

    if "Index:" in text:
        mk = types.InlineKeyboardMarkup()
        mk.add(types.InlineKeyboardButton("🔵 NIFTY", callback_data="SET_NIFTY"),
               types.InlineKeyboardButton("🔴 SENSEX", callback_data="SET_SENSEX"))
        bot.send_message(cid, "Select Index:", reply_markup=mk)

    elif text == "🔄 Refresh Data":
        bot.send_message(cid, "⏳ Updating Data...")
        auto_generate_chain(cid)
        if fetch_data_for_user(cid): bot.send_message(cid, "✅ Data Updated")

    elif "Change ATM" in text:
        bot.send_message(cid, "⚙️ Auto-Detecting ATM...")
        success, msg = auto_generate_chain(cid)
        bot.send_message(cid, f"✅ {msg}" if success else f"❌ {msg}")

    elif text == "💰 P&L":
        try:
            # Query MongoDB directly instead of CSV
            my_open = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"}))
            
            if not my_open:
                bot.send_message(cid, "✅ No Open Trades.")
                return

            client = USER_SESSIONS[cid]
            token_list = []
            for r in my_open:
                conf = INDICES_CONFIG[r['Index']]
                token_list.append({"instrument_token": str(r['Token']), "exchange_segment": conf["Exchange"]})
            
            ltp_map = {}
            if token_list:
                q = client.quotes(instrument_tokens=token_list, quote_type="all")
                raw = q if isinstance(q, list) else q.get('data', [])
                for item in raw:
                    tk = str(item.get('exchange_token') or item.get('tk'))
                    ltp_val = float(item.get('ltp', item.get('lastPrice', 0)))
                    ltp_map[tk] = ltp_val

            msg = "💰 **Live P&L Report**\n\n"
            total_pnl = 0.0
            
            for r in my_open:
                ltp = ltp_map.get(str(r['Token']), 0.0)
                qty = int(r['Qty'])
                entry = float(r['EntryPrice'])
                
                if r['Side'] == 'SELL': pnl = (entry - ltp) * qty
                else: pnl = (ltp - entry) * qty
                
                total_pnl += pnl
                icon = "🟢" if pnl >= 0 else "🔴"
                msg += f"{icon} **{r['TradeSymbol']}**\nEntry: {entry} | LTP: {ltp}\nPnL: **{pnl:+.2f}**\n\n"
            
            msg += f"────────────────\n**Total P&L: {total_pnl:+.2f}**"
            bot.send_message(cid, msg)
        except Exception as e: bot.send_message(cid, f"P&L Error: {e}")

    elif text == "📊 OI Data":
        if cid not in ACTIVE_TOKENS: auto_generate_chain(cid)
        fetch_data_for_user(cid)
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
        if cid not in ACTIVE_TOKENS: auto_generate_chain(cid)
        fetch_data_for_user(cid)
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
            df = df[(df['Type'] == opt_type) & (df['LTP'] > 0)]
            if df.empty:
                bot.send_message(cid, "❌ No Data.")
                return
            main = df[df['LTP'] <= target].sort_values('LTP', ascending=False)
            main = main.iloc[0] if not main.empty else df.sort_values('LTP', ascending=True).iloc[0]
            if opt_type == 'CE': pool = df[df['Strike'] > main['Strike']].copy()
            else: pool = df[df['Strike'] < main['Strike']].copy()
            if pool.empty:
                bot.send_message(cid, "❌ Hedge not found.")
                return
            pool['diff'] = abs(pool['LTP'] - (main['LTP'] * 0.20))
            hedge = pool.sort_values(by=['diff', 'LTP']).iloc[0]
            PENDING_TRADE[cid]["Main"] = main.to_dict()
            PENDING_TRADE[cid]["Hedge"] = hedge.to_dict()
            msg = (f"⚡ **CONFIRM {idx} TRADE**\nLots: {lots} (Qty: {qty})\n"
                   f"🔴 SELL: {main['TradeSymbol']} (@{main['LTP']})\n"
                   f"🟢 BUY: {hedge['TradeSymbol']} (@{hedge['LTP']})\nExecute?")
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
    
    # --- INDEX SELECTION ---
    if call.data == "SET_NIFTY":
        USER_SETTINGS[cid]["Index"] = "NIFTY"
        ACTIVE_TOKENS[cid] = [] 
        bot.send_message(cid, "✅ Index: NIFTY", reply_markup=get_main_menu(cid))
        auto_generate_chain(cid)
    
    elif call.data == "SET_SENSEX":
        USER_SETTINGS[cid]["Index"] = "SENSEX"
        ACTIVE_TOKENS[cid] = []
        bot.send_message(cid, "✅ Index: SENSEX", reply_markup=get_main_menu(cid))
        auto_generate_chain(cid)

    # --- TRADE FLOW ---
    elif call.data in ["TRADE_CE", "TRADE_PE"]:
        PENDING_TRADE[cid] = {"Type": "CE" if "CE" in call.data else "PE"}
        USER_STATE[cid] = "WAIT_PREMIUM"
        bot.send_message(cid, "💰 Enter Sell Premium Target:")

    elif call.data == "EXECUTE_TRADE":
        try:
            bot.edit_message_text("⏳ Executing Market Orders...", cid, call.message.message_id)
            t_data = PENDING_TRADE[cid]
            idx = USER_SETTINGS[cid]["Index"]
            conf = INDICES_CONFIG[idx]
            client = USER_SESSIONS[cid]
            qty = int(t_data["Qty"])
            
            resp_hedge = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(qty), validity="DAY", trading_symbol=t_data["Hedge"]["TradeSymbol"], transaction_type="B", amo="NO")
            if not isinstance(resp_hedge, dict) or 'nOrdNo' not in resp_hedge:
                bot.send_message(cid, f"❌ Hedge Buy Failed: {resp_hedge}")
                return

            time.sleep(0.2)
            resp_main = client.place_order(exchange_segment=conf["Exchange"], product="NRML", price="0", order_type="MKT", quantity=str(qty), validity="DAY", trading_symbol=t_data["Main"]["TradeSymbol"], transaction_type="S", amo="NO")
            
            log_trade(cid, idx, t_data["Hedge"]["TradeSymbol"], t_data["Hedge"]["Token"], t_data["Type"], "BUY", qty, t_data["Hedge"]["LTP"], str(resp_hedge['nOrdNo']))
            
            if not isinstance(resp_main, dict) or 'nOrdNo' not in resp_main:
                bot.send_message(cid, f"⚠️ Hedge placed, but Main SELL failed: {resp_main}")
            else:
                log_trade(cid, idx, t_data["Main"]["TradeSymbol"], t_data["Main"]["Token"], t_data["Type"], "SELL", qty, t_data["Main"]["LTP"], str(resp_main['nOrdNo']))
                mk = types.InlineKeyboardMarkup(row_width=2)
                mk.add(types.InlineKeyboardButton("105% (Auto)", callback_data=f"SLSET_{resp_main['nOrdNo']}_105"),
                       types.InlineKeyboardButton("25%", callback_data=f"SLSET_{resp_main['nOrdNo']}_25"),
                       types.InlineKeyboardButton("50%", callback_data=f"SLSET_{resp_main['nOrdNo']}_50"),
                       types.InlineKeyboardButton("100%", callback_data=f"SLSET_{resp_main['nOrdNo']}_100"))
                bot.send_message(cid, f"✅ Trade Executed!\nID: {resp_main['nOrdNo']}\n\n**Set Stop Loss?**", reply_markup=mk)
        except Exception as e: bot.send_message(cid, f"❌ Execution Err: {e}")

    elif call.data == "CANCEL_TRADE":
        bot.edit_message_text("🚫 Cancelled.", cid, call.message.message_id)

    elif call.data == "EXIT_CANCEL":
        bot.delete_message(cid, call.message.message_id)

    # --- STOP LOSS CALLBACKS ---
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

    # --- SAFE EXIT ALL ---
    elif call.data == "EXIT_ALL_CONFIRM":
        bot.edit_message_text("🚨 **INITIATING SAFE EXIT SEQUENCE...**", cid, call.message.message_id)
        try:
            open_rows = list(trades_col.find({"ChatID": str(cid), "Status": "OPEN"}))
            if not open_rows:
                bot.send_message(cid, "✅ No Open Positions.")
                return
            client = USER_SESSIONS[cid]
            
            # 1. Cancel SL Orders
            for row in open_rows:
                sl_id = str(row.get('SLOrderID', ""))
                if sl_id != "" and sl_id != "nan":
                    try: client.cancel_order(order_id=sl_id)
                    except: pass
            
            # 2. EXIT ALL SELLS
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
            
            # 3. EXIT ALL BUYS
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
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

# Dummy server class to keep Render Web Service happy
class DummyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is active and polling!")

def keep_alive():
    # Render assigns a PORT environment variable dynamically
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), DummyHandler)
    print(f"🌐 Dummy web server running on port {port}")
    server.serve_forever()

def start_bot():
    print("🤖 Bot is polling...")
    bot.infinity_polling()

if __name__ == "__main__":
    # Start the dummy server in a background thread
    threading.Thread(target=keep_alive, daemon=True).start()
    
    # Start the Telegram bot in the main thread
    while True:
        try:
            start_bot()
        except Exception as e:
            print(f"Bot crashed: {e}")
            time.sleep(10)
