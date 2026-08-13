#!/usr/bin/env python3
"""
NomardDesk Concierge Bot
------------------------
Single-file Telegram bot. User picks a category, sends a sample/link,
it lands in your inbox chat. You do the work by hand and send the result
back through the bot. 3 free requests, then TON / USDT subscription.

Deploy: push to GitHub -> Railway -> set env vars -> done.
"""

import asyncio
import base64
import json
import logging
import os
import random
import secrets
import time

import io
from urllib.parse import quote_plus

import httpx
import qrcode
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("nomard")

# ==========================================================================
#  1. CONFIG  — edit this block, everything else can stay untouched
# ==========================================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x]
INBOX_CHAT_ID = int(os.getenv("INBOX_CHAT_ID", "0"))

# --- wallets you receive into ---
TON_WALLET = os.getenv("TON_WALLET", "")
TRON_WALLET = os.getenv("TRON_WALLET", "")   # USDT TRC20
BSC_WALLET = os.getenv("BSC_WALLET", "")     # USDT BEP20

TONCENTER_KEY = os.getenv("TONCENTER_KEY", "")
TRONGRID_KEY = os.getenv("TRONGRID_KEY", "")
BSCSCAN_KEY = os.getenv("BSCSCAN_KEY", "")

USDT_TRC20 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_BEP20 = "0x55d398326f99059fF775485246999027B3197955"

# --- rules ---
TRIAL_QUOTA = 3           # free requests, shared across every category
MAX_OPEN_ORDERS = 2       # per user at once
INVOICE_TTL_MIN = 60      # unpaid invoice dies after this
GRACE_HOURS = 2           # keep access this long past expiry
STALE_HOURS = 12          # nudge you if an order sits unclaimed

PLANS = {
    "daily":   {"label": "Daily",   "price_usd": 2.0,  "days": 1},
    "weekly":  {"label": "Weekly",  "price_usd": 10.0, "days": 7},
    "monthly": {"label": "Monthly", "price_usd": 20.0, "days": 30},
}

# Rename / add / remove freely. The bot builds its menu from this.
CATEGORIES = {
    "posts": {
        "emoji": "📝", "name": "Posts",
        "ask": "Send the post link, or a sample of the style you want.",
    },
    "content": {
        "emoji": "🎬", "name": "Content",
        "ask": "Send the content link, or a sample of what you're after.",
    },
    "music": {
        "emoji": "🎵", "name": "Music",
        "ask": "Send the track name, a link, or an audio clip.",
    },
    "stickers": {
        "emoji": "🩷", "name": "Stickers",
        "ask": "Send a sticker or a pack link and I'll find similar packs.",
    },
    "graphics": {
        "emoji": "🖼", "name": "Graphics",
        "ask": "Send the image or reference you want worked on.",
    },
}

WELCOME = "Hi, ready to work?"

# how often the live invoice card refreshes its countdown
QR_TICK_SECONDS = 20
# manual "I've paid" check: attempts x gap
MANUAL_TRIES = 6
MANUAL_GAP = 5

CHAINS = {
    "ton":   {"label": "TON",          "asset": "TON",  "wallet": TON_WALLET,  "memo": True},
    "trc20": {"label": "USDT (TRC20)", "asset": "USDT", "wallet": TRON_WALLET, "memo": False},
    "bep20": {"label": "USDT (BEP20)", "asset": "USDT", "wallet": BSC_WALLET,  "memo": False},
}

# ==========================================================================
#  2. FIRESTORE
# ==========================================================================

_creds = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
if _creds:
    with open("/tmp/gcp.json", "w") as fh:
        fh.write(_creds)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp.json"

from google.cloud import firestore  # noqa: E402
from google.cloud.firestore_v1.base_query import FieldFilter  # noqa: E402

db = firestore.AsyncClient()

C_USERS = "nd_users"
C_SUBS = "nd_subs"
C_INV = "nd_invoices"
C_ORD = "nd_orders"

OPEN_STATES = ("pending", "working")


def now() -> int:
    return int(time.time())


# ---- users -------------------------------------------------------------

async def get_user(uid: int) -> dict:
    snap = await db.collection(C_USERS).document(str(uid)).get()
    return snap.to_dict() or {"uid": uid, "trials": 0, "created": now()}


async def save_user(u: dict):
    await db.collection(C_USERS).document(str(u["uid"])).set(u, merge=True)


# ---- subscriptions -----------------------------------------------------

async def get_sub(uid: int) -> dict | None:
    snap = await db.collection(C_SUBS).document(str(uid)).get()
    return snap.to_dict() if snap.exists else None


async def save_sub(s: dict):
    await db.collection(C_SUBS).document(str(s["uid"])).set(s, merge=True)


async def all_active_subs() -> list[dict]:
    q = db.collection(C_SUBS).where(filter=FieldFilter("active", "==", True))
    return [d.to_dict() async for d in q.stream()]


# ---- invoices ----------------------------------------------------------

async def new_invoice(uid, plan, chain, amount, memo) -> dict:
    inv = {
        "id": secrets.token_hex(8),
        "uid": uid,
        "plan": plan,
        "chain": chain,
        "amount": amount,
        "memo": memo,
        "status": "pending",
        "created": now(),
        "tx": None,
    }
    await db.collection(C_INV).document(inv["id"]).set(inv)
    return inv


async def get_invoice(iid: str) -> dict | None:
    snap = await db.collection(C_INV).document(iid).get()
    return snap.to_dict() if snap.exists else None


async def pending_invoices() -> list[dict]:
    q = db.collection(C_INV).where(filter=FieldFilter("status", "==", "pending"))
    return [d.to_dict() async for d in q.stream()]


async def paid_invoices() -> list[dict]:
    q = db.collection(C_INV).where(filter=FieldFilter("status", "==", "paid"))
    return [d.to_dict() async for d in q.stream()]


async def close_invoice(iid: str, status: str, tx: str | None = None):
    await db.collection(C_INV).document(iid).set(
        {"status": status, "tx": tx, "closed": now()}, merge=True
    )


async def tx_seen(tx: str) -> bool:
    q = db.collection(C_INV).where(filter=FieldFilter("tx", "==", tx)).limit(1)
    return len([d async for d in q.stream()]) > 0


# ---- orders ------------------------------------------------------------

async def create_order(uid: int, username: str, category: str, note: str, paid: bool) -> dict:
    o = {
        "id": secrets.token_hex(3).upper(),
        "uid": uid,
        "username": username or "—",
        "category": category,
        "status": "pending",
        "note": note,
        "paid": paid,
        "created": now(),
        "claimed_at": None,
        "delivered_at": None,
        "inbox_msg_id": None,
        "nudged": False,
    }
    await db.collection(C_ORD).document(o["id"]).set(o)
    return o


async def get_order(oid: str) -> dict | None:
    snap = await db.collection(C_ORD).document(oid).get()
    return snap.to_dict() if snap.exists else None


async def update_order(oid: str, **fields):
    await db.collection(C_ORD).document(oid).set(fields, merge=True)


async def user_orders(uid: int) -> list[dict]:
    q = db.collection(C_ORD).where(filter=FieldFilter("uid", "==", uid))
    return [d.to_dict() async for d in q.stream()]


async def open_orders() -> list[dict]:
    rows = []
    for st in OPEN_STATES:
        q = db.collection(C_ORD).where(filter=FieldFilter("status", "==", st))
        rows += [d.to_dict() async for d in q.stream()]
    return sorted(rows, key=lambda r: r["created"])


async def count_orders() -> dict:
    out = {}
    for st in ("pending", "working", "delivered", "rejected"):
        q = db.collection(C_ORD).where(filter=FieldFilter("status", "==", st))
        out[st] = len([d async for d in q.stream()])
    return out


# ==========================================================================
#  3. CHAINS
# ==========================================================================

HTTP_TIMEOUT = 20


async def ton_usd_rate() -> float:
    url = "https://api.coingecko.com/api/v3/simple/price?ids=the-open-network&vs_currencies=usd"
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.get(url)
        return float(r.json()["the-open-network"]["usd"])


async def quote(chain: str, usd: float) -> float:
    if chain == "ton":
        return round(usd / await ton_usd_rate(), 2)
    return round(usd, 2)


# how big the identifying "dust" step is on each chain
DUST_STEP = {"ton": 0.0001, "trc20": 0.01, "bep20": 0.01}


async def open_invoice(uid: int, plan: str, chain: str) -> dict:
    """Amount = round base + a small unique tag. Kept short so people can
    actually type it: 0.6742 TON, 2.37 USDT."""
    base = await quote(chain, PLANS[plan]["price_usd"])
    step = DUST_STEP[chain]

    taken = {round(i["amount"], 6) for i in await pending_invoices()
             if i["chain"] == chain}
    amount = None
    for n in random.sample(range(1, 100), 99):
        candidate = round(base + n * step, 6)
        if candidate not in taken:
            amount = candidate
            break
    if amount is None:                       # 99 live invoices on one chain
        amount = round(base + random.randint(100, 199) * step, 6)

    memo = f"ND{random.randint(100000, 999999)}" if CHAINS[chain]["memo"] else ""
    return await new_invoice(uid, plan, chain, amount, memo)


def _ton_comment(m: dict) -> str:
    """TonCenter returns the comment in a few shapes depending on the wallet."""
    txt = (m.get("message") or "").strip()
    if txt:
        return txt
    data = m.get("msg_data") or {}
    raw = data.get("text") or ""
    if raw:
        try:
            return base64.b64decode(raw).decode("utf-8", "ignore").strip()
        except Exception:
            return raw.strip()
    return ""


async def _ton_in() -> list[dict]:
    url = "https://toncenter.com/api/v2/getTransactions"
    params = {"address": TON_WALLET, "limit": 50}
    if TONCENTER_KEY:
        params["api_key"] = TONCENTER_KEY
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.get(url, params=params)
    out = []
    for t in r.json().get("result", []):
        m = t.get("in_msg") or {}
        if not m.get("source"):
            continue
        out.append({
            "tx": t["transaction_id"]["hash"],
            "amount": int(m.get("value", 0)) / 1e9,
            "memo": _ton_comment(m),
            "ts": t.get("utime", 0),
        })
    return out


async def _trc20_in() -> list[dict]:
    url = f"https://api.trongrid.io/v1/accounts/{TRON_WALLET}/transactions/trc20"
    params = {"limit": 50, "contract_address": USDT_TRC20, "only_to": "true"}
    headers = {"TRON-PRO-API-KEY": TRONGRID_KEY} if TRONGRID_KEY else {}
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.get(url, params=params, headers=headers)
    return [{
        "tx": t["transaction_id"],
        "amount": int(t["value"]) / 1e6,
        "memo": "",
        "ts": int(t["block_timestamp"]) // 1000,
    } for t in r.json().get("data", [])]


async def _bep20_in() -> list[dict]:
    url = "https://api.bscscan.com/api"
    params = {
        "module": "account", "action": "tokentx",
        "contractaddress": USDT_BEP20, "address": BSC_WALLET,
        "sort": "desc", "apikey": BSCSCAN_KEY,
    }
    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as c:
        r = await c.get(url, params=params)
    data = r.json().get("result")
    if not isinstance(data, list):
        return []
    out = []
    for t in data:
        if t["to"].lower() != BSC_WALLET.lower():
            continue
        out.append({
            "tx": t["hash"],
            "amount": int(t["value"]) / (10 ** int(t["tokenDecimal"])),
            "memo": "",
            "ts": int(t["timeStamp"]),
        })
    return out


async def incoming(chain: str) -> list[dict]:
    if not CHAINS[chain]["wallet"]:
        return []
    try:
        return await {"ton": _ton_in, "trc20": _trc20_in, "bep20": _bep20_in}[chain]()
    except Exception as e:
        log.warning("chain %s read failed: %s", chain, e)
        return []


# ==========================================================================
#  4. ACCESS ENGINE
# ==========================================================================

async def has_access(uid: int) -> bool:
    sub = await get_sub(uid)
    if not sub or not sub.get("active"):
        return False
    return sub["expires_at"] + GRACE_HOURS * 3600 > now()


async def trial_left(uid: int) -> int:
    u = await get_user(uid)
    return max(0, TRIAL_QUOTA - u.get("trials", 0))


async def can_use(uid: int) -> bool:
    """Check only — burns nothing."""
    return await has_access(uid) or await trial_left(uid) > 0


async def gate(uid: int) -> tuple[bool, str]:
    """Consume. Call once, at the real moment of request."""
    if await has_access(uid):
        return True, "sub"
    u = await get_user(uid)
    used = u.get("trials", 0)
    if used >= TRIAL_QUOTA:
        return False, "locked"
    u["trials"] = used + 1
    await save_user(u)
    return True, f"trial:{TRIAL_QUOTA - u['trials']}"


async def refund_trial(uid: int):
    u = await get_user(uid)
    if u.get("trials", 0) > 0:
        u["trials"] -= 1
        await save_user(u)


async def grant(uid: int, plan: str, days: int | None = None):
    days = days or PLANS[plan]["days"]
    sub = await get_sub(uid)
    live = sub and sub.get("active") and sub["expires_at"] > now()
    start = sub["expires_at"] if live else now()
    await save_sub({
        "uid": uid, "plan": plan, "active": True,
        "started_at": now(), "expires_at": start + days * 86400,
        "notified": False,
    })


async def revoke(uid: int):
    await save_sub({"uid": uid, "active": False, "expires_at": now()})


def _matches(inv: dict, pay: dict) -> bool:
    """Memo OR exact amount. Either one alone is proof enough — the dust in
    every amount makes it unique, and comments often don't survive the trip."""
    if pay["ts"] < inv["created"] - 300:
        return False
    if inv["memo"] and inv["memo"].lower() in (pay["memo"] or "").lower():
        return True
    return round(pay["amount"], 6) == round(inv["amount"], 6)


async def settle(inv: dict) -> bool:
    if inv["status"] != "pending":
        return inv["status"] == "paid"
    if now() - inv["created"] > INVOICE_TTL_MIN * 60:
        await close_invoice(inv["id"], "expired")
        return False

    payments = await incoming(inv["chain"])
    for pay in payments:
        if not _matches(inv, pay) or await tx_seen(pay["tx"]):
            continue
        await close_invoice(inv["id"], "paid", pay["tx"])
        await grant(inv["uid"], inv["plan"])
        log.info("PAID %s uid=%s %s (%s %s)", inv["id"], inv["uid"],
                 inv["plan"], pay["amount"], inv["chain"])
        return True

    # nothing matched — show what we did see, so mismatches are debuggable
    recent = [p for p in payments if p["ts"] > inv["created"] - 300]
    if recent:
        log.info("NO MATCH inv=%s want=%s memo=%r | saw: %s",
                 inv["id"], round(inv["amount"], 6), inv["memo"],
                 [(round(p["amount"], 6), p["memo"][:24]) for p in recent[:5]])
    return False


# ==========================================================================
#  4b. INVOICE CARD — QR + LIVE COUNTDOWN
# ==========================================================================

# invoices currently being checked by hand — stops the watcher fighting the edit
MANUAL_LOCK: set[str] = set()


def payment_uri(inv: dict) -> str:
    """Deep link a wallet app can scan."""
    ch = CHAINS[inv["chain"]]
    if inv["chain"] == "ton":
        nano = int(round(inv["amount"] * 1e9))
        uri = f"ton://transfer/{ch['wallet']}?amount={nano}"
        if inv["memo"]:
            uri += f"&text={quote_plus(inv['memo'])}"
        return uri
    # TRON / BSC wallets scan a bare address reliably; amount goes in the caption
    return ch["wallet"]


def make_qr(data: str) -> BufferedInputFile:
    qr = qrcode.QRCode(version=None, box_size=10, border=2,
                       error_correction=qrcode.constants.ERROR_CORRECT_M)
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return BufferedInputFile(buf.read(), filename="pay.png")


def pay_kb(iid: str, state: str) -> InlineKeyboardMarkup:
    if state == "waiting":
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ I've paid", callback_data=f"check:{iid}")],
            [InlineKeyboardButton(text="✖️ Cancel", callback_data=f"kill:{iid}")],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🏠 Menu", callback_data="menu")]])


def card_text(inv: dict, state: str, secs_left: int = 0, attempt: int = 0) -> str:
    ch = CHAINS[inv["chain"]]
    plan = PLANS[inv["plan"]]

    if state == "verifying":
        dots = "." * (attempt % 4)
        return (f"🔍 <b>Verifying your payment{dots}</b>\n\n"
                f"Checking the {ch['label']} network for your "
                f"<code>{inv['amount']}</code> {ch['asset']}.\n\n"
                f"Step {attempt} of {MANUAL_TRIES} — hang on.")

    if state == "searching":
        return ("⏳ <b>Still looking for your payment</b>\n\n"
                "It hasn't shown up on the network yet. That's normal — "
                "transfers can take a few minutes to confirm.\n\n"
                "🔔 <b>You don't need to do anything.</b> I'm watching the chain "
                "and I'll message you the second it lands.\n\n"
                "Go ahead and use the menu meanwhile.")

    if state == "paid":
        return (f"✅ <b>Payment confirmed</b>\n\n"
                f"{plan['label']} plan is active. Everything's unlocked.")

    if state == "expired":
        return ("⌛ <b>Invoice expired</b>\n\n"
                "No payment arrived in time. Send /plans to make a new one.")

    if state == "cancelled":
        return "✖️ <b>Invoice cancelled.</b>\n\nSend /plans whenever you're ready."

    # waiting — the QR caption
    memo = (f"\n<b>Memo / comment:</b>\n<code>{inv['memo']}</code>  ← required"
            if inv["memo"] else "")
    m, s = divmod(max(0, secs_left), 60)
    filled = min(20, max(0, int(20 * secs_left / (INVOICE_TTL_MIN * 60))))
    bar = "█" * filled + "░" * (20 - filled)
    return (
        f"<b>{plan['label']} plan · ${plan['price_usd']:.0f}</b>\n\n"
        f"Scan the QR, or send manually:\n\n"
        f"<b>Amount (exact):</b>\n<code>{inv['amount']}</code> {ch['asset']}\n\n"
        f"<b>To ({ch['label']}):</b>\n<code>{ch['wallet']}</code>{memo}\n\n"
        f"<code>{bar}</code>\n⏳ Expires in <b>{m}m {s:02d}s</b>\n\n"
        f"👀 Unlocks automatically the moment your payment lands."
    )


async def render_card(bot: Bot, inv: dict, state: str,
                      secs_left: int = 0, attempt: int = 0):
    """Update whichever message is currently acting as the invoice card."""
    if not inv.get("card_chat"):
        return
    body = card_text(inv, state, secs_left, attempt)
    kb = pay_kb(inv["id"], state)
    try:
        if inv.get("card_type") == "text":
            await bot.edit_message_text(
                body, chat_id=inv["card_chat"], message_id=inv["card_msg"],
                reply_markup=kb)
        else:
            await bot.edit_message_caption(
                chat_id=inv["card_chat"], message_id=inv["card_msg"],
                caption=body, reply_markup=kb)
    except Exception:
        pass  # "message is not modified", deleted message, etc.


async def set_card(iid: str, chat_id: int, msg_id: int,
                   card_type: str, mode: str):
    await db.collection(C_INV).document(iid).set(
        {"card_chat": chat_id, "card_msg": msg_id,
         "card_type": card_type, "card_mode": mode}, merge=True)


async def notify_paid(bot: Bot, inv: dict):
    """Flip the card to confirmed and push a separate notification. Once only."""
    fresh = await get_invoice(inv["id"])
    if fresh and fresh.get("paid_notified"):
        return
    await db.collection(C_INV).document(inv["id"]).set(
        {"paid_notified": True}, merge=True)

    inv = fresh or inv
    await render_card(bot, inv, "paid")

    plan = PLANS[inv["plan"]]
    sub = await get_sub(inv["uid"])
    until = ""
    if sub and sub.get("expires_at"):
        until = "\nActive until <b>" + time.strftime(
            "%d %b, %H:%M UTC", time.gmtime(sub["expires_at"])) + "</b>"
    try:
        await bot.send_message(
            inv["uid"],
            f"🎉 <b>Payment received!</b>\n\n"
            f"{plan['label']} plan unlocked — "
            f"<code>{inv['amount']}</code> {CHAINS[inv['chain']]['asset']} confirmed."
            f"{until}\n\nAll five categories are open. Tap below to start.",
            reply_markup=menu_kb())
    except Exception as e:
        log.warning("paid notify %s: %s", inv["uid"], e)


async def watch_invoice(bot: Bot, iid: str):
    """Live countdown + auto-settle. Runs until paid, expired or cancelled."""
    while True:
        await asyncio.sleep(QR_TICK_SECONDS)
        inv = await get_invoice(iid)
        if not inv or inv["status"] != "pending":
            return
        if iid in MANUAL_LOCK:
            continue

        left = INVOICE_TTL_MIN * 60 - (now() - inv["created"])
        if left <= 0:
            await close_invoice(iid, "expired")
            await render_card(bot, inv, "expired")
            return

        try:
            if await settle(inv):
                await notify_paid(bot, inv)
                return
        except Exception as e:
            log.warning("watch settle %s: %s", iid, e)

        # in verify mode the card shows a status message — don't stomp it
        if inv.get("card_mode") != "verify":
            await render_card(bot, inv, "waiting", left)


# ==========================================================================
#  5. KEYBOARDS
# ==========================================================================

def menu_kb() -> InlineKeyboardMarkup:
    rows, row = [], []
    for cid, c in CATEGORIES.items():
        row.append(InlineKeyboardButton(text=f"{c['emoji']} {c['name']}",
                                        callback_data=f"pick:{cid}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        InlineKeyboardButton(text="📦 My requests", callback_data="mine"),
        InlineKeyboardButton(text="💎 Upgrade", callback_data="plans"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def plans_kb() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=f"{p['label']} — ${p['price_usd']:.0f}", callback_data=f"plan:{k}")]
        for k, p in PLANS.items()]
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def chains_kb(plan: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=v["label"], callback_data=f"chain:{plan}:{k}")]
            for k, v in CHAINS.items() if v["wallet"]]
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="plans")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def card_kb(oid: str, status: str) -> InlineKeyboardMarkup:
    if status == "pending":
        rows = [[InlineKeyboardButton(text="🔧 Claim", callback_data=f"claim:{oid}")]]
    elif status == "working":
        rows = [[InlineKeyboardButton(text="📤 Deliver", callback_data=f"deliver:{oid}")]]
    else:
        return InlineKeyboardMarkup(inline_keyboard=[])
    rows.append([InlineKeyboardButton(text="❌ Can't do it", callback_data=f"reject:{oid}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def status_line(uid: int) -> str:
    sub = await get_sub(uid)
    if await has_access(uid):
        left_h = max(0, (sub["expires_at"] - now()) // 3600)
        return f"✅ <b>{PLANS[sub['plan']]['label']} active</b> · {left_h}h left"
    n = await trial_left(uid)
    return f"🎁 <b>{n} free request{'s' if n != 1 else ''} left</b>"


# ==========================================================================
#  6. USER FLOW
# ==========================================================================

user_router = Router()


class Submit(StatesGroup):
    waiting = State()


@user_router.message(CommandStart())
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(f"{WELCOME}\n\n{await status_line(m.from_user.id)}",
                   reply_markup=menu_kb())


@user_router.message(Command("menu"))
async def cmd_menu(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(await status_line(m.from_user.id), reply_markup=menu_kb())


@user_router.callback_query(F.data == "menu")
async def cb_menu(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.edit_text(await status_line(cq.from_user.id), reply_markup=menu_kb())
    await cq.answer()


@user_router.message(Command("plans"))
async def cmd_plans(m: Message):
    await m.answer(
        "💎 <b>Unlock everything</b>\n\n"
        "One subscription covers all categories.\n"
        "Pay in TON or USDT.\n\n"
        f"{await status_line(m.from_user.id)}",
        reply_markup=plans_kb())


@user_router.callback_query(F.data == "plans")
async def cb_plans(cq: CallbackQuery):
    await cq.message.edit_text(
        "💎 <b>Unlock everything</b>\n\n"
        "One subscription covers all categories.\n"
        "Pay in TON or USDT.\n\n"
        f"{await status_line(cq.from_user.id)}",
        reply_markup=plans_kb())
    await cq.answer()


@user_router.callback_query(F.data.startswith("plan:"))
async def cb_plan(cq: CallbackQuery):
    plan = cq.data.split(":")[1]
    p = PLANS[plan]
    await cq.message.edit_text(
        f"<b>{p['label']} — ${p['price_usd']:.0f}</b> · {p['days']} day(s)\n\nPay with:",
        reply_markup=chains_kb(plan))
    await cq.answer()


@user_router.callback_query(F.data.startswith("chain:"))
async def cb_chain(cq: CallbackQuery, bot: Bot):
    _, plan, chain = cq.data.split(":")
    await cq.answer("Building invoice…")
    try:
        inv = await open_invoice(cq.from_user.id, plan, chain)
    except Exception as e:
        log.error("invoice fail: %s", e)
        return await cq.message.edit_text(
            "⚠️ Couldn't build the invoice. Try again in a minute.")

    try:
        await cq.message.delete()
    except Exception:
        pass

    card = await bot.send_photo(
        cq.from_user.id,
        make_qr(payment_uri(inv)),
        caption=card_text(inv, "waiting", INVOICE_TTL_MIN * 60),
        reply_markup=pay_kb(inv["id"], "waiting"))

    await set_card(inv["id"], card.chat.id, card.message_id, "photo", "qr")

    asyncio.create_task(watch_invoice(bot, inv["id"]))


@user_router.callback_query(F.data.startswith("kill:"))
async def cb_kill(cq: CallbackQuery):
    iid = cq.data.split(":")[1]
    inv = await get_invoice(iid)
    if inv and inv["status"] == "pending":
        await close_invoice(iid, "cancelled")
    await cq.answer("Cancelled.")
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.message.answer(
        "✖️ Invoice cancelled.\n\n" + await status_line(cq.from_user.id),
        reply_markup=menu_kb())


@user_router.callback_query(F.data.startswith("check:"))
async def cb_check(cq: CallbackQuery, bot: Bot):
    iid = cq.data.split(":")[1]
    inv = await get_invoice(iid)
    if not inv:
        return await cq.answer("Invoice not found.", show_alert=True)
    if inv["status"] == "paid":
        return await cq.answer("Already confirmed — you're good to go.",
                               show_alert=True)
    if inv["status"] != "pending":
        return await cq.answer("This invoice is closed. Send /plans for a new one.",
                               show_alert=True)

    # answer straight away — the query dies after ~15s and this loop runs longer
    await cq.answer("Checking…")

    # remember this person is actively claiming to have paid this invoice —
    # if their payment lands mismatched, the orphan alert can point back here
    await db.collection(C_INV).document(iid).set(
        {"claimed_paid_at": now()}, merge=True)

    MANUAL_LOCK.add(iid)
    try:
        # QR and address go away; a status message takes their place
        try:
            await cq.message.delete()
        except Exception:
            pass

        status = await bot.send_message(
            cq.from_user.id,
            card_text(inv, "verifying", attempt=1),
            reply_markup=pay_kb(iid, "verifying"))
        await set_card(iid, status.chat.id, status.message_id, "text", "verify")
        inv = await get_invoice(iid)

        for attempt in range(1, MANUAL_TRIES + 1):
            if attempt > 1:
                await render_card(bot, inv, "verifying", attempt=attempt)
            try:
                if await settle(inv):
                    await notify_paid(bot, inv)
                    return
            except Exception as e:
                log.warning("manual settle %s: %s", iid, e)
            if attempt < MANUAL_TRIES:
                await asyncio.sleep(MANUAL_GAP)

        # not found yet — the watcher and the 45s job keep looking
        await render_card(bot, inv, "searching")
    finally:
        MANUAL_LOCK.discard(iid)


@user_router.callback_query(F.data.startswith("pick:"))
async def cb_pick(cq: CallbackQuery, state: FSMContext):
    cid = cq.data.split(":")[1]
    uid = cq.from_user.id

    if not await can_use(uid):
        await cq.answer()
        return await cq.message.edit_text(
            "🔒 <b>Your free requests are used up.</b>\n\n"
            "Daily $2 · Weekly $10 · Monthly $20\n"
            "One subscription covers every category.",
            reply_markup=plans_kb())

    mine = [o for o in await user_orders(uid) if o["status"] in OPEN_STATES]
    if len(mine) >= MAX_OPEN_ORDERS:
        return await cq.answer(
            f"You've got {MAX_OPEN_ORDERS} open already. Let me finish those first.",
            show_alert=True)

    c = CATEGORIES[cid]
    await state.set_state(Submit.waiting)
    await state.update_data(cid=cid)
    await cq.message.edit_text(
        f"{c['emoji']} <b>{c['name']}</b>\n\n{c['ask']}\n\n<i>/cancel to back out.</i>")
    await cq.answer()


@user_router.message(Command("cancel"), StateFilter(Submit.waiting))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Cancelled.", reply_markup=menu_kb())


@user_router.message(StateFilter(Submit.waiting))
async def capture(m: Message, state: FSMContext, bot: Bot):
    cid = (await state.get_data()).get("cid")
    if not cid:
        await state.clear()
        return await m.answer("Something went stale — /menu to start again.")

    uid = m.from_user.id
    ok, why = await gate(uid)
    if not ok:
        await state.clear()
        return await m.answer("🔒 Free requests used up.", reply_markup=plans_kb())

    note = m.text or m.caption or f"[sent {m.content_type.value}]"
    order = await create_order(uid, m.from_user.username, cid, note, paid=(why == "sub"))
    await state.clear()

    await push_card(bot, order, m)

    tail = f"\n\n🎁 {why.split(':')[1]} free requests left." if why.startswith("trial") else ""
    await m.answer(
        f"✅ <b>Got it — request #{order['id']}</b>\n\n"
        f"Please wait, I'm working on it. The result comes straight back to this chat."
        f"{tail}",
        reply_markup=menu_kb())


@user_router.callback_query(F.data == "mine")
async def cb_mine(cq: CallbackQuery):
    rows = [o for o in await user_orders(cq.from_user.id) if o["status"] in OPEN_STATES]
    if not rows:
        return await cq.answer("Nothing open right now.", show_alert=True)
    rows.sort(key=lambda r: r["created"])
    body = "\n".join(
        f"#{o['id']} · {CATEGORIES.get(o['category'], {}).get('name', o['category'])} · "
        f"{'🔧 working' if o['status'] == 'working' else '⏳ queued'}"
        for o in rows)
    await cq.message.edit_text(f"<b>Your open requests</b>\n\n{body}", reply_markup=menu_kb())
    await cq.answer()


# ==========================================================================
#  7. INBOX CARDS
# ==========================================================================

def card_body(o: dict) -> str:
    c = CATEGORIES.get(o["category"], {"emoji": "•", "name": o["category"]})
    tag = "💳 PAID" if o["paid"] else "🎁 TRIAL"
    age = (now() - o["created"]) // 60
    return (
        f"{c['emoji']} <b>#{o['id']} · {c['name']}</b>  {tag}\n"
        f"From: @{o['username']} · <code>{o['uid']}</code>\n"
        f"Status: <b>{o['status']}</b> · {age}m old\n\n"
        f"<b>Request:</b>\n{o['note'][:900]}"
    )


async def push_card(bot: Bot, o: dict, src: Message):
    if not INBOX_CHAT_ID:
        return log.warning("INBOX_CHAT_ID not set — order %s not forwarded", o["id"])
    try:
        await bot.copy_message(INBOX_CHAT_ID, src.chat.id, src.message_id)
        card = await bot.send_message(INBOX_CHAT_ID, card_body(o),
                                      reply_markup=card_kb(o["id"], "pending"))
        await update_order(o["id"], inbox_msg_id=card.message_id)
    except Exception as e:
        log.error("push_card failed: %s", e)


async def refresh_card(bot: Bot, o: dict):
    if not o.get("inbox_msg_id"):
        return
    try:
        await bot.edit_message_text(
            card_body(o), chat_id=INBOX_CHAT_ID,
            message_id=o["inbox_msg_id"], reply_markup=card_kb(o["id"], o["status"]))
    except Exception:
        pass


# ==========================================================================
#  8. ADMIN FLOW
# ==========================================================================

admin_router = Router()


class Fulfil(StatesGroup):
    waiting = State()


def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS


@admin_router.callback_query(F.data.startswith("claim:"))
async def cb_claim(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Not for you.", show_alert=True)
    oid = cq.data.split(":")[1]
    await update_order(oid, status="working", claimed_at=now())
    o = await get_order(oid)
    await refresh_card(bot, o)
    try:
        await bot.send_message(o["uid"], f"🔧 Started on <b>#{oid}</b>. Won't be long.")
    except Exception:
        pass
    await cq.answer("Claimed.")


@admin_router.callback_query(F.data.startswith("deliver:"))
async def cb_deliver(cq: CallbackQuery, state: FSMContext):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Not for you.", show_alert=True)
    oid = cq.data.split(":")[1]
    await state.set_state(Fulfil.waiting)
    await state.update_data(oid=oid)
    await cq.message.reply(
        f"📤 Send the result for <b>#{oid}</b> — link, file, sticker, anything.\n"
        f"/abort to stop.")
    await cq.answer()


@admin_router.message(Command("abort"), StateFilter(Fulfil.waiting))
async def cmd_abort(m: Message, state: FSMContext):
    await state.clear()
    await m.reply("Aborted.")


@admin_router.message(StateFilter(Fulfil.waiting))
async def send_result(m: Message, state: FSMContext, bot: Bot):
    oid = (await state.get_data()).get("oid")
    await state.clear()
    o = await get_order(oid)
    if not o:
        return await m.reply("Order not found.")
    try:
        await bot.send_message(o["uid"], f"✅ <b>#{oid} is ready</b> — here you go:")
        await bot.copy_message(o["uid"], m.chat.id, m.message_id)
        await bot.send_message(o["uid"], "Need anything else? /menu")
    except Exception as e:
        return await m.reply(f"⚠️ Couldn't reach the user: {e}")

    await update_order(oid, status="delivered", delivered_at=now())
    await refresh_card(bot, await get_order(oid))
    await m.reply(f"✅ Delivered #{oid}.")


@admin_router.callback_query(F.data.startswith("reject:"))
async def cb_reject(cq: CallbackQuery, bot: Bot):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Not for you.", show_alert=True)
    oid = cq.data.split(":")[1]
    o = await get_order(oid)
    await update_order(oid, status="rejected")
    if not o["paid"]:
        await refund_trial(o["uid"])
    try:
        await bot.send_message(
            o["uid"],
            f"❌ Couldn't complete <b>#{oid}</b>."
            + ("" if o["paid"] else " Your free credit has been returned."))
    except Exception:
        pass
    await refresh_card(bot, await get_order(oid))
    await cq.answer("Rejected.")


@admin_router.callback_query(F.data.startswith("grec:"))
async def cb_grant_recipient(cq: CallbackQuery):
    """Admin picked which buyer to credit — now show the duration picker."""
    if not is_admin(cq.from_user.id):
        return await cq.answer("Not for you.", show_alert=True)
    _, txpref, uid = cq.data.split(":")
    uid = int(uid)

    # find the pending orphan by tx prefix
    orphan = None
    async for d in db.collection("nd_orphans").stream():
        o = d.to_dict()
        if o["tx"].startswith(txpref) and not o.get("resolved"):
            orphan = o
            break
    if not orphan:
        return await cq.answer("This one's already handled.", show_alert=True)

    cand = next((c for c in orphan["cands"] if c["uid"] == uid), None)
    plan = cand["plan"] if cand else "daily"
    # remember the choice on the orphan for the next tap
    await db.collection("nd_orphans").document(orphan["tx"]).set(
        {"pick_uid": uid, "pick_plan": plan}, merge=True)

    who = f"@{cand['username']}" if cand and cand.get("username") not in (None, "—") \
          else f"id {uid}"
    await cq.message.edit_text(
        f"{cq.message.text}\n\n"
        f"→ Crediting <b>{who}</b> on the <b>{PLANS[plan]['label']}</b> plan.\n"
        f"How long?",
        reply_markup=span_kb(uid))
    await cq.answer()


@admin_router.callback_query(F.data.startswith("gspn:"))
async def cb_grant_span(cq: CallbackQuery, bot: Bot):
    """Admin picked a duration — grant it and tell the user."""
    if not is_admin(cq.from_user.id):
        return await cq.answer("Not for you.", show_alert=True)
    _, uid, label = cq.data.split(":")
    uid = int(uid)
    days = GRANT_SPANS.get(label, 1)

    # locate the orphan holding this pick
    orphan = None
    async for d in db.collection("nd_orphans").stream():
        o = d.to_dict()
        if o.get("pick_uid") == uid and not o.get("resolved"):
            orphan = o
            break

    plan = (orphan or {}).get("pick_plan", "daily")
    await grant_days(uid, plan, days)

    if orphan:
        await db.collection("nd_orphans").document(orphan["tx"]).set(
            {"resolved": True, "granted_days": days, "granted_uid": uid}, merge=True)

    # tell the buyer
    try:
        await bot.send_message(
            uid,
            f"🎉 <b>Payment approved!</b>\n\n"
            f"You've been granted <b>{days} day{'s' if days != 1 else ''}</b> "
            f"of access.\n\nAll categories are open — tap below to start.",
            reply_markup=menu_kb())
    except Exception as e:
        log.warning("grant notify %s: %s", uid, e)

    await cq.message.edit_text(
        f"✅ Granted <b>{days} day{'s' if days != 1 else ''}</b> "
        f"({PLANS[plan]['label']}) to <code>{uid}</code>.\n"
        f"The buyer has been notified.")
    await cq.answer("Granted.")


@admin_router.callback_query(F.data.startswith("gign:"))
async def cb_grant_ignore(cq: CallbackQuery):
    if not is_admin(cq.from_user.id):
        return await cq.answer("Not for you.", show_alert=True)
    txpref = cq.data.split(":")[1]
    async for d in db.collection("nd_orphans").stream():
        o = d.to_dict()
        if o["tx"].startswith(txpref) and not o.get("resolved"):
            await db.collection("nd_orphans").document(o["tx"]).set(
                {"resolved": True, "ignored": True}, merge=True)
            break
    await cq.message.edit_text(f"🙈 Dismissed.\n\n{cq.message.text}")
    await cq.answer("Ignored.")



    if not is_admin(m.from_user.id):
        return
    rows = await open_orders()
    if not rows:
        return await m.answer("Queue empty. ✨")
    body = "\n".join(
        f"#{o['id']} · {CATEGORIES.get(o['category'], {}).get('name', o['category'])} · "
        f"@{o['username']} · {'🔧' if o['status'] == 'working' else '⏳'} "
        f"{(now() - o['created']) // 3600}h"
        for o in rows)
    await m.answer(f"<b>Open ({len(rows)})</b>\n\n{body}")


@admin_router.message(Command("stats"))
async def cmd_stats(m: Message):
    if not is_admin(m.from_user.id):
        return
    counts = await count_orders()
    subs = await all_active_subs()
    live = [s for s in subs if s["expires_at"] > now()]
    revenue = sum(PLANS[i["plan"]]["price_usd"] for i in await paid_invoices()
                  if i["plan"] in PLANS)
    await m.answer(
        f"📊 <b>Stats</b>\n\n"
        f"⏳ Pending: {counts['pending']}\n"
        f"🔧 Working: {counts['working']}\n"
        f"✅ Delivered: {counts['delivered']}\n"
        f"❌ Rejected: {counts['rejected']}\n\n"
        f"💎 Active subs: {len(live)}\n"
        f"💰 Total revenue: ${revenue:.2f}")


@admin_router.message(Command("give"))
async def cmd_give(m: Message, bot: Bot):
    """/give <uid> <daily|weekly|monthly> [days]
    The optional day count overrides the plan length (for mismatch approvals)."""
    if not is_admin(m.from_user.id):
        return
    parts = m.text.split()
    try:
        uid = int(parts[1])
        plan = parts[2]
        if plan not in PLANS:
            return await m.answer(f"Unknown plan. Use: {', '.join(PLANS)}")
        if len(parts) >= 4:
            days = int(parts[3])
            await grant_days(uid, plan, days)
            span = f"{days} day{'s' if days != 1 else ''}"
        else:
            await grant(uid, plan)
            span = f"{PLANS[plan]['label']} ({PLANS[plan]['days']}d)"
        try:
            await bot.send_message(
                uid, f"🎉 Access granted — {span}. Tap below to start.",
                reply_markup=menu_kb())
        except Exception:
            pass
        await m.answer(f"✅ {span} → <code>{uid}</code>. Buyer notified.")
    except (IndexError, ValueError):
        await m.answer(
            "Usage: <code>/give &lt;uid&gt; &lt;daily|weekly|monthly&gt; [days]</code>\n"
            "Example: <code>/give 123456 weekly 4</code> — 4 days on the weekly plan.")


@admin_router.message(Command("take"))
async def cmd_take(m: Message):
    """/take <uid>"""
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid = m.text.split()
        await revoke(int(uid))
        await m.answer(f"🚫 Access revoked for {uid}")
    except Exception as e:
        await m.answer(f"Usage: <code>/take &lt;uid&gt;</code>\n{e}")


@admin_router.message(Command("say"))
async def cmd_say(m: Message, bot: Bot):
    """/say <uid> <message> — talk to a user through the bot."""
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid, text = m.text.split(" ", 2)
        await bot.send_message(int(uid), text)
        await m.answer("Sent.")
    except Exception as e:
        await m.answer(f"Usage: <code>/say &lt;uid&gt; &lt;text&gt;</code>\n{e}")


@admin_router.message(Command("id"))
async def cmd_id(m: Message):
    await m.answer(f"Chat ID: <code>{m.chat.id}</code>\nYour ID: <code>{m.from_user.id}</code>")


# ==========================================================================
#  9. BACKGROUND JOBS
# ==========================================================================

# admin can grant these exact spans from the orphan card (label -> days)
GRANT_SPANS = {
    "1d": 1, "2d": 2, "3d": 3, "4d": 4, "5d": 5, "6d": 6,
    "1w": 7, "2w": 14, "1m": 30,
}


async def grant_days(uid: int, plan: str, days: int):
    """Grant an arbitrary number of days (for partial/mismatch approvals)."""
    sub = await get_sub(uid)
    live = sub and sub.get("active") and sub["expires_at"] > now()
    start = sub["expires_at"] if live else now()
    await save_sub({
        "uid": uid, "plan": plan, "active": True,
        "started_at": now(), "expires_at": start + days * 86400,
        "notified": False,
    })


def orphan_kb(tx: str, cands: list[dict]) -> InlineKeyboardMarkup:
    """Buttons to credit a specific buyer, or dismiss."""
    rows = []
    for c in cands[:4]:
        who = f"@{c.get('username')}" if c.get("username") and c["username"] != "—" \
              else f"id {c['uid']}"
        rows.append([InlineKeyboardButton(
            text=f"✅ Credit {who}",
            callback_data=f"grec:{tx[:16]}:{c['uid']}")])
    rows.append([InlineKeyboardButton(
        text="🙈 Ignore", callback_data=f"gign:{tx[:16]}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def span_kb(uid: int) -> InlineKeyboardMarkup:
    """Duration picker shown after admin chooses who to credit."""
    labels = list(GRANT_SPANS.keys())
    rows, row = [], []
    for lab in labels:
        row.append(InlineKeyboardButton(text=lab, callback_data=f"gspn:{uid}:{lab}"))
        if len(row) == 3:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)





async def orphan_seen(tx: str) -> bool:
    doc = db.collection("nd_orphans").document(tx)
    snap = await doc.get()
    if snap.exists:
        return True
    await doc.set({"tx": tx, "at": now()})
    return False


async def recent_claimants(chain: str, amount: float) -> list[dict]:
    """People who tapped 'I've paid' recently — the likely sender of an
    unmatched payment. Closest expected amount first."""
    since = now() - ORPHAN_WINDOW
    q = db.collection(C_INV).where(
        filter=FieldFilter("chain", "==", chain))
    out = []
    async for d in q.stream():
        inv = d.to_dict()
        if not inv.get("claimed_paid_at"):
            continue
        if inv["claimed_paid_at"] < since:
            continue
        if inv["status"] == "paid":
            continue
        out.append(inv)
    out.sort(key=lambda i: abs(i["amount"] - amount))
    return out


async def alert_orphan(bot: Bot, chain: str, pay: dict, cands: list[dict]):
    if not INBOX_CHAT_ID or await orphan_seen(pay["tx"]):
        return
    ch = CHAINS[chain]

    # widen candidates: exact-fuzzy matches, plus anyone who recently claimed
    claimants = await recent_claimants(chain, pay["amount"])
    seen = {c["uid"] for c in cands}
    for c in claimants:
        if c["uid"] not in seen:
            cands.append(c)
            seen.add(c["uid"])

    # stash the decision so the buttons know the amount/plan later
    await db.collection("nd_orphans").document(pay["tx"]).set({
        "tx": pay["tx"], "chain": chain, "amount": pay["amount"],
        "at": now(), "resolved": False,
        "cands": [{"uid": c["uid"], "username": c.get("username", "—"),
                   "plan": c["plan"], "amount": c["amount"]} for c in cands[:4]],
    }, merge=True)

    if cands:
        lines = []
        for c in cands[:4]:
            who = f"@{c['username']}" if c.get("username") and c["username"] != "—" \
                  else f"id <code>{c['uid']}</code>"
            gap = pay["amount"] - c["amount"]
            tag = "exact" if abs(gap) < 1e-6 else f"{gap:+.2f} vs their {c['amount']}"
            lines.append(f"• {who} — {PLANS[c['plan']]['label']} ({tag})")
        body = ("Likely one of these — they tapped “I've paid” recently:\n"
                + "\n".join(lines) +
                "\n\nTap to credit, then pick how long.")
    else:
        body = ("Nobody has an open invoice near this amount.\n"
                "If you know who it is, use "
                "<code>/give &lt;uid&gt; &lt;plan&gt;</code>.")

    await bot.send_message(
        INBOX_CHAT_ID,
        f"💰 <b>Unmatched payment</b>\n\n"
        f"Received <b>{pay['amount']}</b> {ch['asset']} ({ch['label']})\n"
        f"Comment: <code>{pay['memo'] or '—'}</code>\n"
        f"Tx: <code>{pay['tx'][:24]}…</code>\n\n{body}",
        reply_markup=orphan_kb(pay["tx"], cands) if cands else None)


async def job_reconcile(bot: Bot):
    """Catch payments where someone rounded the amount off."""
    pend = [i for i in await pending_invoices()
            if now() - i["created"] < INVOICE_TTL_MIN * 60]
    if not pend:
        return

    by_chain: dict[str, list[dict]] = {}
    for inv in pend:
        by_chain.setdefault(inv["chain"], []).append(inv)

    for chain, invs in by_chain.items():
        try:
            payments = await incoming(chain)
        except Exception:
            continue
        for pay in payments:
            if pay["ts"] < now() - ORPHAN_WINDOW or await tx_seen(pay["tx"]):
                continue

            cands = [i for i in invs
                     if abs(pay["amount"] - i["amount"])
                     <= max(FUZZY_TOL * i["amount"], 2 * DUST_STEP[chain])]

            if len(cands) == 1:
                inv = cands[0]
                await close_invoice(inv["id"], "paid", pay["tx"])
                await grant(inv["uid"], inv["plan"])
                log.info("PAID(rounded) %s uid=%s got=%s want=%s",
                         inv["id"], inv["uid"], pay["amount"], inv["amount"])
                fresh = await get_invoice(inv["id"])
                await notify_paid(bot, fresh or inv)
                invs.remove(inv)
            else:
                await alert_orphan(bot, chain, pay, cands)


async def job_invoices(bot: Bot):
    for inv in await pending_invoices():
        try:
            if S_expired(inv):
                await close_invoice(inv["id"], "expired")
                await render_card(bot, inv, "expired")
                continue
            if await settle(inv):
                await notify_paid(bot, inv)
        except Exception as e:
            log.warning("settle %s: %s", inv["id"], e)


def S_expired(inv: dict) -> bool:
    return now() - inv["created"] > INVOICE_TTL_MIN * 60


async def job_expiry(bot: Bot):
    for sub in await all_active_subs():
        if sub["expires_at"] + GRACE_HOURS * 3600 < now():
            await revoke(sub["uid"])
            try:
                await bot.send_message(
                    sub["uid"], "⏳ Your subscription ended. /plans to renew.")
            except Exception:
                pass
        elif not sub.get("notified") and sub["expires_at"] - now() < 86400:
            await save_sub({**sub, "notified": True})
            try:
                await bot.send_message(
                    sub["uid"], "⚠️ Subscription expires in under 24h. /plans to renew.")
            except Exception:
                pass


async def job_stale(bot: Bot):
    if not INBOX_CHAT_ID:
        return
    for o in await open_orders():
        if o.get("nudged") or now() - o["created"] < STALE_HOURS * 3600:
            continue
        await update_order(o["id"], nudged=True)
        try:
            await bot.send_message(
                INBOX_CHAT_ID,
                f"⏰ <b>#{o['id']}</b> has been sitting {STALE_HOURS}h+ — @{o['username']}")
        except Exception:
            pass


def start_jobs(bot: Bot):
    sch = AsyncIOScheduler()
    sch.add_job(job_invoices, "interval", seconds=45, args=[bot])
    sch.add_job(job_reconcile, "interval", seconds=90, args=[bot])
    sch.add_job(job_expiry, "interval", minutes=15, args=[bot])
    sch.add_job(job_stale, "interval", minutes=30, args=[bot])
    sch.start()
    return sch


# ==========================================================================
#  10. BOOT
# ==========================================================================

async def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.include_router(admin_router)   # admin first — it owns the inbox chat
    dp.include_router(user_router)

    start_jobs(bot)

    me = await bot.get_me()
    log.info("Running as @%s | admins=%s | inbox=%s", me.username, ADMIN_IDS, INBOX_CHAT_ID)
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
