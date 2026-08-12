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
import json
import logging
import os
import random
import secrets
import time

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
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

WELCOME = (
    "👋 <b>Hi, ready to work?</b>\n\n"
    "Pick what you need below. Send me a sample or a link — "
    "I handle it by hand and send the result straight back here.\n\n"
    "Usually within a few hours."
)

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
        return round(usd / await ton_usd_rate(), 4)
    return round(usd, 2)


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
            "memo": (m.get("message") or "").strip(),
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


async def open_invoice(uid: int, plan: str, chain: str) -> dict:
    base = await quote(chain, PLANS[plan]["price_usd"])
    dust = random.randint(1, 9999) / 1e6          # makes each amount unique
    amount = round(base + dust, 6)
    memo = f"ND{random.randint(100000, 999999)}" if CHAINS[chain]["memo"] else ""
    return await new_invoice(uid, plan, chain, amount, memo)


def _matches(inv: dict, pay: dict) -> bool:
    if pay["ts"] < inv["created"] - 120:
        return False
    if inv["memo"]:
        return inv["memo"].lower() in pay["memo"].lower()
    return abs(pay["amount"] - inv["amount"]) < 1e-6


async def settle(inv: dict) -> bool:
    if inv["status"] != "pending":
        return inv["status"] == "paid"
    if now() - inv["created"] > INVOICE_TTL_MIN * 60:
        await close_invoice(inv["id"], "expired")
        return False
    for pay in await incoming(inv["chain"]):
        if not _matches(inv, pay) or await tx_seen(pay["tx"]):
            continue
        await close_invoice(inv["id"], "paid", pay["tx"])
        await grant(inv["uid"], inv["plan"])
        log.info("PAID %s uid=%s %s", inv["id"], inv["uid"], inv["plan"])
        return True
    return False


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
async def cb_chain(cq: CallbackQuery):
    _, plan, chain = cq.data.split(":")
    await cq.answer("Building invoice…")
    try:
        inv = await open_invoice(cq.from_user.id, plan, chain)
    except Exception as e:
        log.error("invoice fail: %s", e)
        return await cq.message.edit_text("⚠️ Couldn't build the invoice. Try again in a minute.")

    ch = CHAINS[chain]
    memo = (f"\n<b>Memo / comment:</b>\n<code>{inv['memo']}</code>  ← required"
            if inv["memo"] else "")
    await cq.message.edit_text(
        f"<b>{PLANS[plan]['label']} plan</b>\n\n"
        f"Send <b>exactly</b>:\n<code>{inv['amount']}</code> {ch['asset']}\n\n"
        f"<b>To ({ch['label']}):</b>\n<code>{ch['wallet']}</code>{memo}\n\n"
        f"⚠️ The exact amount is how I find your payment — don't round it.\n"
        f"Invoice expires in {INVOICE_TTL_MIN} min.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ I've paid", callback_data=f"check:{inv['id']}")],
            [InlineKeyboardButton(text="⬅️ Back", callback_data="plans")],
        ]))


@user_router.callback_query(F.data.startswith("check:"))
async def cb_check(cq: CallbackQuery):
    inv = await get_invoice(cq.data.split(":")[1])
    if not inv:
        return await cq.answer("Invoice not found.", show_alert=True)
    await cq.answer("Checking the chain…")
    if await settle(inv):
        await cq.message.edit_text(
            f"✅ <b>Payment confirmed</b>\n\n{await status_line(cq.from_user.id)}\n\n"
            "Everything's unlocked. /menu to start.")
    else:
        await cq.answer("Not on chain yet. Wait a minute and tap again.", show_alert=True)


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

    note = m.text or m.caption or f"[{m.content_type}]"
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


@admin_router.message(Command("queue"))
async def cmd_queue(m: Message):
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
async def cmd_give(m: Message):
    """/give <uid> <daily|weekly|monthly>"""
    if not is_admin(m.from_user.id):
        return
    try:
        _, uid, plan = m.text.split()
        await grant(int(uid), plan)
        await m.answer(f"✅ {plan} granted to {uid}")
    except Exception as e:
        await m.answer(f"Usage: <code>/give &lt;uid&gt; &lt;daily|weekly|monthly&gt;</code>\n{e}")


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

async def job_invoices():
    for inv in await pending_invoices():
        try:
            await settle(inv)
        except Exception as e:
            log.warning("settle %s: %s", inv["id"], e)


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
    sch.add_job(job_invoices, "interval", seconds=45)
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
