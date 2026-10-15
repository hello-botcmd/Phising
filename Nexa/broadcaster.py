import os
import asyncio
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (FloodWait, BadRequest, UserDeactivated,
                             AuthKeyUnregistered, PeerIdInvalid, ChannelInvalid)

from config import (
    API_ID, API_HASH, BROADCAST_WORKERS,
    FIRST_AD_DELAY_MIN,
    CONTACT_LIMIT_PER_ROUND, CONTACT_DM_GAP_SEC, GROUP_DM_GAP_SEC,
)
from Nexa.database import (
    users_col, ads_config_col, broadcast_msgs_col,
    update_ads_config, inc_ads_config,
)

# Regular (non-contact) DM gap — Option B
REGULAR_DM_GAP_SEC = 0.5


def _chat_type_value(t):
    return getattr(t, "value", t)


def _clean_ads(config):
    """Ads are buttons only. Missing has_button counts as True (legacy compat)."""
    out = []
    for ad in config.get("ads", []):
        has_button = ad.get("has_button", True)
        if not has_button:
            continue
        label = (ad.get("label") or "").strip()
        url   = (ad.get("url") or "").strip()
        if label and url.startswith(("http://", "https://")):
            out.append({"has_button": True, "label": label, "url": url})
    return out


def _user_created_dt(user):
    ca = user.get("created_at")
    if isinstance(ca, datetime):
        return ca
    if isinstance(ca, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(ca, fmt)
            except ValueError:
                continue
    return None


class AdsBroadcaster:
    def __init__(self):
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._round_count = 0
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=BROADCAST_WORKERS,
                                        thread_name_prefix="bc")

    def is_running(self):
        return self._running

    def start(self):
        with self._lock:
            if self._running:
                return False
            self._stop_event.clear()
            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        print("📢 Broadcaster started")
        return True

    def stop(self):
        with self._lock:
            if not self._running:
                return False
            self._stop_event.set()
            self._running = False
            t = self._thread
        if t:
            t.join(timeout=5)
        update_ads_config({"next_broadcast_at": None})
        print("📢 Broadcaster stopped")
        return True

    def _get_interval(self):
        if ads_config_col is not None:
            c = ads_config_col.find_one({"_id": "main_config"})
            if c:
                return max(1, int(c.get("interval", 600)))
        return 600

    def _wait(self, seconds):
        elapsed = 0
        while not self._stop_event.is_set():
            if elapsed >= self._get_interval():
                return False
            self._stop_event.wait(1)
            elapsed += 1
        return True

    def _run(self):
        while not self._stop_event.is_set():
            secs = self._get_interval()
            nxt = datetime.now() + timedelta(seconds=secs)
            update_ads_config({"next_broadcast_at": nxt})
            print(f"⏳ Next broadcast at {nxt:%H:%M:%S}")
            if self._wait(secs):
                break
            if self._stop_event.is_set():
                break
            try:
                self._run_round()
            except Exception as e:
                print(f"Round error: {e}")

    # ---------- round ----------

    def _run_round(self):
        self._round_count += 1
        bid = f"round_{self._round_count}_{datetime.now():%Y%m%d_%H%M%S}"
        print(f"\n{'=' * 50}\n🚀 ROUND #{self._round_count} ({bid})\n{'=' * 50}")

        if users_col is None or ads_config_col is None:
            return

        config = ads_config_col.find_one({"_id": "main_config"})
        if not config or not config.get("ads_enabled", False):
            return

        # Delete previous messages only if the toggle is ON
        if config.get("delete_after_round", False):
            self._delete_previous_messages()
        else:
            print("📎 Keep mode — previous messages are NOT deleted")

        caption    = (config.get("caption") or "").strip()
        ads        = _clean_ads(config)
        media_path = config.get("media_path") or config.get("photo_path")
        media_type = config.get("media_type") or "photo"

        if not caption and not ads and not media_path:
            print("📭 nothing to send")
            return

        raw_users = list(users_col.find({"ads_enabled": True}))

        # 30-min grace
        now = datetime.now()
        grace = timedelta(minutes=FIRST_AD_DELAY_MIN)

        candidates = []
        for u in raw_users:
            created = _user_created_dt(u)
            if created is None or (now - created) < grace:
                print(f"⏳ {u.get('phone', '?')} still in grace period ({FIRST_AD_DELAY_MIN} min)")
                continue
            candidates.append(u)

        # Verify sessions
        valid = []
        for u in candidates:
            if self._verify_user_session(u):
                valid.append(u)
            else:
                users_col.update_one(
                    {"_id": u["_id"]},
                    {"$set": {"ads_enabled": False, "status": "expired"}}
                )
                print(f"❌ Session expired for {u.get('phone', '?')}")

        if not valid:
            print("📭 no valid users")
            return

        print(f"📨 sending to {len(valid)} users (waiting for all to finish before next round)")
        futures = []
        for u in valid:
            if self._stop_event.is_set():
                break
            futures.append(self._pool.submit(
                self._send_to_user, u, caption, ads, media_path, media_type, bid
            ))

        # Wait for every worker to finish — prevents overlapping rounds
        for f in futures:
            try:
                f.result()
            except Exception as e:
                print(f"Worker error: {e}")

        inc_ads_config("total_broadcasts", 1)
        update_ads_config({"last_broadcast_at": datetime.now()})
        print(f"✅ Round #{self._round_count} complete")

    # ---------- session check ----------

    def _verify_user_session(self, user):
        ss = user.get("session_string")
        if not ss:
            return False

        async def check():
            c = Client(name=f"chk_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=ss,
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                return (await c.get_me()) is not None
            except (AuthKeyUnregistered, UserDeactivated):
                return False
            except Exception:
                return True  # transient — keep user
            finally:
                try:
                    await c.disconnect()
                except Exception:
                    pass

        try:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                r = loop.run_until_complete(check())
            finally:
                loop.close()
            return r
        except Exception:
            return True

    # ---------- cleanup (one client per session) ----------

    def _delete_previous_messages(self):
        if broadcast_msgs_col is None:
            return
        old = list(broadcast_msgs_col.find())
        if not old:
            return
        print(f"🗑️ deleting {len(old)} tracked messages (one client per session)")

        # Group messages by session_string
        by_session = {}
        for m in old:
            ss = m.get("session_string")
            if not ss:
                broadcast_msgs_col.delete_one({"_id": m["_id"]})
                continue
            by_session.setdefault(ss, []).append(m)

        for ss, msgs in by_session.items():
            async def batch(ss=ss, msgs=msgs):
                c = Client(name=f"del_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                           api_hash=API_HASH, session_string=ss,
                           in_memory=True, no_updates=True)
                try:
                    await c.connect()
                    c.me = await c.get_me()
                    for m in msgs:
                        try:
                            await c.delete_messages(m["chat_id"], m["message_id"])
                            broadcast_msgs_col.delete_one({"_id": m["_id"]})
                        except Exception as e:
                            print(f"⚠️ delete msg {m['message_id']}: {e}")
                except Exception as e:
                    print(f"⚠️ batch session error: {e}")
                finally:
                    try:
                        await c.disconnect()
                    except Exception:
                        pass

            try:
                loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(batch())
                finally:
                    loop.close()
            except Exception as e:
                print(f"⚠️ batch loop error: {e}")

    # ---------- layout ----------

    async def _send_layout(self, client, chat_id, caption, ads,
                           media_path, media_type):
        """ONE message: media (or text) + caption + ALL ad buttons inline."""
        buttons = [
            [InlineKeyboardButton(a["label"], url=a["url"])]
            for a in ads if a.get("has_button")
        ]
        kb = InlineKeyboardMarkup(buttons) if buttons else None

        if media_path and os.path.exists(media_path):
            if media_type == "video":
                m = await client.send_video(
                    chat_id, video=media_path,
                    caption=caption or "", reply_markup=kb,
                )
            else:
                m = await client.send_photo(
                    chat_id, photo=media_path,
                    caption=caption or "", reply_markup=kb,
                )
        else:
            m = await client.send_message(
                chat_id, caption or " ", reply_markup=kb,
            )
        return [m.id]

    # ---------- per-user send ----------

    def _send_to_user(self, user, caption, ads, media_path, media_type, bid):
        uid   = user.get("user_id")
        phone = user.get("phone", "?")
        ss    = user.get("session_string")
        if not ss:
            return
        sent = []

        async def work():
            c = Client(name=f"bc_{uid}_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=ss,
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                me = await c.get_me()
                if not me:
                    raise AuthKeyUnregistered("bad session")
                c.me = me  # fixes is_premium crash

                # 1) Saved Messages
                try:
                    for mid in await self._send_layout(c, "me", caption, ads,
                                                       media_path, media_type):
                        sent.append(("me", mid))
                    print(f"✅ DM sent to {phone}")
                except Exception as e:
                    print(f"❌ DM {phone}: {e}")

                # Fetch contacts set for gap classification
                try:
                    contacts = await c.get_contacts()
                    contact_ids = {ct.id for ct in contacts}
                except Exception as e:
                    print(f"⚠️ get_contacts {phone}: {e}")
                    contact_ids = set()

                # 2/3/4) All dialogs — groups, contacts, regular DMs
                group_count = 0
                contact_count = 0
                dm_count = 0
                try:
                    async for d in c.get_dialogs():
                        if self._stop_event.is_set():
                            break
                        if not d or not d.chat:
                            continue
                        chat = d.chat
                        ctype = _chat_type_value(chat.type)

                        # ---- Groups / Supergroups ----
                        if ctype in ("group", "supergroup"):
                            try:
                                for mid in await self._send_layout(
                                    c, chat.id, caption, ads, media_path, media_type
                                ):
                                    sent.append((chat.id, mid))
                                group_count += 1
                                await asyncio.sleep(GROUP_DM_GAP_SEC)
                            except FloodWait as fw:
                                await asyncio.sleep(min(fw.value, 30))
                            except (PeerIdInvalid, ChannelInvalid, ValueError, BadRequest):
                                continue
                            except Exception as e:
                                print(f"⚠️ group {chat.id}: {e}")
                            continue

                        # ---- Private chats ----
                        if ctype == "private":
                            if chat.id == me.id:
                                continue
                            if getattr(chat, "is_bot", False):
                                continue
                            if getattr(chat, "is_deleted", False):
                                continue
                            if getattr(chat, "is_scam", False):
                                continue
                            if getattr(chat, "is_fake", False):
                                continue

                            is_contact = chat.id in contact_ids
                            gap = CONTACT_DM_GAP_SEC if is_contact else REGULAR_DM_GAP_SEC

                            try:
                                for mid in await self._send_layout(
                                    c, chat.id, caption, ads, media_path, media_type
                                ):
                                    sent.append((chat.id, mid))
                                if is_contact:
                                    contact_count += 1
                                else:
                                    dm_count += 1
                                await asyncio.sleep(gap)
                            except FloodWait as fw:
                                await asyncio.sleep(min(fw.value, 30))
                            except (PeerIdInvalid, BadRequest):
                                continue
                            except Exception as e:
                                print(f"⚠️ dm {chat.id}: {e}")
                            continue

                        # channels skipped

                    print(f"✅ {phone}: {group_count} groups, "
                          f"{contact_count} contacts, {dm_count} DMs")
                except Exception as e:
                    print(f"❌ dialogs {phone}: {e}")

                # stats
                if sent:
                    users_col.update_one(
                        {"_id": user["_id"]},
                        {"$set": {"last_ad_time": datetime.now(),
                                  "last_broadcast_id": bid},
                         "$inc": {"deliveries": 1}}
                    )
                else:
                    users_col.update_one({"_id": user["_id"]},
                                         {"$inc": {"failures": 1}})

            finally:
                try:
                    await c.disconnect()
                except Exception:
                    pass

        try:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(work())
            finally:
                loop.close()
        except (UserDeactivated, AuthKeyUnregistered):
            users_col.update_one({"_id": user["_id"]},
                                 {"$set": {"ads_enabled": False, "status": "expired"}})
        except Exception as e:
            print(f"❌ {phone}: {e}")

        # ALWAYS record sent messages — delete toggle decides whether to clean next round
        if broadcast_msgs_col is not None and sent:
            broadcast_msgs_col.insert_many([
                {"user_id": uid, "phone": phone, "broadcast_id": bid,
                 "chat_id": str(cid), "message_id": mid,
                 "session_string": ss, "sent_at": datetime.now()}
                for cid, mid in sent
            ])


ads_broadcaster = AdsBroadcaster()
