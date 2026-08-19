"""
Database layer — talks to Postgres (Supabase) via DATABASE_URL.
Used by main.py (bot + web + scheduler) and checker.py.

Design notes:
- view_count column is kept in the schema but never written to for now
  (feature intentionally disabled) — re-enabling it later only needs a
  change in checker.py, not here or in the dashboard templates.
- live_status of 'unavailable' means both the lightweight check and the
  Playwright fallback failed to get a clear answer — surfaced on the
  dashboard as "flagged for review".
"""

import os
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def get_db():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Create all tables if they don't exist yet. Safe to call every startup."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS profiles (
            id SERIAL PRIMARY KEY,
            discord_user_id TEXT NOT NULL,
            discord_username TEXT NOT NULL,
            ig_username TEXT NOT NULL,
            ig_link TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',   -- active | banned
            created_at DOUBLE PRECISION NOT NULL,
            banned_at DOUBLE PRECISION
        );

        CREATE INDEX IF NOT EXISTS idx_profiles_user ON profiles(discord_user_id);
        CREATE INDEX IF NOT EXISTS idx_profiles_status ON profiles(discord_user_id, status);

        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            profile_id INTEGER NOT NULL REFERENCES profiles(id),
            discord_user_id TEXT NOT NULL,
            post_type TEXT NOT NULL,                 -- reel | carousel
            post_link TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',   -- pending | validated
            submitted_at DOUBLE PRECISION NOT NULL,
            validated_at DOUBLE PRECISION,
            view_count INTEGER,                        -- kept for future use, always NULL for now
            live_status TEXT,                            -- live | banned | unavailable | not_checked
            checked_at DOUBLE PRECISION
        );

        CREATE INDEX IF NOT EXISTS idx_posts_profile ON posts(profile_id);
        CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(discord_user_id);
        CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
        CREATE INDEX IF NOT EXISTS idx_posts_live_status ON posts(live_status);

        CREATE TABLE IF NOT EXISTS scraper_accounts (
            id SERIAL PRIMARY KEY,
            label TEXT NOT NULL,
            ig_username TEXT NOT NULL,
            ig_password TEXT NOT NULL,
            proxy_url TEXT,                            -- nullable; empty = no proxy
            session_state TEXT,                        -- serialized Playwright storage_state
            health TEXT NOT NULL DEFAULT 'active',      -- active | cooling_down | flagged | logged_out
            daily_check_count INTEGER NOT NULL DEFAULT 0,
            daily_count_reset_at DOUBLE PRECISION,
            last_used_at DOUBLE PRECISION,
            created_at DOUBLE PRECISION NOT NULL
        );

        CREATE TABLE IF NOT EXISTS admin_users (
            id SERIAL PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at DOUBLE PRECISION NOT NULL
        );
        """)
        cur.close()


# ---------- Profiles ----------

def get_active_profile(discord_user_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM profiles WHERE discord_user_id=%s AND status='active'",
            (discord_user_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def add_profile(discord_user_id: str, discord_username: str, ig_username: str, ig_link: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO profiles (discord_user_id, discord_username, ig_username, ig_link, status, created_at) "
            "VALUES (%s, %s, %s, %s, 'active', %s) RETURNING id",
            (discord_user_id, discord_username, ig_username, ig_link, time.time()),
        )
        return cur.fetchone()["id"]


def ban_active_profile(discord_user_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE profiles SET status='banned', banned_at=%s WHERE discord_user_id=%s AND status='active'",
            (time.time(), discord_user_id),
        )
        return cur.rowcount > 0


def get_user_profiles(discord_user_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM profiles WHERE discord_user_id=%s ORDER BY created_at DESC",
            (discord_user_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_all_profiles(search: str = None):
    """search filters by discord_username (case-insensitive partial match)."""
    with get_db() as conn:
        cur = conn.cursor()
        if search:
            cur.execute(
                "SELECT * FROM profiles WHERE discord_username ILIKE %s ORDER BY created_at DESC",
                (f"%{search}%",),
            )
        else:
            cur.execute("SELECT * FROM profiles ORDER BY created_at DESC")
        return [dict(r) for r in cur.fetchall()]


# ---------- Posts (reels / carousels) ----------

def get_last_post_for_profile(profile_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM posts WHERE profile_id=%s ORDER BY submitted_at DESC LIMIT 1",
            (profile_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def add_post(profile_id: int, discord_user_id: str, post_type: str, post_link: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO posts (profile_id, discord_user_id, post_type, post_link, status, submitted_at, live_status) "
            "VALUES (%s, %s, %s, %s, 'pending', %s, 'not_checked') RETURNING id",
            (profile_id, discord_user_id, post_type, post_link, time.time()),
        )
        return cur.fetchone()["id"]


def get_user_posts(discord_user_id: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM posts WHERE discord_user_id=%s ORDER BY submitted_at DESC",
            (discord_user_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def get_pending_posts_due_for_validation(cutoff_ts: float):
    """Posts submitted more than 12h ago that are still pending."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM posts WHERE status='pending' AND submitted_at<=%s",
            (cutoff_ts,),
        )
        return [dict(r) for r in cur.fetchall()]


def mark_post_validated_and_checked(post_id: int, live_status: str, view_count=None):
    """view_count is accepted but defaults to None — kept for easy re-enabling later."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE posts SET status='validated', validated_at=%s, view_count=%s, live_status=%s, checked_at=%s "
            "WHERE id=%s",
            (time.time(), view_count, live_status, time.time(), post_id),
        )


def get_all_posts(search: str = None):
    """search filters by discord_user_id (exact) — dashboard passes the resolved ID from a username lookup."""
    with get_db() as conn:
        cur = conn.cursor()
        if search:
            cur.execute(
                "SELECT * FROM posts WHERE discord_user_id=%s ORDER BY submitted_at DESC",
                (search,),
            )
        else:
            cur.execute("SELECT * FROM posts ORDER BY submitted_at DESC")
        return [dict(r) for r in cur.fetchall()]


def get_flagged_posts():
    """Posts where both check methods failed to get a clear answer — needs manual review."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM posts WHERE live_status='unavailable' ORDER BY checked_at DESC"
        )
        return [dict(r) for r in cur.fetchall()]


def get_user_post_summary(discord_user_id: str):
    """Quick counts for a user: total, live, banned, pending — used for the admin per-user summary."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE live_status='live') AS live,
                COUNT(*) FILTER (WHERE live_status='banned') AS banned,
                COUNT(*) FILTER (WHERE status='pending') AS pending
            FROM posts WHERE discord_user_id=%s
        """, (discord_user_id,))
        return dict(cur.fetchone())


# ---------- Scraper account pool ----------

def add_scraper_account(label: str, ig_username: str, ig_password: str, proxy_url: str = None):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO scraper_accounts (label, ig_username, ig_password, proxy_url, health, daily_check_count, created_at) "
            "VALUES (%s, %s, %s, %s, 'active', 0, %s) RETURNING id",
            (label, ig_username, ig_password, proxy_url, time.time()),
        )
        return cur.fetchone()["id"]


def get_healthy_scraper_accounts():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM scraper_accounts WHERE health IN ('active','cooling_down') "
            "ORDER BY last_used_at ASC NULLS FIRST"
        )
        return [dict(r) for r in cur.fetchall()]


def set_scraper_health(account_id: int, health: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE scraper_accounts SET health=%s WHERE id=%s", (health, account_id))


def save_scraper_session(account_id: int, session_state_json: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE scraper_accounts SET session_state=%s, last_used_at=%s WHERE id=%s",
            (session_state_json, time.time(), account_id),
        )


def bump_scraper_usage(account_id: int):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE scraper_accounts SET daily_check_count=daily_check_count+1, last_used_at=%s WHERE id=%s",
            (time.time(), account_id),
        )


def reset_daily_counts():
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE scraper_accounts SET daily_check_count=0, daily_count_reset_at=%s", (time.time(),))


# ---------- Admin auth ----------

def create_admin_user(username: str, password_hash: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO admin_users (username, password_hash, created_at) VALUES (%s, %s, %s)",
            (username, password_hash, time.time()),
        )


def get_admin_user(username: str):
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM admin_users WHERE username=%s", (username,))
        row = cur.fetchone()
        return dict(row) if row else None