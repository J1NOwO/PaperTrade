import hashlib
import os
import secrets
import sqlite3
from contextlib import contextmanager

DB_PATH = "papertrade.db"


# ──────────────────────────────────────────────────────────
# Password hashing (PBKDF2-SHA256, stdlib only)
# ──────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    salt = os.urandom(16)
    key  = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260_000)
    return salt.hex() + ":" + key.hex()


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, key_hex = stored.split(":")
        salt = bytes.fromhex(salt_hex)
        key  = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260_000)
        return secrets.compare_digest(key.hex(), key_hex)
    except Exception:
        return False


# ──────────────────────────────────────────────────────────
# DB init & migration
# ──────────────────────────────────────────────────────────

def init_db():
    with get_db() as conn:
        # Auth tables (always safe to create)
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        """)

        # Detect old schema (no user_id on settings/balances)
        _migrate(conn)


def _migrate(conn):
    """Recreate data tables with user_id if upgrading from single-user version."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(settings)").fetchall()}
    if "user_id" in cols:
        # Incremental column migrations for transactions
        tx_cols = {r[1] for r in conn.execute("PRAGMA table_info(transactions)").fetchall()}
        if "fee" not in tx_cols:
            conn.execute("ALTER TABLE transactions ADD COLUMN fee REAL DEFAULT 0")
        if "fee_amount" not in tx_cols:
            conn.execute("ALTER TABLE transactions ADD COLUMN fee_amount REAL DEFAULT 0")
        if "tax_amount" not in tx_cols:
            conn.execute("ALTER TABLE transactions ADD COLUMN tax_amount REAL DEFAULT 0")
        if "slippage_amount" not in tx_cols:
            conn.execute("ALTER TABLE transactions ADD COLUMN slippage_amount REAL DEFAULT 0")
        if "sec_fee" not in tx_cols:
            conn.execute("ALTER TABLE transactions ADD COLUMN sec_fee REAL DEFAULT 0")
        # Migrate pending_orders new columns
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pending_orders'").fetchone():
            po_cols = {r[1] for r in conn.execute("PRAGMA table_info(pending_orders)").fetchall()}
            if "trigger_type" not in po_cols:
                conn.execute("ALTER TABLE pending_orders ADD COLUMN trigger_type TEXT")
            if "parent_order_id" not in po_cols:
                conn.execute("ALTER TABLE pending_orders ADD COLUMN parent_order_id INTEGER")
            if "oco_group_id" not in po_cols:
                conn.execute("ALTER TABLE pending_orders ADD COLUMN oco_group_id TEXT")
        # short_positions leverage columns
        sp_cols = {r[1] for r in conn.execute("PRAGMA table_info(short_positions)").fetchall()}
        if "leverage" not in sp_cols:
            conn.execute("ALTER TABLE short_positions ADD COLUMN leverage REAL DEFAULT 1.0")
        if "margin_amount" not in sp_cols:
            conn.execute("ALTER TABLE short_positions ADD COLUMN margin_amount REAL DEFAULT 0.0")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_orders (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                ticker          TEXT    NOT NULL,
                market          TEXT    NOT NULL,
                order_type      TEXT    NOT NULL,
                quantity        REAL    NOT NULL,
                limit_price     REAL    NOT NULL,
                currency        TEXT    NOT NULL,
                created_at      TEXT    NOT NULL,
                status          TEXT    DEFAULT 'PENDING',
                trigger_type    TEXT,
                parent_order_id INTEGER,
                oco_group_id    TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS short_positions (
                id                INTEGER   PRIMARY KEY AUTOINCREMENT,
                user_id           INTEGER   NOT NULL,
                ticker            TEXT      NOT NULL,
                market            TEXT      NOT NULL,
                quantity          REAL      NOT NULL,
                entry_price       REAL      NOT NULL,
                currency          TEXT      NOT NULL,
                daily_borrow_rate REAL      DEFAULT 0.0003,
                opened_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status            TEXT      DEFAULT 'OPEN',
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leveraged_positions (
                id                  INTEGER   PRIMARY KEY AUTOINCREMENT,
                user_id             INTEGER   NOT NULL,
                ticker              TEXT      NOT NULL,
                market              TEXT      NOT NULL,
                quantity            REAL      NOT NULL,
                entry_price         REAL      NOT NULL,
                leverage            INTEGER   NOT NULL,
                margin_amount       REAL      NOT NULL,
                borrowed_amount     REAL      NOT NULL,
                currency            TEXT      NOT NULL,
                daily_interest_rate REAL      DEFAULT 0.0002,
                opened_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status              TEXT      DEFAULT 'OPEN',
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dividend_checks (
                ticker       TEXT PRIMARY KEY,
                last_checked TIMESTAMP NOT NULL
            )
        """)
        if "position_type" not in tx_cols:
            conn.execute("ALTER TABLE transactions ADD COLUMN position_type TEXT DEFAULT 'LONG'")
        return  # already new schema

    # Drop old single-user tables (data loss is acceptable on schema upgrade)
    conn.executescript("""
        DROP TABLE IF EXISTS settings;
        DROP TABLE IF EXISTS balances;
        DROP TABLE IF EXISTS holdings;
        DROP TABLE IF EXISTS transactions;

        CREATE TABLE settings (
            user_id INTEGER NOT NULL,
            key     TEXT    NOT NULL,
            value   TEXT    NOT NULL,
            PRIMARY KEY (user_id, key),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE balances (
            user_id  INTEGER NOT NULL,
            currency TEXT    NOT NULL,
            amount   REAL    NOT NULL,
            PRIMARY KEY (user_id, currency),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE holdings (
            user_id   INTEGER NOT NULL,
            symbol    TEXT    NOT NULL,
            market    TEXT    NOT NULL,
            quantity  REAL    NOT NULL,
            avg_price REAL    NOT NULL,
            currency  TEXT    NOT NULL,
            PRIMARY KEY (user_id, symbol),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE transactions (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id   INTEGER NOT NULL,
            symbol    TEXT    NOT NULL,
            market    TEXT    NOT NULL,
            action    TEXT    NOT NULL,
            quantity  REAL    NOT NULL,
            price     REAL    NOT NULL,
            total     REAL    NOT NULL,
            currency  TEXT    NOT NULL,
            timestamp  TEXT    NOT NULL,
            fee             REAL    DEFAULT 0,
            fee_amount      REAL    DEFAULT 0,
            tax_amount      REAL    DEFAULT 0,
            slippage_amount REAL    DEFAULT 0,
            sec_fee         REAL    DEFAULT 0,
            position_type   TEXT    DEFAULT 'LONG',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS short_positions (
            id                INTEGER   PRIMARY KEY AUTOINCREMENT,
            user_id           INTEGER   NOT NULL,
            ticker            TEXT      NOT NULL,
            market            TEXT      NOT NULL,
            quantity          REAL      NOT NULL,
            entry_price       REAL      NOT NULL,
            currency          TEXT      NOT NULL,
            daily_borrow_rate REAL      DEFAULT 0.0003,
            opened_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status            TEXT      DEFAULT 'OPEN',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS leveraged_positions (
            id                  INTEGER   PRIMARY KEY AUTOINCREMENT,
            user_id             INTEGER   NOT NULL,
            ticker              TEXT      NOT NULL,
            market              TEXT      NOT NULL,
            quantity            REAL      NOT NULL,
            entry_price         REAL      NOT NULL,
            leverage            INTEGER   NOT NULL,
            margin_amount       REAL      NOT NULL,
            borrowed_amount     REAL      NOT NULL,
            currency            TEXT      NOT NULL,
            daily_interest_rate REAL      DEFAULT 0.0002,
            opened_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status              TEXT      DEFAULT 'OPEN',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS dividend_checks (
            ticker       TEXT PRIMARY KEY,
            last_checked TIMESTAMP NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending_orders (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         INTEGER NOT NULL,
            ticker          TEXT    NOT NULL,
            market          TEXT    NOT NULL,
            order_type      TEXT    NOT NULL,
            quantity        REAL    NOT NULL,
            limit_price     REAL    NOT NULL,
            currency        TEXT    NOT NULL,
            created_at      TEXT    NOT NULL,
            status          TEXT    DEFAULT 'PENDING',
            trigger_type    TEXT,
            parent_order_id INTEGER,
            oco_group_id    TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)



# ──────────────────────────────────────────────────────────
# Connection helper
# ──────────────────────────────────────────────────────────

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
