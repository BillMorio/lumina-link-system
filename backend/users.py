"""Console logins: username + password, created only from this CLI.

Tokens are stateless and signed: user.expiry.sig, where the signature also covers the
password hash, so a password reset or disable ends every session of that user.

    .venv/bin/python users.py add <username>       # prints a new password
    .venv/bin/python users.py reset <username>     # prints a new password
    .venv/bin/python users.py disable <username>
    .venv/bin/python users.py enable <username>
    .venv/bin/python users.py list
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import sys
import time

import db

ITER = 200_000
TOKEN_DAYS = 30


_ready = False


def _ensure() -> None:
    global _ready
    if _ready:
        return
    _ready = True
    db.ex("""CREATE TABLE IF NOT EXISTS users (
                 id         INTEGER PRIMARY KEY AUTOINCREMENT,
                 username   TEXT NOT NULL UNIQUE COLLATE NOCASE,
                 pw_hash    TEXT NOT NULL,
                 active     INTEGER NOT NULL DEFAULT 1,
                 created_at INTEGER NOT NULL)""")


def _hash(pw: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    return f"{salt}${hashlib.pbkdf2_hmac('sha256', pw.encode(), salt.encode(), ITER).hex()}"


def _secret() -> bytes:
    from config import API_ACCESS_CODE, SESSION_SECRET
    return (SESSION_SECRET or API_ACCESS_CODE).encode()


def _sig(username: str, exp: int, pw_hash: str) -> str:
    return hmac.new(_secret(), f"{username.lower()}|{exp}|{pw_hash}".encode(), hashlib.sha256).hexdigest()


def _get(username: str) -> dict | None:
    _ensure()
    return db.q1("SELECT * FROM users WHERE username = ?", (username.strip(),))


def check_password(username: str, password: str) -> dict | None:
    u = _get(username)
    if not u or not u["active"]:
        _hash(password)   # same cost either way
        return None
    salt = u["pw_hash"].split("$", 1)[0]
    return u if hmac.compare_digest(_hash(password, salt), u["pw_hash"]) else None


def issue_token(u: dict) -> str:
    exp = int(time.time()) + TOKEN_DAYS * 86400
    return f"{u['username']}.{exp}.{_sig(u['username'], exp, u['pw_hash'])}"


def user_for_token(token: str) -> str | None:
    try:
        name, exp, sig = token.rsplit(".", 2)
        exp_i = int(exp)
    except ValueError:
        return None
    if exp_i < time.time():
        return None
    u = _get(name)
    if not u or not u["active"] or not hmac.compare_digest(sig, _sig(u["username"], exp_i, u["pw_hash"])):
        return None
    return u["username"]


def _new_pw() -> str:
    return secrets.token_urlsafe(9)


def main(argv: list[str]) -> int:
    _ensure()
    cmd, name = (argv + ["", ""])[:2]
    name = name.strip()
    if cmd == "list":
        for u in db.q("SELECT username, active, created_at FROM users ORDER BY id"):
            print(u["username"], "active" if u["active"] else "disabled",
                  time.strftime("%Y-%m-%d", time.localtime(u["created_at"])))
        return 0
    if cmd not in ("add", "reset", "disable", "enable") or not name or "." in name:
        print(__doc__)
        return 1
    u = _get(name)
    if cmd == "add":
        if u:
            print(f"{name} already exists. Use reset.")
            return 1
        pw = _new_pw()
        db.ex("INSERT INTO users (username, pw_hash, created_at) VALUES (?,?,?)", (name, _hash(pw), int(time.time())))
        print(f"username: {name}\npassword: {pw}")
        return 0
    if not u:
        print(f"No user {name}.")
        return 1
    if cmd == "reset":
        pw = _new_pw()
        db.ex("UPDATE users SET pw_hash = ?, active = 1 WHERE id = ?", (_hash(pw), u["id"]))
        print(f"username: {u['username']}\npassword: {pw}")
    else:
        db.ex("UPDATE users SET active = ? WHERE id = ?", (1 if cmd == "enable" else 0, u["id"]))
        print(f"{u['username']} {cmd}d")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
