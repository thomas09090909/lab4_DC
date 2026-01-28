#!/usr/bin/env python3
"""
Lab 4 Starter — Participant (2PC/3PC) (HTTP, standard library only)
===================================================================

2PC endpoints:
- POST /prepare   {"txid":"TX1","op":{...}} -> {"vote":"YES"/"NO"}
- POST /commit    {"txid":"TX1"}
- POST /abort     {"txid":"TX1"}

3PC endpoints (bonus scaffold):
- POST /can_commit {"txid":"TX1","op":{...}} -> {"vote":"YES"/"NO"}
- POST /precommit  {"txid":"TX1"}

GET:
- /status
"""
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import threading
import time
from typing import Dict, Any, Optional

lock = threading.Lock()
NODE_ID, PORT = "", 8001
kv: Dict[str, str] = {}
TX: Dict[str, Dict[str, Any]] = {}
WAL_PATH: Optional[str] = None

def jdump(obj: Any) -> bytes: return json.dumps(obj).encode("utf-8")
def jload(b: bytes) -> Any: return json.loads(b.decode("utf-8"))

def wal_append(line: str) -> None:
    if not WAL_PATH: return
    with open(WAL_PATH, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())

def replay_wal():
    if not WAL_PATH or not os.path.exists(WAL_PATH): return
    with open(WAL_PATH, "r") as f:
        for line in f:
            parts = line.strip().split(" ", 2)
            if len(parts) < 2: continue
            txid, event = parts[0], parts[1]
            if event in ("PREPARE", "CAN_COMMIT"):
                vote_data = parts[2].split(" ", 1)
                vote, op = vote_data[0], json.loads(vote_data[1])
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op}
            elif event == "PRECOMMIT":
                if txid in TX: TX[txid]["state"] = "PRECOMMIT"
            elif event == "COMMIT":
                if txid in TX and TX[txid]["state"] in ("READY", "PRECOMMIT"):
                    apply_op(TX[txid]["op"])
                    TX[txid]["state"] = "COMMITTED"
            elif event == "ABORT":
                if txid in TX: TX[txid]["state"] = "ABORTED"

def validate_op(op: dict) -> bool:
    return str(op.get("type", "")).upper() == "SET"

def apply_op(op: dict) -> None:
    if op and op.get("type") == "SET":
        kv[str(op["key"])] = str(op.get("value", ""))

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = jload(self.rfile.read(length))
        txid = body.get("txid")
        
        if self.path == "/prepare" or self.path == "/can_commit":
            op = body.get("op")
            vote = "YES" if validate_op(op) else "NO"
            with lock:
                TX[txid] = {"state": "READY" if vote == "YES" else "ABORTED", "op": op}
            wal_append(f"{txid} {self.path[1:].upper()} {vote} {json.dumps(op)}")
            self._send(200, {"vote": vote, "state": TX[txid]["state"]})
            
        elif self.path == "/precommit":
            with lock:
                if txid in TX: TX[txid]["state"] = "PRECOMMIT"
            wal_append(f"{txid} PRECOMMIT")
            self._send(200, {"ok": True})

        elif self.path == "/commit":
            with lock:
                if txid in TX and TX[txid]["state"] in ("READY", "PRECOMMIT"):
                    apply_op(TX[txid]["op"])
                    TX[txid]["state"] = "COMMITTED"
            wal_append(f"{txid} COMMIT")
            self._send(200, {"ok": True})

        elif self.path == "/abort":
            with lock:
                if txid in TX: TX[txid]["state"] = "ABORTED"
            wal_append(f"{txid} ABORT")
            self._send(200, {"ok": True})

def main():
    global NODE_ID, PORT, WAL_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument("--wal", default="")
    args = ap.parse_args()
    NODE_ID, PORT, WAL_PATH = args.id, args.port, args.wal
    replay_wal()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
