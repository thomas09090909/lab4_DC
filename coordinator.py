import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
from typing import Dict, Any, List, Optional, Tuple

lock = threading.Lock()
NODE_ID: str = ""
PORT: int = 8000
PARTICIPANTS: List[str] = []
TIMEOUT_S: float = 2.0
TX: Dict[str, Dict[str, Any]] = {}
COORD_LOG = "coordinator.log"

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

def log_decision(txid, decision):
    with open(COORD_LOG, "a") as f:
        f.write(f"{txid} {decision}\n")
        os.fsync(f.fileno())

def post_json(url: str, payload: dict, timeout: float = TIMEOUT_S) -> Tuple[int, dict]:
    data = jdump(payload)
    req = request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        return resp.status, jload(resp.read())

def two_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {"txid": txid, "protocol": "2PC", "state": "PREPARE_SENT", "op": op, "votes": {}}
    
    votes = {}
    all_yes = True
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/prepare", {"txid": txid, "op": op})
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            if vote != "YES": all_yes = False
        except Exception:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    print(f"[{NODE_ID}] TX {txid} waiting before decision (simulate crash window)")
    time.sleep(10)

    decision = "COMMIT" if all_yes else "ABORT"
    log_decision(txid, decision)
    
    with lock:
        TX[txid].update({"votes": votes, "decision": decision, "state": f"{decision}_SENT"})

    endpoint = "/commit" if decision == "COMMIT" else "/abort"
    for p in PARTICIPANTS:
        try: post_json(p.rstrip("/") + endpoint, {"txid": txid})
        except Exception: pass

    with lock: TX[txid]["state"] = "DONE"
    return {"ok": True, "txid": txid, "decision": decision, "votes": votes}

def three_pc(txid: str, op: dict) -> dict:
    with lock:
        TX[txid] = {"txid": txid, "protocol": "3PC", "state": "CAN_COMMIT_SENT", "op": op, "votes": {}}

    votes = {}
    all_yes = True
    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p.rstrip("/") + "/can_commit", {"txid": txid, "op": op})
            vote = str(resp.get("vote", "NO")).upper()
            votes[p] = vote
            if vote != "YES": all_yes = False
        except Exception:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    if not all_yes:
        log_decision(txid, "ABORT")
        for p in PARTICIPANTS:
            try: post_json(p.rstrip("/") + "/abort", {"txid": txid})
            except Exception: pass
        return {"ok": True, "decision": "ABORT", "votes": votes}

    with lock: TX[txid]["state"] = "PRECOMMIT_SENT"
    for p in PARTICIPANTS:
        try: post_json(p.rstrip("/") + "/precommit", {"txid": txid})
        except Exception: pass

    log_decision(txid, "COMMIT")
    for p in PARTICIPANTS:
        try: post_json(p.rstrip("/") + "/commit", {"txid": txid})
        except Exception: pass

    return {"ok": True, "decision": "COMMIT", "votes": votes}

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            self._send(200, {"ok": True, "tx": TX})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = jload(self.rfile.read(length))
        if self.path == "/tx/start":
            txid, op = body.get("txid"), body.get("op")
            proto = body.get("protocol", "2PC").upper()
            res = two_pc(txid, op) if proto == "2PC" else three_pc(txid, op)
            self._send(200, res)

def main():
    global NODE_ID, PORT, PARTICIPANTS
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="COORD")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True)
    args = ap.parse_args()
    NODE_ID, PORT = args.id, args.port
    PARTICIPANTS = [p.strip() for p in args.participants.split(",")]
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

if __name__ == "__main__":
    main()
