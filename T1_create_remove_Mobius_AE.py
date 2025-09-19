import argparse
import json
import os
import sys
import uuid
from typing import Any, Dict, Optional, List

import requests

DEFAULT_BASE = os.getenv("MOBIUS_BASE_URL", "http://192.168.0.58:7579/Mobius").rstrip("/")
DEFAULT_ORIGIN = os.getenv("MOBIUS_ORIGIN", "CAdmin")
RVI = os.getenv("ONEM2M_RVI", "3")
DEFAULT_TIMEOUT = float(os.getenv("MOBIUS_TIMEOUT", "10"))


def pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)


def base_headers(origin: str, *, include_accept: bool = True, include_rvi: bool = True) -> Dict[str, str]:
    h: Dict[str, str] = {
        "X-M2M-Origin": origin,
        "X-M2M-RI": str(uuid.uuid4()),
    }
    if include_accept:
        h["Accept"] = "application/json"
    if include_rvi:
        h["X-M2M-RVI"] = RVI
    return h

def content_headers(ty: Optional[int] = None) -> Dict[str, str]:
    if ty is not None:
        return {"Content-Type": f"application/json;ty={ty}"}
    return {"Content-Type": "application/json"}


# AE 생성
def post_ae(base: str, origin: str, rn: str, api: str, rr: bool, poa: Optional[List[str]] = None, timeout: float = DEFAULT_TIMEOUT) -> None:
    url = f"{base}?ty=2"
    body = {
        "m2m:ae": {
            "rn": rn,
            "api": api,
            "rr": bool(rr),
            "poa": poa or [],
        }
    }
    hdr = {**base_headers(origin, include_accept=True, include_rvi=True), **content_headers(ty=2)}
    resp = requests.post(url, headers=hdr, json=body, timeout=timeout)

    if resp.status_code in (200, 201):
        print(f"[OK] AE created: rn={rn}")
        try:
            print(pretty(resp.json()))
        except Exception:
            print(resp.text)
    elif resp.status_code == 409:
        print(f"[WARN] AE already exists: rn={rn}")
        try:
            print(pretty(resp.json()))
        except Exception:
            print(resp.text)
        sys.exit(0)
    else:
        print(f"[ERR] create AE failed: {resp.status_code}")
        try:
            print(pretty(resp.json()))
        except Exception:
            print(resp.text)
        sys.exit(1)


# AE 조회
def get_ae(base: str, origin: str, rn: str, timeout: float = DEFAULT_TIMEOUT) -> None:
    url = f"{base}/{rn}"
    hdr = base_headers(origin, include_accept=True, include_rvi=True)
    resp = requests.get(url, headers=hdr, timeout=timeout)

    if resp.ok:
        print(f"[OK] AE fetched: rn={rn}")
        try:
            print(pretty(resp.json()))
        except Exception:
            print(resp.text)
    else:
        print(f"[ERR] get AE failed: {resp.status_code}")
        try:
            print(pretty(resp.json()))
        except Exception:
            print(resp.text)
        sys.exit(1)


# AE 삭제
def delete_ae(base: str, origin: str, rn: str, timeout: float = DEFAULT_TIMEOUT) -> None:
    url = f"{base}/{rn}"
    delete_hdr = base_headers(origin, include_accept=True, include_rvi=False)
    resp = requests.delete(url, headers=delete_hdr, timeout=timeout)

    if resp.status_code in (200, 202, 204):
        print(f"[OK] AE deleted: rn={rn}")
        if resp.content:
            try:
                print(pretty(resp.json()))
            except Exception:
                print(resp.text)
    else:
        print(f"[ERR] delete AE failed: {resp.status_code}")
        try:
            print(pretty(resp.json()))
        except Exception:
            print(resp.text)
        sys.exit(1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Mobius(oneM2M) AE create/get/delete")
    ap.add_argument("--base-url", default=DEFAULT_BASE, help=f"Mobius base URL (default: {DEFAULT_BASE})")
    ap.add_argument("--origin", default=DEFAULT_ORIGIN, help=f"X-M2M-Origin (default: {DEFAULT_ORIGIN})")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help=f"HTTP timeout seconds (default: {DEFAULT_TIMEOUT})")

    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_create = sub.add_parser("create", help="Create AE")
    ap_create.add_argument("--rn", required=True, help="AE resourceName (rn)")
    ap_create.add_argument("--api", default="app.fire.detection", help="AE app-ID (api)")
    ap_create.add_argument("--rr", default="true", choices=["true", "false"], help="requestReachability (rr)")
    ap_create.add_argument("--poa", default="", help='comma-separated POA list (e.g. "http://host:8080/notify")')

    ap_get = sub.add_parser("get", help="Get AE")
    ap_get.add_argument("--rn", required=True)

    ap_del = sub.add_parser("delete", help="Delete AE")
    ap_del.add_argument("--rn", required=True)

    args = ap.parse_args()

    base = args.base_url.rstrip("/")
    origin = args.origin
    timeout = args.timeout

    if args.cmd == "create":
        rn = args.rn
        api = args.api
        rr = args.rr.lower() == "true"
        poa = [p.strip() for p in args.poa.split(",") if p.strip()] if args.poa else []
        post_ae(base, origin, rn, api, rr, poa, timeout)

    elif args.cmd == "get":
        get_ae(base, origin, args.rn, timeout)

    elif args.cmd == "delete":
        delete_ae(base, origin, args.rn, timeout)

if __name__ == "__main__":
    main()
