#!/usr/bin/env python3
import argparse, json, os, re, signal, sys, time, uuid, threading, glob
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import quote as urlquote

import requests
from paho.mqtt import client as mqtt

# -------------------- 환경 기본값 --------------------
DEFAULT_BASE = os.getenv("MOBIUS_BASE_URL", "http://192.168.0.58:7579/Mobius").rstrip("/")
DEFAULT_ORIGIN = os.getenv("MOBIUS_ORIGIN", "CAdmin")
DEFAULT_AE = os.getenv("MOBIUS_AE", "Meta-Sejong")
DEFAULT_ROBOT = os.getenv("MOBIUS_ROBOT_CNT", "Robot1")
DEFAULT_CTRL = os.getenv("MOBIUS_CTRL_CNT", "Ctrl")
DEFAULT_CAM1 = os.getenv("MOBIUS_CAM1_CNT", "Cam1")
DEFAULT_CAM2 = os.getenv("MOBIUS_CAM2_CNT", "Cam2")
DEFAULT_RVI = os.getenv("ONEM2M_RVI", "3")
DEFAULT_TIMEOUT = float(os.getenv("MOBIUS_TIMEOUT", "10"))

DEFAULT_MEDIA_ROOT = os.getenv("ROBOT_MEDIA_ROOT", os.path.join(os.getcwd(), "robot"))
DEFAULT_MEDIA_BASE_URL = os.getenv("ROBOT_MEDIA_BASE_URL", "http://localhost:8000/robot")
IMG_EXTS = (".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp")

# -------------------- 유틸 --------------------
def pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)

def base_headers(origin: str, *, include_accept=True, include_rvi=True) -> Dict[str, str]:
    h = {"X-M2M-Origin": origin, "X-M2M-RI": str(uuid.uuid4())}
    if include_accept:
        h["Accept"] = "application/json"
    if include_rvi:
        h["X-M2M-RVI"] = DEFAULT_RVI
    return h

def content_headers(ty: Optional[int] = None) -> Dict[str, str]:
    return {"Content-Type": f"application/json;ty={ty}" if ty is not None else "application/json"}

# -------------------- m2m:sgn 파서 --------------------
def parse_notification(payload: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
    """
    MQTT NOTIFY(op:5) -> (cin, con, sur)
    - con 이 문자열 JSON이면 디코딩
    """
    try:
        obj = json.loads(payload)
    except Exception:
        return None, None, None

    # op:5 래핑 (pc.m2m:sgn.*)
    if isinstance(obj, dict) and isinstance(obj.get("pc"), dict) and "m2m:sgn" in obj["pc"]:
        sgn = obj["pc"]["m2m:sgn"]
        sur = sgn.get("sur")
        rep = (sgn.get("nev") or {}).get("rep", {})
        cin = rep.get("m2m:cin", rep) if isinstance(rep, dict) else None
        if isinstance(cin, dict):
            con = cin.get("con")
            if isinstance(con, str):
                try:
                    con = json.loads(con)
                except Exception:
                    con = {"_raw": con}
            return cin, con, sur

    # sgn 직접
    if isinstance(obj, dict) and "m2m:sgn" in obj:
        sgn = obj["m2m:sgn"]
        sur = sgn.get("sur")
        rep = (sgn.get("nev") or {}).get("rep", {})
        cin = rep.get("m2m:cin", rep) if isinstance(rep, dict) else None
        if isinstance(cin, dict):
            con = cin.get("con")
            if isinstance(con, str):
                try:
                    con = json.loads(con)
                except Exception:
                    con = {"_raw": con}
            return cin, con, sur

    # direct CIN
    if isinstance(obj, dict) and "m2m:cin" in obj:
        cin = obj["m2m:cin"]
        con = cin.get("con")
        if isinstance(con, str):
            try:
                con = json.loads(con)
            except Exception:
                con = {"_raw": con}
        return cin, con, None

    return None, None, None

# -------------------- 파일 정렬 & URL 매핑 --------------------
TS_PATTERNS = [
    re.compile(r'(\d{8}T\d{6}Z?)'),     # 20250916T074241Z / 20250916T074241
    re.compile(r'(\d{14})'),            # 20250916074241
    re.compile(r'(\d{8})[_-](\d{6})'),  # 20250916_074241
]

def parse_ts_from_name(name: str) -> Optional[datetime]:
    s = os.path.basename(name)
    for p in TS_PATTERNS:
        m = p.search(s)
        if not m:
            continue
        if len(m.groups()) == 2:
            s14 = m.group(1) + m.group(2)
            try: return datetime.strptime(s14, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            except Exception: continue
        g = m.group(1)
        try:
            if len(g) == 15 and g.endswith("Z"):
                return datetime.strptime(g, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            if len(g) == 15:
                return datetime.strptime(g, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
            if len(g) == 14:
                return datetime.strptime(g, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def file_iso_ts(path: str) -> str:
    dt = parse_ts_from_name(path)
    if dt is None:
        dt = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def list_sorted_images(dirpath: str) -> List[str]:
    files: List[str] = []
    for ext in IMG_EXTS:
        files.extend(glob.glob(os.path.join(dirpath, f"*{ext}")))
    if not files: return []
    def key(p):
        dt = parse_ts_from_name(p)
        if dt is None:
            dt = datetime.fromtimestamp(os.path.getmtime(p), tz=timezone.utc)
        return dt.timestamp()
    files.sort(key=key)
    return files

def path_to_url(path: str, media_root: str, media_base_url: str) -> str:
    rel = os.path.relpath(path, media_root).replace(os.sep, "/")
    parts = [urlquote(p) for p in rel.split("/")]
    return media_base_url.rstrip("/") + "/" + "/".join(parts)

# -------------------- Mobius: Cam에 URL CIN 올리기 --------------------
def post_cin_url(base: str, origin: str, ae: str, robot: str, cam: str,
                 url: str, ts_iso: str, sid: str, sensor_no: int, view: str,
                 *, timeout: float = DEFAULT_TIMEOUT, stringify_con: bool = True) -> Tuple[bool, str]:
    endpoint = f"{base}/{ae}/{robot}/{cam}?ty=4"
    con_obj = {"url": url, "ts": ts_iso, "sid": sid, "sensor": sensor_no, "view": view}
    body = {"m2m:cin": {"cnf": "application/json",
                        "con": json.dumps(con_obj, ensure_ascii=False) if stringify_con else con_obj}}
    hdrs = {**base_headers(origin, include_accept=True, include_rvi=True), **content_headers(ty=4)}
    try:
        resp = requests.post(endpoint, headers=hdrs, json=body, timeout=timeout)
    except Exception as e:
        return False, f"[ERR] HTTP failed: {e}"
    if resp.status_code in (200, 201):
        return True, f"[OK] {cam} <- {os.path.basename(url)}"
    try:
        return False, f"[ERR] {cam} status={resp.status_code} {pretty(resp.json())}"
    except Exception:
        return False, f"[ERR] {cam} status={resp.status_code} {resp.text}"

# -------------------- 스트리머 --------------------
class Streamer:
    def __init__(self, *, base, origin, ae, robot, cam1, cam2,
                 media_root, media_base_url, frames, timeout):
        self.base = base; self.origin = origin; self.ae = ae; self.robot = robot
        self.cam1 = cam1; self.cam2 = cam2
        self.media_root = media_root; self.media_base_url = media_base_url
        self.frames = frames; self.timeout = timeout
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._lock = threading.Lock()

    def stop(self):
        with self._lock:
            if self._thread and self._thread.is_alive():
                self._stop_evt.set()
                self._thread.join(timeout=2.0)
            self._thread = None
            self._stop_evt = threading.Event()

    def start(self, sensor_no: int, sid: str, start_ct_iso: Optional[str]):
        self.stop()
        self._thread = threading.Thread(target=self._run, args=(sensor_no, sid, start_ct_iso), daemon=True)
        self._thread.start()

    def _run(self, sensor_no: int, sid: str, start_ct_iso: Optional[str]):
        be_dir = os.path.join(self.media_root, f"sensor{sensor_no}", "birdeye_view")
        ego_dir = os.path.join(self.media_root, f"sensor{sensor_no}", "egocentric_view")

        be_files = list_sorted_images(be_dir)
        ego_files = list_sorted_images(ego_dir)
        if not be_files or not ego_files:
            print(f"[WARN] images missing for sensor{sensor_no}: be={len(be_files)} ego={len(ego_files)}")
            return

        be_idx = 0; ego_idx = 0
        if start_ct_iso:
            try:
                ct = datetime.strptime(start_ct_iso, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
            except Exception:
                ct = datetime.now(timezone.utc)
            def find_start(files):
                for i, p in enumerate(files):
                    dt = parse_ts_from_name(p)
                    if dt and dt >= ct:
                        return i
                return 0
            be_idx = find_start(be_files); ego_idx = find_start(ego_files)

        print(f"[STREAM] sensor{sensor_no} sid={sid} start be={be_idx}/{len(be_files)} "
              f"ego={ego_idx}/{len(ego_files)} frames={self.frames}")

        for k in range(self.frames):
            if self._stop_evt.is_set():
                print("[STREAM] stopped"); return
            if be_idx >= len(be_files) or ego_idx >= len(ego_files):
                print("[STREAM] reached end of files"); return

            be_path = be_files[be_idx]; ego_path = ego_files[ego_idx]
            be_ts = file_iso_ts(be_path); ego_ts = file_iso_ts(ego_path)
            be_url = path_to_url(be_path, self.media_root, self.media_base_url)
            ego_url = path_to_url(ego_path, self.media_root, self.media_base_url)

            ok1, m1 = post_cin_url(self.base, self.origin, self.ae, self.robot, self.cam1,
                                   be_url, be_ts, sid, sensor_no, "birdeye",
                                   timeout=self.timeout, stringify_con=True)
            ok2, m2 = post_cin_url(self.base, self.origin, self.ae, self.robot, self.cam2,
                                   ego_url, ego_ts, sid, sensor_no, "egocentric",
                                   timeout=self.timeout, stringify_con=True)
            print(m1); print(m2)
            be_idx += 1; ego_idx += 1
            time.sleep(1.0)

# -------------------- 메인 --------------------
def main():
    ap = argparse.ArgumentParser(description="Stream Cam URLs to Cam1/Cam2 when Ctrl CIN arrives (NOTIFY op:5)")
    # MQTT
    ap.add_argument("--broker", default=os.getenv("MQTT_BROKER", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    ap.add_argument("--qos", type=int, default=int(os.getenv("MQTT_QOS", "1")))
    ap.add_argument("--cse-id", default=os.getenv("ONEM2M_CSE_ID", "Mobius2"))
    ap.add_argument("--origin-mqtt", default=os.getenv("ONEM2M_ORIGIN_MQTT", "SZlK9SDKWNx"))
    ap.add_argument("--topics", default=os.getenv("MQTT_TOPICS", ""))

    # Mobius HTTP
    ap.add_argument("--base-url", default=DEFAULT_BASE)
    ap.add_argument("--origin", default=DEFAULT_ORIGIN)
    ap.add_argument("--ae", default=DEFAULT_AE)
    ap.add_argument("--robot", default=DEFAULT_ROBOT)
    ap.add_argument("--ctrl", default=DEFAULT_CTRL)
    ap.add_argument("--cam1", default=DEFAULT_CAM1)
    ap.add_argument("--cam2", default=DEFAULT_CAM2)
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)

    # Media
    ap.add_argument("--media-root", default=DEFAULT_MEDIA_ROOT)
    ap.add_argument("--media-base-url", default=DEFAULT_MEDIA_BASE_URL)
    ap.add_argument("--frames", type=int, default=10)

    args = ap.parse_args()

    # 구독 토픽: 표준/역순 모두 구독해서 환경 차이 흡수
    topics: List[str]
    if args.topics.strip():
        topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    else:
        topics = [
            f"/oneM2M/req/{args.cse_id}/{args.origin_mqtt}/json",
            f"/oneM2M/req/{args.origin_mqtt}/{args.cse_id}/json",
        ]

    streamer = Streamer(base=args.base_url.rstrip("/"), origin=args.origin,
                        ae=args.ae, robot=args.robot, cam1=args.cam1, cam2=args.cam2,
                        media_root=os.path.abspath(args.media_root),
                        media_base_url=args.media_base_url,
                        frames=args.frames, timeout=args.timeout)

    # MQTT 클라이언트: v5 우선, 실패 시 v3 폴백
    use_v5 = True
    try:
        from paho.mqtt.enums import CallbackAPIVersion
        cli = mqtt.Client(client_id=f"ctrl-listener-{os.getpid()}",
                          protocol=mqtt.MQTTv5,
                          callback_api_version=CallbackAPIVersion.VERSION2)
    except Exception:
        use_v5 = False
        cli = mqtt.Client(client_id=f"ctrl-listener-{os.getpid()}",
                          protocol=mqtt.MQTTv311)

    def on_connect_v5(client, userdata, flags, reason_code, properties):
        ok = (hasattr(reason_code, "is_failure") and not reason_code.is_failure) or (getattr(reason_code, "value", 1) == 0)
        code_num = getattr(reason_code, "value", reason_code)
        if ok:
            print(f"[MQTT] connected {args.broker}:{args.port} rc={code_num}")
            for t in topics:
                client.subscribe(t, qos=args.qos)
                print(f"[SUB] {t}")
        else:
            print(f"[ERR] connect failed rc={code_num} ({reason_code})")

    def on_connect_v3(client, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT] connected {args.broker}:{args.port} rc={rc}")
            for t in topics:
                client.subscribe(t, qos=args.qos)
                print(f"[SUB] {t}")
        else:
            print(f"[ERR] connect failed rc={rc}")

    def extract_sensor_no_from_sid(sid: str) -> Optional[int]:
        if not isinstance(sid, str):
            return None
        m = re.search(r'(\d+)', sid)  # C-S3 / S3 / S-3
        return int(m.group(1)) if m else None

    def on_message(client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        cin, con, sur = parse_notification(payload)

        # NOTIFY(op:5) 에서만 처리되도록 필터링 (sgn 없으면 스킵)
        if cin is None and con is None and sur is None:
            # 디버깅 원하면 아래 주석 해제
            # print(f"[RAW] {msg.topic} {payload}")
            return

        need = f"/{args.ae}/{args.robot}/{args.ctrl}"
        if not (sur and need in f"/{sur}".replace("//", "/")):
            return

        if not (isinstance(con, dict) and "sid" in con):
            print(f"[SKIP] Ctrl without sid sur={sur} con={pretty(con)}")
            return

        sid = str(con["sid"])
        sensor_no = extract_sensor_no_from_sid(sid)
        if sensor_no is None:
            print(f"[WARN] cannot extract sensor_no from sid='{sid}'")
            return

        ct = None
        if isinstance(cin, dict) and isinstance(cin.get("ct"), str):
            ct = cin["ct"]  # YYYYMMDDTHHMMSS

        print(f"[TRIGGER] Ctrl CIN sid={sid} sensor={sensor_no} ct={ct}")
        streamer.start(sensor_no=sensor_no, sid=sid, start_ct_iso=ct)

    cli.on_message = on_message
    if use_v5:
        cli.on_connect = on_connect_v5
    else:
        cli.on_connect = on_connect_v3

    cli.connect(args.broker, args.port, keepalive=30)
    cli.loop_start()

    def _stop(*_):
        try:
            streamer.stop()
            cli.loop_stop()
            cli.disconnect()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        _stop()

if __name__ == "__main__":
    main()
