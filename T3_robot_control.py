import argparse, json, os, re, signal, sys, time, uuid, threading, glob
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple
from urllib.parse import quote as urlquote
from paho.mqtt import client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
import requests

# 환경 기본값
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


# Utils
def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)


def base_headers(origin: str, *, include_accept=True, include_rvi=True) -> Dict[str, str]:
    h = {
        "X-M2M-Origin": origin,
        "X-M2M-RI": str(uuid.uuid4()),
    }
    if include_accept:
        h["Accept"] = "application/json"
    if include_rvi:
        h["X-M2M-RVI"] = DEFAULT_RVI
    return h


def content_headers(ty: Optional[int] = None) -> Dict[str, str]:
    if ty is not None:
        return {"Content-Type": f"application/json;ty={ty}"}
    return {"Content-Type": "application/json"}

def parse_notification(payload: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
    """
    반환: (cin_dict, con_obj, sur_path)
    """
    try:
        obj = json.loads(payload)
    except Exception:
        return None, None, None

    # A) 최상위 pc 래핑
    if isinstance(obj, dict) and isinstance(obj.get("pc"), dict) and "m2m:sgn" in obj["pc"]:
        sgn = obj["pc"]["m2m:sgn"]
        sur = sgn.get("sur")
        rep = (sgn.get("nev") or {}).get("rep", {})
        cin = rep.get("m2m:cin", rep) if isinstance(rep, dict) else None
        if isinstance(cin, dict):
            con = cin.get("con")
            if isinstance(con, str):
                try: con = json.loads(con)
                except Exception: con = {"_raw": con}
            return cin, con, sur

    # B) sgn directly
    if isinstance(obj, dict) and "m2m:sgn" in obj:
        sgn = obj["m2m:sgn"]
        sur = sgn.get("sur")
        rep = (sgn.get("nev") or {}).get("rep", {})
        cin = rep.get("m2m:cin", rep) if isinstance(rep, dict) else None
        if isinstance(cin, dict):
            con = cin.get("con")
            if isinstance(con, str):
                try: con = json.loads(con)
                except Exception: con = {"_raw": con}
            return cin, con, sur

    # C) 직접 CIN
    if isinstance(obj, dict) and "m2m:cin" in obj:
        cin = obj["m2m:cin"]
        con = cin.get("con")
        if isinstance(con, str):
            try: con = json.loads(con)
            except Exception: con = {"_raw": con}
        return cin, con, None

    return None, None, None

# -------------------- 이미지 파일 정렬/URL 매핑 --------------------
TS_PATTERNS = [
    re.compile(r'(\d{8}T\d{6}Z?)'),          # 20250916T074241Z
    re.compile(r'(\d{14})'),                  # 20250916074241
    re.compile(r'(\d{8})[_-](\d{6})'),        # 20250916_074241
]

def parse_ts_from_name(name: str) -> Optional[datetime]:
    s = os.path.basename(name)
    for p in TS_PATTERNS:
        m = p.search(s)
        if not m: 
            continue
        if len(m.groups()) == 2:  # 8 + 6
            s14 = m.group(1) + m.group(2)
            try: return datetime.strptime(s14, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            except Exception: 
                continue
        else:
            g = m.group(1)
            if len(g) == 15 and g.endswith("Z"):
                try: return datetime.strptime(g, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                except Exception: 
                    continue
            if len(g) == 15:
                try: return datetime.strptime(g, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
                except Exception:
                    continue
            if len(g) == 14:
                try: return datetime.strptime(g, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                except Exception: 
                    continue
    return None

def file_iso_ts(path: str) -> str:
    dt = parse_ts_from_name(path)
    if dt is None:
        # mtime fallback
        dt = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

def list_sorted_images(dirpath: str) -> List[str]:
    files = []
    for ext in IMG_EXTS:
        files.extend(glob.glob(os.path.join(dirpath, f"*{ext}")))
    if not files:
        return []
    # 정렬 키: 파일명에서 ts 추출 → 없으면 mtime
    def key(p):
        dt = parse_ts_from_name(p)
        if dt is None:
            dt = datetime.fromtimestamp(os.path.getmtime(p), tz=timezone.utc)
        return dt.timestamp()
    files.sort(key=key)
    return files

def path_to_url(path: str, media_root: str, media_base_url: str) -> str:
    rel = os.path.relpath(path, media_root)
    rel = rel.replace(os.sep, "/")
    # 안전하게 URL 인코딩(경로 조각 단위)
    parts = [urlquote(p) for p in rel.split("/")]
    return media_base_url.rstrip("/") + "/" + "/".join(parts)

# -------------------- Mobius: Cam에 URL CIN 올리기 --------------------
def post_cin_url(base: str, origin: str, ae: str, robot: str, cam: str,
                 url: str, ts_iso: str, sid: str, sensor_no: int, view: str,
                 *, timeout: float = DEFAULT_TIMEOUT, stringify_con: bool = True) -> Tuple[bool, str]:
    endpoint = f"{base}/{ae}/{robot}/{cam}?ty=4"
    con_obj = {
        "url": url,
        "ts": ts_iso,
        "sid": sid,
        "sensor": sensor_no,
        "view": view,     # "birdeye" or "egocentric"
    }
    body = {
        "m2m:cin": {
            "cnf": "application/json",
            "con": json.dumps(con_obj, ensure_ascii=False) if stringify_con else con_obj
        }
    }
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

# -------------------- 스트리밍 스레드 --------------------
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
        # 새 스트림 시작 전에 기존 중단
        self.stop()
        args = (sensor_no, sid, start_ct_iso)
        self._thread = threading.Thread(target=self._run, args=args, daemon=True)
        self._thread.start()

    def _run(self, sensor_no: int, sid: str, start_ct_iso: Optional[str]):
        be_dir = os.path.join(self.media_root, f"sensor{sensor_no}", "birdeye_view")
        ego_dir = os.path.join(self.media_root, f"sensor{sensor_no}", "egocentric_view")

        be_files = list_sorted_images(be_dir)
        ego_files = list_sorted_images(ego_dir)
        if not be_files or not ego_files:
            print(f"[WARN] images missing for sensor{sensor_no}: "
                  f"birdeye={len(be_files)} egocentric={len(ego_files)}")
            return

        # 시작 인덱스: Ctrl CIN ct 기준으로 가장 가까운/이후 프레임부터
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
            be_idx = find_start(be_files)
            ego_idx = find_start(ego_files)

        print(f"[STREAM] sensor{sensor_no} sid={sid} start be={be_idx}/{len(be_files)} "
              f"ego={ego_idx}/{len(ego_files)} frames={self.frames}")

        for k in range(self.frames):
            if self._stop_evt.is_set():
                print("[STREAM] stopped")
                return
            if be_idx >= len(be_files) or ego_idx >= len(ego_files):
                print("[STREAM] reached end of files")
                return

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

# -------------------- 메인: MQTT 구독 및 트리거 --------------------
def main():
    ap = argparse.ArgumentParser(description="Stream camera URLs to Cam1/Cam2 when Ctrl CIN arrives")
    # MQTT
    ap.add_argument("--broker", default=os.getenv("MQTT_BROKER", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    ap.add_argument("--qos", type=int, default=int(os.getenv("MQTT_QOS", "1")))
    ap.add_argument("--cse-id", default=os.getenv("ONEM2M_CSE_ID", "Mobius2"))
    ap.add_argument("--origin-mqtt", default=os.getenv("ONEM2M_ORIGIN_MQTT", "S0T9DaJqg9u"))
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
    ap.add_argument("--frames", type=int, default=10, help="frames to send per trigger")

    args = ap.parse_args()

    # MQTT 토픽
    if args.topics.strip():
        topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    else:
        topics = [f"/oneM2M/req/{args.cse_id}/{args.origin_mqtt}/json"]

    # 스트리머
    streamer = Streamer(base=args.base_url.rstrip("/"), origin=args.origin,
                        ae=args.ae, robot=args.robot, cam1=args.cam1, cam2=args.cam2,
                        media_root=os.path.abspath(args.media_root),
                        media_base_url=args.media_base_url,
                        frames=args.frames, timeout=args.timeout)

    # MQTT v5로 생성(Deprecation 경고 회피)
    cli = mqtt.Client(
                      client_id=f"ctrl-listener-{os.getpid()}",
                      protocol=mqtt.MQTTv5,
                      callback_api_version=CallbackAPIVersion.VERSION2)

    def on_connect(client, userdata, flags, reason_code, properties):
        # v3(정수) 또는 v5(ReasonCode 객체) 모두 대응
        ok = (isinstance(reason_code, int) and reason_code == 0) or \
            (hasattr(reason_code, "is_failure") and not reason_code.is_failure)

        code_num = getattr(reason_code, "value", reason_code)  # v5면 정수 코드, v3면 그대로
        if ok:
            print(f"[MQTT] connected {args.broker}:{args.port} rc={code_num}")
            for t in topics:
                client.subscribe(t, qos=args.qos)
                print(f"[SUB] {t}")
        else:
            print(f"[ERR] connect failed rc={code_num} ({reason_code})")

    def extract_sensor_no_from_sid(sid: str) -> Optional[int]:
        if not isinstance(sid, str):
            return None
        m = re.search(r'(\d+)', sid)  # C-S3, S-3, S3 등
        try:
            return int(m.group(1)) if m else None
        except Exception:
            return None

    def on_message(client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        cin, con, sur = parse_notification(payload)

        # Ctrl 경로만 관심
        need = f"/{args.ae}/{args.robot}/{args.ctrl}"
        if not (sur and need in f"/{sur}".replace("//", "/")):
            # 디버깅: 다른 sur도 보고 싶다면 주석 해제
            # print(f"[SKIP] sur={sur}")
            return

        if not (isinstance(con, dict) and ("sid" in con)):
            print(f"[SKIP] Ctrl without sid: sur={sur}")
            return

        sid = str(con["sid"])
        sensor_no = extract_sensor_no_from_sid(sid)
        if sensor_no is None:
            print(f"[WARN] cannot extract sensor_no from sid='{sid}'")
            return

        # Ctrl CIN 생성 시간(ct)로 시작 인덱스 결정
        ct = None
        if isinstance(cin, dict) and isinstance(cin.get("ct"), str):
            # Mobius ct: YYYYMMDDTHHMMSS
            ct = cin["ct"]

        print(f"[TRIGGER] Ctrl CIN sid={sid} sensor={sensor_no} ct={ct}")
        streamer.start(sensor_no=sensor_no, sid=sid, start_ct_iso=ct)

    cli.on_connect = on_connect
    cli.on_message = on_message

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