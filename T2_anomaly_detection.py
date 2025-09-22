import argparse, csv, json, os, re, signal, sys, time, uuid
from datetime import datetime, timezone
from typing import Any, Dict, Tuple, Optional, List
import paho.mqtt.client as mqtt
import requests

# 환경 기본값 설정
DEFAULT_BASE = os.getenv("MOBIUS_BASE_URL", "http://192.168.0.58:7579/Mobius").rstrip("/")
DEFAULT_ORIGIN = os.getenv("MOBIUS_ORIGIN", "CAdmin")
DEFAULT_AE = os.getenv("MOBIUS_AE", "Meta-Sejong")
DEFAULT_ROBOT = os.getenv("MOBIUS_ROBOT_CNT", "Robot1")
DEFAULT_CTRL = os.getenv("MOBIUS_CTRL_CNT", "Ctrl")
DEFAULT_RVI = os.getenv("ONEM2M_RVI", "3")
DEFAULT_TIMEOUT = float(os.getenv("MOBIUS_TIMEOUT", "10"))

# 센서 → 기본 목표(orientation만 기본 제공; x,y는 lbl에서 읽음)
OZ_DEFAULT = 0.707
OW_DEFAULT = 0.707
SENSOR_MAP_DEFAULT: Dict[int, Dict[str, float]] = {
    1: {"oz": OZ_DEFAULT, "ow": OW_DEFAULT},
    2: {"oz": 0.000, "ow": 1.000},
    3: {"oz": -0.383, "ow": 0.924},
}

# Utils
def iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
        h["X-M2M-RVI"] = DEFAULT_RVI
    return h


def content_headers(ty: Optional[int] = None) -> Dict[str, str]:
    if ty is not None:
        return {"Content-Type": f"application/json;ty={ty}"}
    return {"Content-Type": "application/json"}


# -------------------- Mobius: CIN 생성 (로봇 명령) --------------------
def post_cin_pose(
    base: str,
    origin: str,
    ae: str,
    robot_cnt: str,
    ctrl_cnt: str,
    x: float,
    y: float,
    oz: float,
    ow: float,
    *,
    sid: Optional[str] = None,
    timeout: float = DEFAULT_TIMEOUT,
    stringify_con: bool = True
) -> Tuple[bool, str]:
    url = f"{base}/{ae}/{robot_cnt}/{ctrl_cnt}?ty=4"
    con_obj = {
        "position": {"x": float(x), "y": float(y), "z": 0.0},
        "orientation": {"z": float(oz), "w": float(ow)},
    }
    if sid:
        con_obj["sid"] = sid

    body = {"m2m:cin": {"cnf": "application/json",
                        "con": json.dumps(con_obj, ensure_ascii=False) if stringify_con else con_obj}}
    hdrs = {**base_headers(origin, include_accept=True, include_rvi=True), **content_headers(ty=4)}

    try:
        resp = requests.post(url, headers=hdrs, json=body, timeout=timeout)
    except Exception as e:
        return False, f"[ERR] HTTP request failed: {e}"

    if resp.status_code in (200, 201):
        return True, f"[OK] CIN created to {ae}/{robot_cnt}/{ctrl_cnt}\n{resp.text}"
    else:
        try:
            return False, f"[ERR] create CIN failed: {resp.status_code}\n{pretty(resp.json())}"
        except Exception:
            return False, f"[ERR] create CIN failed: {resp.status_code}\n{resp.text}"

# -------------------- 라벨 GET & 파싱 --------------------
def get_cnt_labels(base: str, origin: str, resource_path: str, *, timeout: float) -> Optional[List[str]]:
    """
    컨테이너 리소스 경로(resource_path 예: '/Meta-Sejong/Sensor1')의 lbl 배열을 GET으로 수집.
    base는 .../Mobius 까지 포함.
    """
    # base에 이미 /Mobius 포함이므로 sur 시작에 '/Mobius'가 있으면 제거
    if resource_path.startswith("/"):
        path = resource_path
    else:
        path = "/" + resource_path

    if path.startswith("/Mobius/"):
        path = path[len("/Mobius"):]  # '/Mobius'만 제거 → '/Meta-Sejong/...'

    url = f"{base.rstrip('/')}{path}"
    hdrs = base_headers(origin, include_accept=True, include_rvi=True)

    try:
        resp = requests.get(url, headers=hdrs, timeout=timeout)
    except Exception as e:
        print(f"[WARN] GET {url} failed: {e}", file=sys.stderr)
        return None

    if not resp.ok:
        print(f"[WARN] GET {url} status={resp.status_code}", file=sys.stderr)
        return None

    try:
        data = resp.json()
    except Exception:
        print(f"[WARN] non-JSON response from {url}", file=sys.stderr)
        return None

    # 일반적으로 컨테이너는 'm2m:cnt' 루트
    cnt = data.get("m2m:cnt") if isinstance(data, dict) else None
    if isinstance(cnt, dict) and isinstance(cnt.get("lbl"), list):
        return cnt["lbl"]

    # 혹시 다른 래핑 케이스 대비
    # 깊이 1단계에서 lbl 찾아보기
    for v in (data.values() if isinstance(data, dict) else []):
        if isinstance(v, dict) and isinstance(v.get("lbl"), list):
            return v["lbl"]

    print(f"[WARN] lbl not found in {url}", file=sys.stderr)
    return None

LBL_PATTERNS = {
    "adjx": re.compile(r"\badjx\b\s*[:=]?\s*(-?\d+(?:\.\d+)?)", re.I),
    "adjy": re.compile(r"\badjy\b\s*[:=]?\s*(-?\d+(?:\.\d+)?)", re.I),
    "oz":   re.compile(r"\boz\b\s*[:=]?\s*(-?\d+(?:\.\d+)?)", re.I),
    "ow":   re.compile(r"\bow\b\s*[:=]?\s*(-?\d+(?:\.\d+)?)", re.I),
    "sid":  re.compile(r"\bsid\b\s*[:=]\s*([A-Za-z0-9._\-]+)", re.I),
}

def parse_pose_from_labels(lbl: List[str]) -> Dict[str, Optional[float]]:
    """
    lbl 문자열 리스트에서 adjx/adjy/oz/ow 값을 추출.
    반환: {"x": float|None, "y": float|None, "oz": float|None, "ow": float|None}
    """
    res = {"x": None, "y": None, "oz": None, "ow": None, "sid": None}
    if not isinstance(lbl, list):
        return res
    for item in lbl:
        if not isinstance(item, str):
            continue
        for key, pat in LBL_PATTERNS.items():
            m = pat.search(item)
            if not m:
                continue
            if key in ("adjx", "adjy", "oz", "ow"):
                val = float(m.group(1))
                if key == "adjx": res["x"] = val
                elif key == "adjy": res["y"] = val
                else: res[key] = val
            elif key == "sid":
                res["sid"] = m.group(1)
    return res

def derive_sensor_cnt_paths(ae: str, sur: Optional[str], sensor_no: int) -> List[str]:
    """
    시도할 센서 컨테이너 경로 후보를 반환.
    1) 기본: /{AE}/Sensor{n}
    2) sur 기반으로 '/.../Sensor{n}'까지의 경로를 잘라 시도
    """
    paths = [f"/{ae}/Sensor{sensor_no}"]
    if sur and isinstance(sur, str):
        # '/.../Sensor{n}' 부분만 추출
        m = re.search(r"(/[^ \t\n\r]+?/Sensor%s)(?:/|$)" % re.escape(str(sensor_no)), sur)
        if m:
            paths.insert(0, m.group(1))  # sur 기반 경로를 우선 시도
    # 중복 제거
    out, seen = [], set()
    for p in paths:
        if p not in seen:
            out.append(p); seen.add(p)
    return out


def parse_notification(payload: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[str]]:
    try:
        obj = json.loads(payload)
    except Exception:
        return None, None, None

    # A) oneM2M NOTIFY wrapper (최상위 pc)
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

    # B) m2m:sgn directly
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
    
    if isinstance(obj, dict) and obj.get("op") == 1 and isinstance(obj.get("pc"), dict):
        pc = obj["pc"]
        cin = pc.get("m2m:cin") if isinstance(pc.get("m2m:cin"), dict) else None
        if cin:
            con = cin.get("con")
            if isinstance(con, str):
                try: con = json.loads(con)
                except Exception: con = {"_raw": con}
            # sur 대신 to 경로를 넘겨서 추후 센서번호 추출에 사용
            to_path = obj.get("to")
            return cin, con, to_path

    # C) direct CIN
    if isinstance(obj, dict) and "m2m:cin" in obj:
        cin = obj["m2m:cin"]
        con = cin.get("con")
        if isinstance(con, str):
            try: con = json.loads(con)
            except Exception: con = {"_raw": con}
        return cin, con, None

    # D) raw body
    if isinstance(obj, dict) and any(k in obj for k in ("temp","temperature")) and "fire_alarm" in obj and "ts" in obj:
        return None, obj, None

    return None, None, None


def extract_fields(con: Any) -> Optional[Tuple[float, int, str]]:
    if not isinstance(con, dict):
        return None

    # 키 탐색: temp/temperature, temperature[c], temp_c 등 변형까지 허용
    def find_key(d: Dict[str, Any], *cands: str) -> Optional[str]:
        for k in d.keys():
            kl = k.lower()
            for c in cands:
                cl = c.lower()
                if kl == cl or kl.startswith(cl) or cl in kl:
                    return k
        return None

    k_temp = find_key(con, "temp", "temperature")
    k_fire = find_key(con, "fire_alarm", "firealarm")
    k_ts   = find_key(con, "ts", "time", "timestamp", "datetime")
    if not k_temp:
        return None

    try:
        temp = float(con[k_temp])
    except Exception:
        return None

    try:
        fire = int(con[k_fire]) if k_fire is not None else 0
        fire = 1 if fire == 1 else 0
    except Exception:
        fire = 0

    ts = con[k_ts] if (k_ts and isinstance(con[k_ts], str) and con[k_ts]) else iso_now()
    return round(temp, 1), fire, ts


def guess_sensor_no(sur: Optional[str], con: Optional[Dict[str, Any]]) -> Optional[int]:
    # 1) sur에서 추출
    if sur and isinstance(sur, str):
        m = re.search(r'/Sensor(\d+)/', sur)
        if m:
            try: return int(m.group(1))
            except: pass

    # 2) con.sid에서 추출
    if isinstance(con, dict):
        sid = con.get("sid") or con.get("SID") or ""
        if isinstance(sid, str):
            m = re.search(r'[Ss]-?(\d+)$', sid)                # ...-S2
            n = re.search(r'Sensor\s*(\d+)', sid, re.I)        # Sensor 2
            if m:
                try: return int(m.group(1))
                except: pass
            if n:
                try: return int(n.group(1))
                except: pass

    return None


# --- 센서별 처리 핸들러(원하는 로직으로 바꿔 써) --------------------
def handle_sensor1(temp: float, fire: int, ts: str, meta: Dict[str, Any]):
    print(f"[S1] temp={temp} fire_alarm={fire} ts={ts} meta={meta}")


def handle_sensor2(temp: float, fire: int, ts: str, meta: Dict[str, Any]):
    print(f"[S2] temp={temp} fire_alarm={fire} ts={ts} meta={meta}")


def handle_sensor3(temp: float, fire: int, ts: str, meta: Dict[str, Any]):
    print(f"[S3] temp={temp} fire_alarm={fire} ts={ts} meta={meta}")


def append_csv(path: str, row: List[Any], header: List[str]):
    try:
        exists = os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(header)
            w.writerow(row)
    except Exception as e:
        print(f"[WARN] CSV write failed: {e}", file=sys.stderr)


# -------------------- 메인 --------------------
def main():
    ap = argparse.ArgumentParser()
    # MQTT
    ap.add_argument("--broker", default=os.getenv("MQTT_BROKER", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    ap.add_argument("--qos", type=int, default=int(os.getenv("MQTT_QOS", "1")))
    ap.add_argument("--cse-id", default=os.getenv("ONEM2M_CSE_ID", ""))
    ap.add_argument("--origin-mqtt", default=os.getenv("ONEM2M_ORIGIN", ""))
    ap.add_argument("--topics", default=os.getenv("MQTT_TOPICS", ""))
    ap.add_argument("--csv-dir", default=os.getenv("CSV_DIR", ""))

    # Mobius (HTTP)
    ap.add_argument("--base-url", default=DEFAULT_BASE)
    ap.add_argument("--origin",   default=DEFAULT_ORIGIN, help="X-M2M-Origin for HTTP")
    ap.add_argument("--ae",       default=DEFAULT_AE)
    ap.add_argument("--robot",    default=DEFAULT_ROBOT)
    ap.add_argument("--ctrl",     default=DEFAULT_CTRL)
    ap.add_argument("--timeout",  type=float, default=DEFAULT_TIMEOUT)
    ap.add_argument("--stringify-con", action="store_true",
                    help="Send m2m:cin.con as stringified JSON (recommended).")

    # 좌표/라벨 관련
    ap.add_argument("--sensor-map", default=os.getenv("SENSOR_MAP_FILE", ""),
                    help="JSON/CSV로 orientation 기본값 제공(oz,ow). x,y는 lbl에서 읽음.")
    ap.add_argument("--label-cache-sec", type=float, default=30.0,
                    help="라벨 재조회 주기(초). 0이면 매 이벤트마다 GET.")
    ap.add_argument("--cooldown-sec", type=float, default=10.0,
                    help="센서별 CIN 전송 쿨다운(초).")

    args = ap.parse_args()

    # MQTT 토픽 설정
    if args.topics.strip():
        topics = [t.strip() for t in args.topics.split(",") if t.strip()]
    elif args.cse_id and args.origin_mqtt:
        # 표준/역순 둘 다 구독해 브로커 구성 차이를 흡수
        topics = [
            f"/oneM2M/req/{args.cse_id}/{args.origin_mqtt}/json",
            f"/oneM2M/req/{args.origin_mqtt}/{args.cse_id}/json",
        ]
    else:
        print("[ERR] Provide --topics or (--cse-id AND --origin-mqtt).", file=sys.stderr)
        sys.exit(1)

    # orientation 기본값 로드 (x,y는 lbl에서 읽음)
    ori_map: Dict[int, Dict[str, float]] = dict(SENSOR_MAP_DEFAULT)
    if args.sensor_map:
        try:
            if args.sensor_map.lower().endswith(".json"):
                with open(args.sensor_map, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    for k, v in data.items():
                        sid = int(k)
                        oz = float(v.get("oz", ori_map.get(sid, {}).get("oz", OZ_DEFAULT)))
                        ow = float(v.get("ow", ori_map.get(sid, {}).get("ow", OW_DEFAULT)))
                        ori_map[sid] = {"oz": oz, "ow": ow}
                elif isinstance(data, list):
                    for row in data:
                        sid = int(row["sensor"])
                        oz = float(row.get("oz", ori_map.get(sid, {}).get("oz", OZ_DEFAULT)))
                        ow = float(row.get("ow", ori_map.get(sid, {}).get("ow", OW_DEFAULT)))
                        ori_map[sid] = {"oz": oz, "ow": ow}
            else:
                with open(args.sensor_map, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        sid = int(row["sensor"])
                        oz = float(row.get("oz", ori_map.get(sid, {}).get("oz", OZ_DEFAULT)))
                        ow = float(row.get("ow", ori_map.get(sid, {}).get("ow", OW_DEFAULT)))
                        ori_map[sid] = {"oz": oz, "ow": ow}
        except Exception as e:
            print(f"[WARN] sensor_map load failed: {e}", file=sys.stderr)

    # 캐시: 센서별 (last_fetch_ts, pose_from_lbl)
    label_cache: Dict[int, Tuple[float, Dict[str, Optional[float]]]] = {}
    # 전송 쿨다운
    last_sent_at: Dict[int, float] = {}

    cli = mqtt.Client(client_id=f"onem2m-subscriber-{os.getpid()}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"[MQTT] connected to {args.broker}:{args.port}")
            for t in topics:
                client.subscribe(t, qos=args.qos)
                print(f"[SUB] {t}")
        else:
            print(f"[ERR] connect rc={rc}", file=sys.stderr)

    def on_message(client, userdata, msg):
        payload = msg.payload.decode("utf-8", errors="replace")
        cin, con, sur = parse_notification(payload)
        triplet = extract_fields(con) if con is not None else None
        sensor_no = guess_sensor_no(sur, con)

        if triplet:
            temp, fire_alarm, ts = triplet
            meta = {"topic": msg.topic, "sur": sur}

            if sensor_no in (1, 2, 3):
                print(f"[S{sensor_no}] temp={temp} fire_alarm={fire_alarm} ts={ts} meta={meta}")
                if args.csv_dir:
                    append_csv(os.path.join(args.csv_dir, f"sensor{sensor_no}.csv"),
                               [ts, temp, fire_alarm], ["ts", "temp", "fire_alarm"])
            else:
                print(f"[DATA] topic={msg.topic} temp={temp} fire_alarm={fire_alarm} ts={ts} sensor=? sur={sur}")

            # -------------------- 화재 감지 시: 라벨로 좌표 읽어와 CIN 전송 --------------------
            if fire_alarm == 1 and sensor_no is not None:
                now = time.time()
                if now - last_sent_at.get(sensor_no, 0.0) < args.cooldown_sec:
                    print(f"[SKIP] sensor {sensor_no}: cooldown {args.cooldown_sec}s")
                    return

                # 1) 라벨 캐시 확인/갱신
                pose_from_lbl: Dict[str, Optional[float]]
                cached = label_cache.get(sensor_no)
                if (not cached) or (args.label_cache_sec <= 0) or (now - cached[0] >= args.label_cache_sec):
                    # 센서 CNT 경로 후보
                    paths = derive_sensor_cnt_paths(args.ae, sur, sensor_no)
                    lbl_vals: Optional[List[str]] = None
                    for p in paths:
                        lbl_vals = get_cnt_labels(args.base_url, args.origin, p, timeout=args.timeout)
                        if lbl_vals:
                            break
                    if not lbl_vals:
                        print(f"[WARN] labels not found for Sensor{sensor_no}; skip.", file=sys.stderr)
                        return
                    pose_from_lbl = parse_pose_from_labels(lbl_vals)
                    label_cache[sensor_no] = (now, pose_from_lbl)
                else:
                    pose_from_lbl = cached[1]

                # 2) 좌표/자세 결정: x,y는 lbl에서 필수; oz,ow는 lbl 있으면 사용, 없으면 ori_map/default
                x = pose_from_lbl.get("x")
                y = pose_from_lbl.get("y")
                if x is None or y is None:
                    print(f"[WARN] Sensor{sensor_no} lbl missing adjx/adjy; skip.", file=sys.stderr)
                    return

                oz = pose_from_lbl.get("oz")
                ow = pose_from_lbl.get("ow")
                if oz is None or ow is None:
                    odef = ori_map.get(sensor_no, {"oz": OZ_DEFAULT, "ow": OW_DEFAULT})
                    oz = odef["oz"]; ow = odef["ow"]

                sid_from_lbl = pose_from_lbl.get("sid")
                sid_from_con = con.get("sid") if isinstance(con, dict) else None
                sid_from_cmd = sid_from_lbl or sid_from_con or (f"S{sensor_no}" if sensor_no is not None else None)

                ok, detail = post_cin_pose(
                    base=args.base_url.rstrip("/"),
                    origin=args.origin,
                    ae=args.ae,
                    robot_cnt=args.robot,
                    ctrl_cnt=args.ctrl,
                    x=x, y=y, oz=oz, ow=ow,
                    sid=sid_from_cmd,
                    timeout=args.timeout,
                    stringify_con=True
                )
                print(detail)
                if ok:
                    last_sent_at[sensor_no] = now

        else:
            print(f"[RAW] topic={msg.topic} payload={payload}")

    cli.on_connect = on_connect
    cli.on_message = on_message

    cli.connect(args.broker, args.port, keepalive=30)
    cli.loop_start()

    def _stop(*_):
        try:
            cli.loop_stop()
            cli.disconnect()
        finally:
            sys.exit(0)

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        _stop()

if __name__ == "__main__":
    main()
