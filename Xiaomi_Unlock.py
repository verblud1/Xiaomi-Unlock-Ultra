"""
Xiaomi Unlock Ultra v6.2
Установка зависимостей: pip install -r requirements.txt
Требуется Python 3.10+
"""
import hashlib
import os
import time
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

if sys.version_info < (3, 10):
    print("[ERROR] Требуется Python 3.10 или выше.")
    sys.exit(1)

try:
    import ntplib
    import pytz
    import requests
    import urllib3
    from colorama import init, Fore, Style
    from requests.adapters import HTTPAdapter
except ImportError as e:
    print(f"[ERROR] Отсутствует зависимость: {e}")
    print("Установите: pip install -r requirements.txt")
    sys.exit(1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
init(autoreset=True)

col_g  = Fore.GREEN
col_yb = Style.BRIGHT + Fore.YELLOW
col_r  = Fore.RED
col_b  = Fore.CYAN
col_w  = Fore.WHITE

# --- КОНФИГУРАЦИЯ ---
TARGET_HOST       = "sgp-api.buy.mi.com"
TARGET_PATH       = "/bbs/api/global/apply/bl-auth"
STATUS_PATH       = "/bbs/api/global/user/bl-switch/state"
NTP_SERVERS       = [
    "ntp2.vniiftri.ru", "0.ru.pool.ntp.org", "1.ru.pool.ntp.org",
    "2.ru.pool.ntp.org", "3.ru.pool.ntp.org", "time.cloudflare.com",
    "pool.ntp.org",
]
NTP_SAMPLES       = 7     # сколько NTP-ответов собирать для trimmed mean
NTP_TRIM          = 2     # сколько крайних значений отбрасывать с каждого конца
GOLDEN_OFFSETS_MS = [10, 40, 70, 100]
LAG_COMPENSATION  = 0.7  # коэффициент одностороннего RTT

print_lock = threading.Lock()


# --- Статистика ---
@dataclass
class Stats:
    lock:              threading.Lock = field(default_factory=threading.Lock)
    success:           int = 0
    fail:              int = 0
    timeout:           int = 0
    bad_json:          int = 0
    response_times_ms: list[float] = field(default_factory=list)

    def record(self, kind: str, elapsed_ms: float | None = None) -> None:
        with self.lock:
            match kind:
                case "success": self.success  += 1
                case "fail":    self.fail     += 1
                case "timeout": self.timeout  += 1
                case "badjson": self.bad_json += 1
            if elapsed_ms is not None:
                self.response_times_ms.append(elapsed_ms)

    def print_summary(self) -> None:
        avg = (sum(self.response_times_ms) / len(self.response_times_ms)
               if self.response_times_ms else 0)
        print(f"\n{col_yb}{'─' * 52}")
        print(f"{col_yb}  ИТОГИ")
        print(f"{col_yb}{'─' * 52}")
        print(f"  {col_g}Успешно:     {self.success}")
        print(f"  {col_r}Ошибок:      {self.fail}")
        print(f"  {col_r}Таймаутов:   {self.timeout}")
        print(f"  {col_r}Плохой JSON: {self.bad_json}")
        if self.response_times_ms:
            print(f"  {col_w}Среднее RTT: {avg:.1f}ms")
        print(f"{col_yb}{'─' * 52}\n")


stats = Stats()


def log(thread_id: int | str, color: str, msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    with print_lock:
        print(f"{col_b}[{str(thread_id):>9}] {color}{msg} | {ts}")


# --- Создать сессию с явным пулом ---
def make_session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1)
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    s.headers.update({"Connection": "keep-alive"})
    return s


# --- NTP: trimmed mean из N запросов к разным серверам ---
def get_accurate_beijing_time(
    n_samples: int = NTP_SAMPLES,
    trim:      int = NTP_TRIM,
) -> tuple[datetime, float, float] | None:
    """
    Собирает n_samples NTP tx_time со случайной ротацией серверов.
    Отбрасывает trim крайних значений с каждой стороны (trimmed mean).
    Возвращает (beijing_time, mean_offset, mono_anchor).
    """
    client  = ntplib.NTPClient()
    samples = []   # (tx_time, offset)

    servers = list(NTP_SERVERS)
    for server in servers:
        if len(samples) >= n_samples:
            break
        try:
            resp = client.request(server, timeout=2)
            samples.append((resp.tx_time, resp.offset))
        except Exception:
            continue

    if len(samples) < (trim * 2 + 1):
        # Недостаточно ответов — берём что есть без обрезки
        if not samples:
            return None
        trim = 0

    # Сортируем по tx_time, обрезаем выбросы
    samples.sort(key=lambda x: x[0])
    trimmed = samples[trim: len(samples) - trim] if trim else samples

    mean_tx_time = sum(s[0] for s in trimmed) / len(trimmed)
    mean_offset  = sum(s[1] for s in trimmed) / len(trimmed)

    mono_anchor = time.perf_counter()
    beijing = datetime.fromtimestamp(mean_tx_time, timezone.utc).astimezone(
        pytz.timezone("Asia/Shanghai")
    )
    return beijing, mean_offset, mono_anchor


def current_beijing(beijing_anchor: datetime, mono_anchor: float) -> datetime:
    return beijing_anchor + timedelta(seconds=time.perf_counter() - mono_anchor)


# --- Гибридное ожидание ---
def hybrid_wait(target_pc: float) -> None:
    while True:
        diff = target_pc - time.perf_counter()
        if diff <= 0:
            break
        if diff > 0.002:
            time.sleep(diff - 0.001)


# --- Замер латентности (медиана 5 GET-пингов) ---
def measure_latency(session: requests.Session, ip: str, token: str) -> float:
    headers = {
        "Cookie":     f"new_bbs_serviceToken={token};",
        "Host":       TARGET_HOST,
        "User-Agent": "okhttp/4.12.0",
        "Connection": "keep-alive",
    }
    url     = f"https://{ip}{STATUS_PATH}"
    samples = []
    for _ in range(5):
        try:
            t0 = time.perf_counter()
            session.get(url, headers=headers, verify=False, timeout=3)
            samples.append((time.perf_counter() - t0) * 1000)
        except Exception:
            pass

    if not samples:
        log("LAG", col_r, "Все пинги упали — используется дефолт 150 мс")
        return 150.0

    samples.sort()
    mid = len(samples) // 2
    return (samples[mid] + samples[~mid]) / 2


# --- Прогрев: TCP + TLS + Keep-alive ---
def warm_up_session(session: requests.Session, ip: str, token: str) -> bool:
    headers = {
        "Cookie":     f"new_bbs_serviceToken={token};",
        "Host":       TARGET_HOST,
        "User-Agent": "okhttp/4.12.0",
        "Connection": "keep-alive",
    }
    url = f"https://{ip}{STATUS_PATH}"
    try:
        session.get(url, headers=headers, verify=False, timeout=3)  # TCP + TLS
        session.get(url, headers=headers, verify=False, timeout=3)  # Keep-alive
        return True
    except Exception:
        return False


# --- Повторный прогрев TLS за ~2с до выстрела ---
def refresh_tls(session: requests.Session, ip: str, token: str) -> None:
    """
    Один GET за ~2с до полуночи — гарантирует что TLS-контекст живой.
    Сервер не может успеть закрыть idle-соединение между этим и выстрелом.
    """
    headers = {
        "Cookie":     f"new_bbs_serviceToken={token};",
        "Host":       TARGET_HOST,
        "User-Agent": "okhttp/4.12.0",
        "Connection": "keep-alive",
    }
    try:
        session.get(
            f"https://{ip}{STATUS_PATH}",
            headers=headers,
            verify=False,
            timeout=2,
        )
    except Exception:
        pass


# --- Финальный выстрел ---
def sync_shot(
    index:     int,
    target_pc: float,
    session:   requests.Session,
    prepared:  requests.PreparedRequest,
    barrier:   threading.Barrier,
    ip:        str,
    token:     str,
    refresh_pc: float,   # момент повторного прогрева TLS
) -> None:
    """
    1. Барьер — синхронный старт всех потоков.
    2. Ждём момента refresh_pc → делаем один GET (обновляем TLS).
    3. Гибридное ожидание до target_pc.
    4. session.send() с готовым PreparedRequest.
    """
    try:
        barrier.wait(timeout=10)
    except threading.BrokenBarrierError:
        log(f"Thread-{index:02d}", col_r, "Barrier broken — выход")
        stats.record("fail")
        return

    # Ждём момента TLS-обновления
    hybrid_wait(refresh_pc)
    refresh_tls(session, ip, token)

    # Финальное ожидание
    hybrid_wait(target_pc)
    t_send = time.perf_counter()

    try:
        resp = session.send(prepared, verify=False, timeout=5)
        elapsed_ms = (time.perf_counter() - t_send) * 1000

        ct = resp.headers.get("Content-Type", "")
        if "application/json" not in ct:
            log(f"Thread-{index:02d}", col_r,
                f"NON-JSON ({resp.status_code}): {ct[:40]}")
            stats.record("badjson")
            return

        try:
            data = resp.json()
        except ValueError:
            log(f"Thread-{index:02d}", col_r, "INVALID JSON")
            stats.record("badjson")
            return

        code       = data.get("code", -1)
        data_field = data.get("data") or {}
        res        = data_field.get("apply_result", "N/A")
        color      = col_g if code == 0 else col_r

        log(f"Thread-{index:02d}", color,
            f"CODE: {code} | RES: {res} | RTT: {elapsed_ms:.1f}ms")

        stats.record("success" if code == 0 else "fail", elapsed_ms)

    except requests.exceptions.Timeout:
        log(f"Thread-{index:02d}", col_r, "TIMEOUT (5s)")
        stats.record("timeout")
    except requests.exceptions.RequestException as e:
        log(f"Thread-{index:02d}", col_r, f"REQUEST ERROR: {e}")
        stats.record("fail")
    except Exception as e:
        log(f"Thread-{index:02d}", col_r, f"FAILED: {e}")
        stats.record("fail")


# --- Главная функция ---
def main() -> None:
    os.system("cls" if os.name == "nt" else "clear")
    print(f"{col_yb}╔════════════════════════════════════════════════════╗")
    print(f"{col_yb}║   XIAOMI UNLOCK ULTRA v6.2 | TRIMMED NTP + TLS     ║")
    print(f"{col_yb}╚════════════════════════════════════════════════════╝\n")

    sessions: list[requests.Session] = []

    try:
        print(f"{col_g}[✔]    Python {sys.version.split()[0]}")

        # 1. DNS Pinning
        try:
            resolved_ip = socket.gethostbyname(TARGET_HOST)
            print(f"{col_g}[DNS]   {col_w}{TARGET_HOST} -> {col_g}{resolved_ip}")
        except Exception:
            print(f"{col_r}[DNS] Ошибка резолва!")
            return

        # 2. Загрузка токенов
        token_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "token.txt"
        )
        if not os.path.exists(token_path):
            print(f"{col_r}Файл token.txt не найден!")
            return
        with open(token_path) as f:
            tokens = [line.strip() for line in f if line.strip()]
        if not tokens:
            print(f"{col_r}token.txt пуст!")
            return

        used_count = min(len(tokens), len(GOLDEN_OFFSETS_MS))
        tokens     = tokens[:used_count]
        print(f"{col_g}[✔]    Токенов к использованию: {used_count}")

        # 3. NTP: trimmed mean из NTP_SAMPLES запросов
        print(
            f"{col_b}[NTP]  {col_w}Калибровка "
            f"({NTP_SAMPLES} запросов, trim={NTP_TRIM})...",
            end=" ", flush=True,
        )
        ntp_result = get_accurate_beijing_time()
        if not ntp_result:
            print(f"{col_r}Ошибка!")
            return
        beijing_anchor, ntp_offset, mono_anchor = ntp_result
        print(
            f"{col_g}{beijing_anchor.strftime('%H:%M:%S.%f')[:-3]}"
            f"  offset={ntp_offset * 1000:+.2f}ms"
        )

        # 4. Цель — ближайшая полночь
        target_beijing = (
            beijing_anchor.replace(hour=0, minute=0, second=0, microsecond=0)
            + timedelta(days=1)
        )

        # 5. Параллельный прогрев сессий
        print(f"{col_b}[WARM] {col_w}Параллельный прогрев {used_count} сессий...")
        raw_sessions = [make_session() for _ in tokens]

        def _warm(args):
            i, token = args
            ok = warm_up_session(raw_sessions[i], resolved_ip, token)
            label = f"{col_g}OK" if ok else f"{col_r}FAIL"
            with print_lock:
                print(f"  Сессия {i + 1}: {label}")
            return ok

        with ThreadPoolExecutor(max_workers=used_count) as pool:
            results = list(pool.map(_warm, enumerate(tokens)))

        valid = [
            (s, t) for s, t, ok in zip(raw_sessions, tokens, results) if ok
        ]
        for s, t, ok in zip(raw_sessions, tokens, results):
            if not ok:
                s.close()

        if not valid:
            print(f"{col_r}Нет валидных сессий — выход.")
            return

        sessions, valid_tokens = map(list, zip(*valid))
        used_count   = min(len(sessions), len(GOLDEN_OFFSETS_MS))
        sessions     = sessions[:used_count]
        valid_tokens = valid_tokens[:used_count]
        print(f"{col_g}[✔]    Валидных сессий: {used_count}")

        # 6. Замер латентности
        print(
            f"{col_b}[LAG]  {col_w}Замер латентности (5 пингов)...",
            end=" ", flush=True,
        )
        actual_lag = measure_latency(sessions[0], resolved_ip, valid_tokens[0])
        print(f"{col_yb}{actual_lag:.1f}ms")

        # 7. Адаптивное ожидание с повторной NTP за ~30с
        resync_done    = False
        warmup_trigger = target_beijing - timedelta(seconds=35)
        launch_trigger = target_beijing - timedelta(seconds=5)

        print(f"{col_b}[WAIT] {col_w}Ожидание полуночи...\n")

        while True:
            now_b = current_beijing(beijing_anchor, mono_anchor)
            rem   = (target_beijing - now_b).total_seconds()

            if now_b >= launch_trigger:
                break

            if not resync_done and now_b >= warmup_trigger:
                print(
                    f"\n{col_b}[NTP]  {col_w}Повторная калибровка...",
                    end=" ", flush=True,
                )
                ntp2 = get_accurate_beijing_time()
                if ntp2:
                    beijing_anchor, ntp_offset, mono_anchor = ntp2
                    resync_done = True
                    print(f"{col_g}OK  offset={ntp_offset * 1000:+.2f}ms")
                else:
                    print(f"{col_r}Ошибка — повторим в следующей итерации")

            if rem <= 0:
                break

            print(
                f"\r  {col_w}Осталось: {col_yb}{rem:7.2f}s"
                f"  {col_w}| Пекин: {now_b.strftime('%H:%M:%S')}",
                end="", flush=True,
            )

            if rem > 60:
                time.sleep(0.5)
            elif rem > 10:
                time.sleep(0.1)
            else:
                time.sleep(0.02)

        # 8. Подготовка аргументов потоков
        shot_url    = f"https://{resolved_ip}{TARGET_PATH}"
        thread_args = []

        for i, token in enumerate(valid_tokens):
            dev_id  = hashlib.sha1(token.encode()).hexdigest().upper()
            headers = {
                "Cookie": (
                    f"new_bbs_serviceToken={token};"
                    "versionCode=500411;versionName=5.4.11;"
                    f"deviceId={dev_id};"
                ),
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent":   "okhttp/4.12.0",
                "Host":         TARGET_HOST,
                "Connection":   "keep-alive",
            }
            req      = requests.Request(
                "POST", shot_url, headers=headers, json={"is_retry": True}
            )
            prepared = sessions[i].prepare_request(req)

            shift_ms  = GOLDEN_OFFSETS_MS[i] + actual_lag * LAG_COMPENSATION
            target_dt = target_beijing - timedelta(milliseconds=shift_ms)
            target_pc = mono_anchor + (target_dt - beijing_anchor).total_seconds()

            # Момент TLS-обновления: за 2с до выстрела каждого потока
            refresh_pc = target_pc - 2.0

            thread_args.append({
                "index":      i + 1,
                "target_pc":  target_pc,
                "session":    sessions[i],
                "prepared":   prepared,
                "ip":         resolved_ip,
                "token":      token,
                "refresh_pc": refresh_pc,
            })

        # 9. Барьер + запуск потоков заранее
        barrier = threading.Barrier(used_count)
        threads = [
            threading.Thread(
                target=sync_shot,
                kwargs={**args, "barrier": barrier},
                daemon=True,
            )
            for args in thread_args
        ]
        for t in threads:
            t.start()

        print(f"\n{col_yb}» {len(threads)} потоков на барьере. Ждём полночь...\n")

        for t in threads:
            t.join(timeout=15)
            if t.is_alive():
                with print_lock:
                    print(f"{col_r}[WARN] Поток не завершился за 15с")

    finally:
        for s in sessions:
            try:
                s.close()
            except Exception:
                pass

    stats.print_summary()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{col_r}Прервано пользователем.")
        stats.print_summary()
    if sys.stdout.isatty():
        input("\nНажмите Enter для выхода.")