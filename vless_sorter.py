import asyncio
import urllib.parse
import urllib.request
import re
import base64
import time
import os
import ssl
import sys
from concurrent.futures import ThreadPoolExecutor

# ================= НАСТРОЙКИ ФАЙЛОВ И ПАРАМЕТРОВ =================

SOURCES_FILE = "sources.txt"  # Файл, откуда будут считываться ссылки на источники
CONCURRENT_LIMIT = 500         # Максимальное количество одновременных сетевых проверок
TIMEOUT = 3.5                  # Таймаут соединения в секундах
OUTPUT_DIR = "sorted_configs"
LOOP_INTERVAL = 3600           # Интервал обновления при локальном запуске (3600 сек = 1 час)

# Дефолтные источники на случай, если файл источников отсутствует
DEFAULT_SOURCES = [
    "https://raw.githubusercontent.com/freefq/free/master/v2ray",
    "https://raw.githubusercontent.com/WilliamStar007/Clash-Vmess-Sub/main/v2ray.txt",
    "https://raw.githubusercontent.com/roosterkid/openvpn/main/vless"
]

# ======================================================================

class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_status(message, color=Colors.END):
    print(f"{color}{message}{Colors.END}")

class VlessConfig:
    def __init__(self, raw_url):
        self.raw_url = raw_url.strip()
        self.valid = False
        self.host = None
        self.port = None
        self.uuid = None
        self.sni = None
        self.security = None
        self.remark = ""
        self.ping = float('inf')
        self.global_rank = None  # Позиция в глобальном топе после сортировки
        self._parse()

    def _parse(self):
        try:
            if not self.raw_url.startswith("vless://"):
                return
            
            parsed = urllib.parse.urlparse(self.raw_url)
            netloc = parsed.netloc
            if "@" in netloc:
                self.uuid, endpoint = netloc.split("@", 1)
            else:
                return

            if ":" in endpoint:
                self.host, port_str = endpoint.split(":", 1)
                if "/" in port_str:
                    port_str = port_str.split("/", 1)[0]
                self.port = int(port_str)
            else:
                self.host = endpoint
                self.port = 443

            query_params = urllib.parse.parse_qs(parsed.query)
            self.security = query_params.get("security", [None])[0]
            self.sni = query_params.get("sni", [None])[0]
            
            if parsed.fragment:
                self.remark = urllib.parse.unquote(parsed.fragment)
            
            if self.host and self.port:
                self.valid = True
        except Exception:
            self.valid = False

def extract_emojis(text):
    """
    Извлекает из текста только флаги стран (двухбуквенные региональные символы)
    и другие стандартные эмодзи.
    """
    if not text:
        return ""
    emoji_pattern = re.compile(
        r'[\U0001F1E6-\U0001F1FF]{2}|'  # Флаги стран (напр. 🇨🇦)
        r'[\U0001F600-\U0001F64F]|'      # Смайлы
        r'[\U0001F300-\U0001F5FF]|'      # Символы и иконки
        r'[\U0001F680-\U0001F6FF]|'      # Транспорт/карты
        r'[\u2600-\u27BF]|'              # Разные символы
        r'[\U0001F900-\U0001F9FF]'       # Дополнительные эмодзи
    )
    emojis = emoji_pattern.findall(text)
    return "".join(emojis).strip() if emojis else ""

def load_sources_from_file():
    """Загружает список URL-адресов источников из текстового файла"""
    sources = []
    
    if not os.path.exists(SOURCES_FILE):
        print_status(f"Файл '{SOURCES_FILE}' не найден. Создаю новый с дефолтными источниками...", Colors.YELLOW)
        try:
            with open(SOURCES_FILE, "w", encoding="utf-8") as f:
                f.write("# Поместите сюда ссылки на источники VLESS конфигураций (каждая с новой строки).\n")
                f.write("# Строки с '#' в начале и пустые строки игнорируются.\n\n")
                for src in DEFAULT_SOURCES:
                    f.write(f"{src}\n")
            return DEFAULT_SOURCES
        except Exception as e:
            print_status(f"Не удалось создать файл {SOURCES_FILE}: {e}", Colors.RED)
            return DEFAULT_SOURCES

    try:
        with open(SOURCES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                sources.append(line)
    except Exception as e:
        print_status(f"Ошибка при чтении файла {SOURCES_FILE}: {e}", Colors.RED)
        return DEFAULT_SOURCES

    return sources

def fetch_url(url):
    """Синхронное скачивание контента с обработкой SSL и User-Agent"""
    try:
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        ctx = ssl._create_unverified_context()
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            return response.read()
    except Exception:
        return None

def decode_base64_safely(data_bytes):
    """Безопасное декодирование Base64 подписок с автозаполнением паддинга"""
    try:
        cleaned = data_bytes.decode('utf-8', errors='ignore').strip()
        cleaned = re.sub(r'\s+', '', cleaned)
        missing_padding = len(cleaned) % 4
        if missing_padding:
            cleaned += '=' * (4 - missing_padding)
        return base64.b64decode(cleaned).decode('utf-8', errors='ignore')
    except Exception:
        return ""

async def collect_configs_async(sources):
    """Асинхронный запуск загрузки из всех источников"""
    loop = asyncio.get_running_loop()
    print_status(f"Начало сбора конфигов из {len(sources)} источников...", Colors.BLUE)
    
    raw_results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [loop.run_in_executor(executor, fetch_url, url) for url in sources]
        results = await asyncio.gather(*futures)
        for res in results:
            if res:
                raw_results.append(res)
                
    extracted_links = set()
    
    for content_bytes in raw_results:
        text_content = content_bytes.decode('utf-8', errors='ignore')
        
        if "vless://" not in text_content and len(text_content) > 20:
            decoded = decode_base64_safely(content_bytes)
            if "vless://" in decoded:
                text_content = decoded
                
        urls = re.findall(r'vless://[^\s#"\']+', text_content)
        for u in urls:
            u_cleaned = u.split('\\')[0].split('"')[0].split("'")[0]
            extracted_links.add(u_cleaned)
            
    print_status(f"Успешно собрано уникальных VLESS ссылок: {len(extracted_links)}", Colors.GREEN)
    return list(extracted_links)

async def ping_vless(config: VlessConfig, semaphore: asyncio.Semaphore):
    """Асинхронное тестирование пинга методом TLS Handshake"""
    async with semaphore:
        start_time = time.perf_counter()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(config.host, config.port),
                timeout=TIMEOUT
            )
            
            if config.security in ['tls', 'reality'] or config.port in [443, 8443]:
                try:
                    ssl_context = ssl._create_unverified_context()
                    server_hostname = config.sni if config.sni else config.host
                    
                    transport = writer.transport
                    sock = transport.get_extra_info('socket')
                    
                    if sock:
                        loop = asyncio.get_running_loop()
                        await asyncio.wait_for(
                            loop.run_in_executor(
                                None, 
                                lambda: ssl_context.wrap_socket(sock, server_hostname=server_hostname, do_handshake_on_connect=True)
                            ),
                            timeout=TIMEOUT
                        )
                except Exception:
                    config.ping = int((time.perf_counter() - start_time) * 1000) + 150
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                    return

            elapsed = time.perf_counter() - start_time
            config.ping = int(elapsed * 1000)
            
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        except (asyncio.TimeoutError, OSError):
            config.ping = float('inf')

async def run_once():
    """Один полный цикл сбора, проверки, переименования и сохранения"""
    sources = load_sources_from_file()
    if not sources:
        print_status(f"Критическая ошибка: Файл '{SOURCES_FILE}' пуст!", Colors.RED)
        return False

    collected_urls = await collect_configs_async(sources)
    if not collected_urls:
        print_status("Не удалось собрать ссылки ни из одного источника!", Colors.RED)
        return False

    configs = []
    for url in collected_urls:
        cfg = VlessConfig(url)
        if cfg.valid:
            configs.append(cfg)

    print_status(f"Прошли первичную валидацию структуры: {len(configs)} конфигов", Colors.BLUE)
    if not configs:
        print_status("Нет валидных конфигураций для тестирования.", Colors.RED)
        return False

    print_status(f"Запуск тестирования в {CONCURRENT_LIMIT} потоков...", Colors.BLUE)
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)
    
    start_time = time.time()
    tasks = [ping_vless(cfg, semaphore) for cfg in configs]
    
    total_tasks = len(tasks)
    pending = tasks
    
    while pending:
        done, pending = await asyncio.wait(pending, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
        completed = total_tasks - len(pending)
        percent = (completed / total_tasks) * 100
        print(f"\rПрогресс проверки: {completed}/{total_tasks} ({percent:.1f}%) ...", end="", flush=True)

    print("\n")
    duration = time.time() - start_time
    print_status(f"Проверка {total_tasks} серверов завершена за {duration:.2f} сек.", Colors.GREEN)

    working_configs = [c for c in configs if c.ping != float('inf')]
    working_configs.sort(key=lambda x: x.ping)

    for rank, c in enumerate(working_configs, start=1):
        c.global_rank = rank

    print_status(f"Найдено живых серверов: {len(working_configs)} из {len(configs)}", Colors.GREEN)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    elite_configs = [c for c in working_configs if c.ping < 150]
    good_configs = [c for c in working_configs if 150 <= c.ping < 300]
    slow_configs = [c for c in working_configs if c.ping >= 300]

    files_to_write = {
        "elite.txt": elite_configs,
        "good.txt": good_configs,
        "slow.txt": slow_configs,
        "all_working_sorted.txt": working_configs
    }

    for filename, cfgs in files_to_write.items():
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as out_f:
            for c in cfgs:
                original_emojis = extract_emojis(c.remark)
                emoji_part = f"{original_emojis} " if original_emojis else ""
                new_remark_text = f"{emoji_part}Топ {c.global_rank} [{c.ping}ms]"
                encoded_remark = urllib.parse.quote(new_remark_text)
                
                base_url, _, _ = c.raw_url.partition("#")
                out_f.write(f"{base_url}#{encoded_remark}\n")

    print_status("\n=== ОТЧЕТ О РАБОТЕ ===", Colors.BOLD)
    print_status(f"📁 Результаты сохранены в папку: '{OUTPUT_DIR}/'", Colors.BLUE)
    print_status(f"🔥 Elite (пинг < 150ms): {len(elite_configs)} шт. -> {OUTPUT_DIR}/elite.txt", Colors.GREEN)
    print_status(f"⚡ Good (пинг 150-300ms): {len(good_configs)} шт. -> {OUTPUT_DIR}/good.txt", Colors.YELLOW)
    print_status(f"🐢 Slow (пинг > 300ms): {len(slow_configs)} шт. -> {OUTPUT_DIR}/slow.txt", Colors.RED)
    print_status(f"📝 Всего рабочих (отсортировано): {len(working_configs)} шт. -> {OUTPUT_DIR}/all_working_sorted.txt", Colors.BOLD)
    return True

async def main():
    if "--once" in sys.argv:
        print_status("Режим разового запуска (--once). Запускаю проверку...", Colors.BLUE)
        await run_once()
        return

    print_status("Скрипт запущен в режиме бесконечного цикла обновления.", Colors.BOLD)
    print_status(f"Конфиги будут автоматически обновляться каждые {LOOP_INTERVAL // 60} минут.", Colors.YELLOW)
    print_status("Для запуска в режиме разовой проверки используйте: python vless_sorter.py --once\n", Colors.BLUE)

    while True:
        current_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
        print_status(f"\n[{current_time_str}] Запуск планового обновления конфигов...", Colors.BOLD)
        
        try:
            await run_once()
        except Exception as e:
            print_status(f"Произошла непредвиденная ошибка во время цикла обновления: {e}", Colors.RED)

        print_status(f"\nСледующее обновление через {LOOP_INTERVAL // 60} минут. Ожидание...", Colors.BLUE)
        await asyncio.sleep(LOOP_INTERVAL)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
