"""Общие настройки: адреса ансамбля ZooKeeper и RustFS, пути к журналам, журнал с метками времени."""
import os
import sys
from datetime import datetime
from pathlib import Path

# Ансамбль ZooKeeper из трёх серверов (conf/zk1..zk3/zoo.cfg)
ZK_SERVERS = {1: 2181, 2: 2182, 3: 2183}
ZK_ADMIN_PORTS = {1: 8081, 2: 8082, 3: 8083}
ZK_HOSTS = os.environ.get("ZK_HOSTS", ",".join(f"127.0.0.1:{p}" for p in ZK_SERVERS.values()))
SESSION_TIMEOUT = 10.0         # секунд, по условию задания
ZK_ROOT = "/hh"                # корень узлов координатора

# Объектное хранилище RustFS из ДЗ-2 и ДЗ-3
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "rustfsadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "rustfsadmin")
BUCKET = "hh-lake"
RAW_KEY = "raw/it_vacancies_2024.csv"
ZK_PREFIX = "zk"               # чекпоинты и результаты этого задания в бакете

# Вариант 2: «Программист, разработчик на Python», условие то же, что в ДЗ-3
VARIANT_NAME = "Программист, разработчик на Python"
VARIANT_ROLE = "Программист, разработчик"
VARIANT_KEYWORD = "python"

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / "logs"
OUTPUTS_DIR = REPO_ROOT / "outputs"
REFERENCE_FILE = REPO_ROOT / "reference" / "dz3_mapreduce_summary.json"


def now():
    """Текущее время с миллисекундами для строк журнала."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


class Log:
    """Журнал: каждая строка с временем и именем идёт в консоль и в файл."""

    def __init__(self, path, name=""):
        path.parent.mkdir(parents=True, exist_ok=True)
        sys.stdout.reconfigure(encoding="utf-8")
        self.file = open(path, "w", encoding="utf-8")
        self.prefix = f"[{name}] " if name else ""

    def __call__(self, msg=""):
        line = f"{now()} {self.prefix}{msg}"
        print(line, flush=True)
        self.file.write(line + "\n")
        self.file.flush()


class Output:
    """Вывод команды без меток времени: в консоль и в outputs/<name>.txt."""

    def __init__(self, name):
        OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
        sys.stdout.reconfigure(encoding="utf-8")
        self.file = open(OUTPUTS_DIR / f"{name}.txt", "w", encoding="utf-8")

    def __call__(self, msg=""):
        print(msg, flush=True)
        self.file.write(msg + "\n")
        self.file.flush()


def s3_client():
    """Создаёт boto3-клиент к RustFS с path-style адресацией."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY, region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, signature_version="s3v4",
                      retries={"max_attempts": 3, "mode": "standard"}),
    )
