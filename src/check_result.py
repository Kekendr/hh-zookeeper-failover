"""Итог запуска: указатель результата, записи в хранилище, сверка с ДЗ-3 и время смены лидера.

Скрипт читает узел-указатель /hh/<run>/result (данные и версию), эпоху, отметки чекпоинта,
все результаты, записанные в хранилище, и сравнивает контрольные числа с итогом MapReduce
из ДЗ-3 (reference/dz3_mapreduce_summary.json). Время смены лидера считается по журналам:
от строки «ПРИОСТАНОВИЛ» в pause_leader.log до строки «стал лидером» запасного координатора.

Запуск: python src/check_result.py --run run1_nofence
"""
import argparse
import json
import re
import sys
from collections import Counter
from datetime import datetime

from kazoo.client import KazooClient

from settings import BUCKET, LOGS_DIR, REFERENCE_FILE, ZK_HOSTS, ZK_PREFIX, ZK_ROOT, Output, s3_client

FIELDS = ("variant_vacancies", "vacancies_with_skills", "distinct", "total_occurrences", "sha256")


def log_time(path, pattern):
    """Возвращает время первой строки журнала, где есть pattern."""
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        if pattern in line:
            return datetime.strptime(line[:12], "%H:%M:%S.%f"), line
    return None


def main():
    """Печатает итог запуска и сохраняет его в outputs/check_<run>.txt."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run", required=True)
    args = ap.parse_args()
    out = Output(f"check_{args.run}")
    base = f"{ZK_ROOT}/{args.run}"
    ref = json.loads(REFERENCE_FILE.read_text(encoding="utf-8"))

    zk = KazooClient(hosts=ZK_HOSTS)
    zk.start(timeout=15)
    data, stat = zk.get(f"{base}/result")
    _, epoch_stat = zk.get(f"{base}/epoch")
    marks = [json.loads(zk.get(f"{base}/done/{c}")[0]) for c in sorted(zk.get_children(f"{base}/done"))]
    zk.stop()
    zk.close()
    pointer = json.loads(data) if data else {}

    out(f"Запуск {args.run}")
    out(f"Указатель {base}/result: версия {stat.version} (сколько раз записан), данные:")
    out(f"  {json.dumps(pointer, ensure_ascii=False)}")
    out(f"Эпоха {base}/epoch: {epoch_stat.version} (сколько раз сменялся лидер)")
    by = Counter(f"{m['by']} (эпоха {m['epoch']})" for m in marks)
    out(f"Чекпоинт: {len(marks)} сплитов, кто посчитал: {dict(by)}")

    s3 = s3_client()
    prefix = f"{ZK_PREFIX}/{args.run}/result/"
    keys = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
                  if o["Key"].endswith("summary.json"))
    out(f"\nРезультаты в хранилище s3://{BUCKET}/{prefix}: {len(keys)}")
    for key in keys:
        summ = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        same = all(summ[f] == ref[f] for f in FIELDS)
        active = pointer.get("prefix", "") + "/summary.json" == key
        note = "действующий, на него ссылается указатель" if active else "указатель на него не ссылается"
        out(f"  {key.removeprefix(prefix)}  ({note})")
        out(f"    вакансий {summ['variant_vacancies']:,}, с навыками {summ['vacancies_with_skills']:,}; "
            f"уникальных k1/k2/k3: {summ['distinct']['k1']:,}/{summ['distinct']['k2']:,}/"
            f"{summ['distinct']['k3']:,}")
        out(f"    sha256 {summ['sha256']}")
        out(f"    совпадает с ДЗ-3 (MapReduce, sha256 {ref['sha256'][:16]}...): {'ДА' if same else 'НЕТ'}")

    run_logs = LOGS_DIR / args.run
    paused = log_time(run_logs / "pause_leader.log", "ПРИОСТАНОВИЛ")
    if paused:
        leader = re.search(r"лидера (\S+)", paused[1]).group(1)
        out("\nСмена лидера по журналам:")
        out(f"  {paused[1]}")
        for path in sorted(run_logs.glob("coord-*.log")):
            if path.stem == leader:
                continue
            elected = log_time(path, "стал лидером")
            if elected:
                out(f"  {elected[1]}")
                out(f"  время от приостановки лидера до нового лидера: "
                    f"{(elected[0] - paused[0]).total_seconds():.3f} с")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
