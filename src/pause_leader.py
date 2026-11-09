"""Приостанавливает лидера-координатора посреди расчёта и замеряет время смены лидера.

Скрипт находит лидера по очереди выборов в ZooKeeper (первый участник, в его имени есть pid),
ждёт, пока в чекпоинте наберётся заданное число сплитов, и замораживает процесс лидера
(все потоки, включая поток пульса сессии kazoo). Затем следит за очередью выборов и пишет,
через сколько секунд лидером стал другой координатор. По истечении паузы процесс возобновляется.

Запуск: python src/pause_leader.py --run run1_nofence --after-splits 15 --seconds 40
"""
import argparse
import sys
import time

import psutil
from kazoo.client import KazooClient

from settings import LOGS_DIR, ZK_HOSTS, ZK_ROOT, Log


def main():
    """Ждёт середины расчёта, приостанавливает лидера, замеряет смену, возобновляет."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run", required=True)
    ap.add_argument("--after-splits", type=int, default=25, help="приостановить после N сплитов")
    ap.add_argument("--seconds", type=float, default=40, help="длительность паузы, с")
    ap.add_argument("--hosts", default=ZK_HOSTS)
    args = ap.parse_args()
    log = Log(LOGS_DIR / args.run / "pause_leader.log", "pause")
    base = f"{ZK_ROOT}/{args.run}"

    zk = KazooClient(hosts=args.hosts)
    zk.start(timeout=15)
    election = zk.Election(f"{base}/election")
    log(f"жду двух координаторов в очереди {base}/election")
    while len(election.contenders()) < 2:
        time.sleep(0.05)
    queue = election.contenders()
    leader = queue[0]
    pid = int(leader.split("pid=")[1])
    log(f"очередь выборов: {queue}; лидер: {leader}")

    log(f"жду, пока лидер посчитает {args.after_splits} сплитов")
    while not zk.exists(f"{base}/done") or len(zk.get_children(f"{base}/done")) < args.after_splits:
        time.sleep(0.05)
    proc = psutil.Process(pid)
    proc.suspend()
    t0 = time.time()
    done = len(zk.get_children(f"{base}/done"))
    log(f"ПРИОСТАНОВИЛ лидера {leader}: в чекпоинте {done} сплитов; статус процесса {proc.status()}")

    new_leader = None
    while time.time() - t0 < args.seconds:
        queue = election.contenders()
        if new_leader is None and queue and queue[0] != leader:
            new_leader = queue[0]
            log(f"лидером стал {new_leader} через {time.time() - t0:.2f} с после приостановки")
        time.sleep(0.05)
    if new_leader is None:
        log("за время паузы лидер не сменился")

    proc.resume()
    log(f"ВОЗОБНОВИЛ {leader} через {time.time() - t0:.1f} с; очередь выборов: {election.contenders()}")
    zk.stop()
    zk.close()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
