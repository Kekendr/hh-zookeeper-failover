"""Отказоустойчивый координатор расчёта навыков, пар и троек навыков из ДЗ-3 на ZooKeeper.

Несколько копий координатора участвуют в выборах лидера (рецепт Election библиотеки kazoo,
эфемерные последовательные узлы). Считает только лидер: он идёт по сплитам CSV из бакета
hh-lake и после каждого сплита сохраняет чекпоинт: частичные счётчики в хранилище и узел
done/<номер> в ZooKeeper. Запасной координатор, став лидером, берёт готовые сплиты из
чекпоинта и досчитывает остальные. Результат пишется в хранилище, а узел-указатель result
в ZooKeeper показывает, чей результат действующий.

Каждый новый лидер увеличивает эпоху (версию узла epoch). При FENCING=1 указатель обновляется
транзакцией «проверить, что эпоха не изменилась, и записать», поэтому запись лидера,
которого уже сменили, отклоняется с BadVersionError.

Запуск: python src/coordinator.py --run run1_nofence --name coord-A
С защитой по эпохе: $env:FENCING='1'; python src/coordinator.py --run run2_fencing --name coord-A
"""
import argparse
import json
import logging
import os
import sys
import time
from collections import Counter

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import BadVersionError, NodeExistsError

import hh_calc
from settings import (BUCKET, LOGS_DIR, SESSION_TIMEOUT, VARIANT_NAME, ZK_HOSTS, ZK_PREFIX,
                      ZK_ROOT, Log, now, s3_client)


class Paths:
    """Пути узлов ZooKeeper и ключей хранилища для одного запуска."""

    def __init__(self, run):
        self.base = f"{ZK_ROOT}/{run}"
        self.election = f"{self.base}/election"
        self.epoch = f"{self.base}/epoch"
        self.done = f"{self.base}/done"
        self.result = f"{self.base}/result"
        self.s3 = f"{ZK_PREFIX}/{run}"

    def ckpt_key(self, i):
        """Ключ частичного результата сплита в хранилище."""
        return f"{self.s3}/checkpoint/split-{i:03d}.json.gz"


class Coordinator:
    """Одна копия координатора: ждёт лидерства, считает с чекпоинта, публикует результат."""

    def __init__(self, args):
        self.args = args
        self.name = args.name
        self.fencing = os.environ.get("FENCING", "0") == "1"
        self.paths = Paths(args.run)
        self.log = Log(LOGS_DIR / args.run / f"{args.name}.log", args.name)
        self.s3 = s3_client()
        self.zk = KazooClient(hosts=args.hosts, timeout=args.timeout)
        self.zk.add_listener(self.on_state)
        self.epoch = None
        self.stopping = False          # штатное закрытие сессии не пишется как её потеря

    def on_state(self, state):
        """Пишет в журнал смену состояния сессии ZooKeeper."""
        if self.stopping:
            return
        text = {KazooState.SUSPENDED: "связь с ZooKeeper прервана (SUSPENDED)",
                KazooState.LOST: "сессия ZooKeeper истекла (LOST), эфемерные узлы удалены",
                KazooState.CONNECTED: "подключён к ZooKeeper (CONNECTED)"}[state]
        self.log(text)

    def start(self):
        """Подключается к ансамблю, создаёт узлы запуска и встаёт в очередь на лидерство."""
        log, zk, p = self.log, self.zk, self.paths
        log(f"старт: pid={os.getpid()}, запуск {self.args.run}, вариант 2: {VARIANT_NAME}")
        log(f"защита по эпохе FENCING={'1 (включена)' if self.fencing else '0 (выключена)'}; "
            f"пауза после сплита {self.args.split_delay} с")
        zk.start(timeout=15)
        log(f"ансамбль {self.args.hosts}; сессия 0x{zk.client_id[0]:x}, тайм-аут "
            f"{zk._session_timeout / 1000:g} с")
        for path in (p.done, p.epoch, p.result):
            zk.ensure_path(path)
        election = zk.Election(p.election, f"{self.name} pid={os.getpid()}")
        others = election.contenders()
        log(f"встаю в очередь на лидерство {p.election}; сейчас в очереди: {others or 'никого'}")
        election.run(self.lead)
        log("работа завершена, закрываю сессию и выхожу")
        self.stopping = True
        zk.stop()
        zk.close()

    def lead(self):
        """Работа лидера: новая эпоха, расчёт с чекпоинта, запись результата."""
        log, zk, p = self.log, self.zk, self.paths
        log("стал лидером")
        data, _ = zk.get(p.result)
        if data:
            log(f"результат уже опубликован: {data.decode()}; расчёт не нужен")
            return
        self.epoch = zk.set(p.epoch, self.name.encode()).version
        log(f"эпоха лидера: {self.epoch} (версия узла {p.epoch})")

        size, cols, splits = hh_calc.plan_splits(self.s3, self.args.chunk_mb)
        done = set(zk.get_children(p.done))
        log(f"сплитов: {len(splits)} по {self.args.chunk_mb:g} МиБ; в чекпоинте готово: "
            f"{len(done)}" + (f", продолжаю с чекпоинта" if done else ", начинаю с нуля"))

        totals = [Counter() for _ in hh_calc.K_SIZES]
        stats = Counter()
        from_ckpt = computed = 0
        t0 = time.perf_counter()
        for i, start, end in splits:
            done = set(zk.retry(zk.get_children, p.done))
            if f"{i:03d}" in done:                 # сплит уже посчитан, берём из чекпоинта
                blob = self.s3.get_object(Bucket=BUCKET, Key=p.ckpt_key(i))["Body"].read()
                counters, st = hh_calc.unpack(blob)
                from_ckpt += 1
            else:
                counters, st = hh_calc.count_split(self.s3, i, start, end, size, cols)
                self.save_checkpoint(i, counters, st)
                computed += 1
                log(f"сплит {i + 1:>2}/{len(splits)} посчитан: вакансий варианта "
                    f"{st.get('variant_vacancies', 0):>3}, пар {len(counters[1]):>5,}; чекпоинт сохранён")
                time.sleep(self.args.split_delay)
            for total, c in zip(totals, counters):
                total.update(c)
            stats.update({k: v for k, v in st.items() if k != "seconds"})
        log(f"все сплиты готовы за {time.perf_counter() - t0:.1f} с: посчитал сам {computed}, "
            f"взял из чекпоинта {from_ckpt}")
        self.publish(totals, stats, len(splits))

    def save_checkpoint(self, i, counters, st):
        """Сохраняет частичный результат сплита в хранилище и отмечает сплит в ZooKeeper."""
        p = self.paths
        self.s3.put_object(Bucket=BUCKET, Key=p.ckpt_key(i), Body=hh_calc.pack(counters, st))
        try:
            self.zk.retry(self.zk.create, f"{p.done}/{i:03d}",
                          json.dumps({"by": self.name, "epoch": self.epoch}).encode())
        except NodeExistsError:            # сплит уже отметил другой координатор
            pass

    def publish(self, totals, stats, n_splits):
        """Пишет результат в хранилище и обновляет указатель result в ZooKeeper."""
        log, zk, p = self.log, self.zk, self.paths
        digest = hh_calc.canonical_digest(totals)
        log(f"вакансий варианта: {stats['variant_vacancies']:,}; из них с навыками: "
            f"{stats['with_skills']:,}; строк данных: {stats['lines']:,}")
        for k, c in zip(hh_calc.K_SIZES, totals):
            top = ", ".join(f"{' + '.join(key)} ({n})" for key, n in hh_calc.to_rows(c)[:3])
            log(f"k={k}: уникальных {len(c):,}, сумма частот {sum(c.values()):,}; топ-3: {top}")
        log(f"контрольный sha256: {digest}")

        prefix = f"{p.s3}/result/{self.name}-epoch{self.epoch}"
        names = {1: "skills", 2: "pairs", 3: "triples"}
        for k, c in zip(hh_calc.K_SIZES, totals):
            self.s3.put_object(Bucket=BUCKET, Key=f"{prefix}/k{k}_{names[k]}.csv",
                               Body=hh_calc.to_csv(k, c), ContentType="text/csv")
        summary = {"writer": self.name, "epoch": self.epoch, "splits": n_splits,
                   "data_lines": stats["lines"], "variant_vacancies": stats["variant_vacancies"],
                   "vacancies_with_skills": stats["with_skills"],
                   "distinct": {f"k{k}": len(c) for k, c in zip(hh_calc.K_SIZES, totals)},
                   "total_occurrences": {f"k{k}": sum(c.values())
                                         for k, c in zip(hh_calc.K_SIZES, totals)},
                   "sha256": digest}
        self.s3.put_object(Bucket=BUCKET, Key=f"{prefix}/summary.json",
                           Body=json.dumps(summary, ensure_ascii=False, indent=2).encode())
        log(f"файлы результата записаны в s3://{BUCKET}/{prefix}/")

        pointer = json.dumps({"writer": self.name, "epoch": self.epoch, "prefix": prefix,
                              "sha256": digest, "time": now()}).encode()
        zk.retry(zk.exists, p.result)      # дождаться связи после возможной паузы процесса
        log(f"публикую результат в указатель {p.result} "
            + ("с проверкой эпохи" if self.fencing else "без проверки эпохи"))
        if self.fencing:
            def commit():
                """Транзакция: эпоха не изменилась, тогда обновить указатель."""
                tx = zk.transaction()
                tx.check(p.epoch, self.epoch)
                tx.set_data(p.result, pointer)
                return tx.commit()
            res = zk.retry(commit)
            if isinstance(res[0], Exception):
                current = zk.exists(p.epoch).version
                log(f"{type(res[0]).__name__}: запись результата отклонена, моя эпоха {self.epoch}, "
                    f"текущая эпоха {current}; указатель не изменён")
                raise SystemExit(3)
            version = zk.exists(p.result).version
        else:
            version = zk.retry(zk.set, p.result, pointer).version
        log(f"записал результат: указатель {p.result}, версия {version}, эпоха {self.epoch}")


def main():
    """Разбирает аргументы и запускает копию координатора."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--run", required=True, help="имя запуска: папка журналов и узел /hh/<run>")
    ap.add_argument("--name", required=True, help="имя копии координатора, например coord-A")
    ap.add_argument("--hosts", default=ZK_HOSTS)
    ap.add_argument("--timeout", type=float, default=SESSION_TIMEOUT, help="тайм-аут сессии, с")
    ap.add_argument("--chunk-mb", type=float, default=16, help="размер сплита, МиБ")
    ap.add_argument("--split-delay", type=float, default=0.3,
                    help="пауза после сплита, с: растягивает расчёт, чтобы успеть приостановить лидера")
    logging.getLogger("kazoo").setLevel(logging.CRITICAL)  # состояния сессии пишет on_state
    Coordinator(ap.parse_args()).start()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
