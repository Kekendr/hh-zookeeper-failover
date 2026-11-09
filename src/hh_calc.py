"""Расчёт навыков, пар и троек навыков варианта 2 по одному сплиту CSV из хранилища.

Функции перенесены из src/mapreduce.py репозитория ДЗ-3 (hh-skills-mapreduce) без изменения
логики: строка принадлежит сплиту, в котором лежит её первый байт, навыки приводятся
к нижнему регистру, контрольная сумма sha256 считается так же, как в ДЗ-3.
"""
import csv
import gzip
import hashlib
import io
import json
import time
from collections import Counter
from itertools import combinations

from settings import BUCKET, RAW_KEY, VARIANT_KEYWORD, VARIANT_ROLE

csv.field_size_limit(1 << 30)
K_SIZES = (1, 2, 3)
TAIL_STEP = 64 * 1024          # шаг дочитывания строки за границей сплита
SEP = "\x1f"                   # разделитель навыков в ключе сочетания
ROLE_BYTES = VARIANT_ROLE.encode("utf-8")
KEYWORD_BYTES = VARIANT_KEYWORD.encode("ascii")


def get_range(s3, lo, hi):
    """Читает байты [lo, hi] объекта через Range-запрос."""
    return s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={lo}-{hi}")["Body"].read()


def plan_splits(s3, chunk_mb):
    """Возвращает размер объекта, номера нужных колонок и список сплитов (номер, начало, конец)."""
    size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
    header = next(csv.reader([get_range(s3, 0, 65535).split(b"\n", 1)[0].decode("utf-8")]))
    cols = (header.index("role"), header.index("name"), header.index("key_skills"))
    chunk = int(chunk_mb * 2**20)
    splits = [(i, s, min(s + chunk, size)) for i, s in enumerate(range(0, size, chunk))]
    return size, cols, splits


def read_split(s3, start, end, size):
    """Возвращает строки, которые начинаются в диапазоне [start, end)."""
    lo = start - 1 if start > 0 else 0
    data = get_range(s3, lo, end - 1)
    if start > 0:
        nl = data.find(b"\n")
        if nl == -1:                       # весь сплит внутри чужой строки
            return b""
        data = data[nl + 1:]
    if not data:
        return b""
    if end < size and not data.endswith(b"\n"):
        pos = end                          # дочитываем последнюю строку за границей сплита
        while pos < size:
            tail = get_range(s3, pos, min(pos + TAIL_STEP, size) - 1)
            nl = tail.find(b"\n")
            if nl != -1:
                data += tail[: nl + 1]
                break
            data += tail
            pos += len(tail)
    return data


def normalize_skills(raw):
    """Приводит навыки к виду: уникальные, без пробелов по краям, в нижнем регистре."""
    return sorted({s.strip().lower() for s in raw.split(";") if s.strip()})


def is_variant(role, name, skills):
    """Проверяет условие варианта: нужная роль и Python в навыках или в названии."""
    return role == VARIANT_ROLE and (VARIANT_KEYWORD in skills or VARIANT_KEYWORD in name.lower())


def count_split(s3, idx, start, end, size, cols, prefilter=False):
    """Считает сочетания навыков в одном сплите, возвращает счётчики k=1..3 и статистику."""
    t0 = time.perf_counter()
    i_role, i_name, i_skills = cols
    counters = [Counter() for _ in K_SIZES]
    st = Counter()
    for li, line in enumerate(read_split(s3, start, end, size).split(b"\n")):
        if not line or (idx == 0 and li == 0):     # пустая строка или заголовок CSV
            continue
        st["lines"] += 1
        # предфильтр из ДЗ-3: без байтов роли и слова python строка точно не подходит
        if prefilter and (ROLE_BYTES not in line or KEYWORD_BYTES not in line.lower()):
            continue
        rec = next(csv.reader([line.decode("utf-8")]))
        skills = normalize_skills(rec[i_skills])
        if not is_variant(rec[i_role], rec[i_name], skills):
            continue
        st["variant_vacancies"] += 1
        if not skills:
            continue
        st["with_skills"] += 1
        for ki, k in enumerate(K_SIZES):
            for combo in combinations(skills, k):  # навыки уже отсортированы
                counters[ki][SEP.join(combo)] += 1
    st["seconds"] = round(time.perf_counter() - t0, 3)
    return counters, dict(st)


def pack(counters, stats):
    """Упаковывает частичный результат сплита в gzip-JSON для чекпоинта."""
    body = {"stats": stats, **{f"k{k}": dict(c) for k, c in zip(K_SIZES, counters)}}
    return gzip.compress(json.dumps(body, ensure_ascii=False).encode("utf-8"), 6)


def unpack(blob):
    """Распаковывает частичный результат сплита из чекпоинта."""
    body = json.loads(gzip.decompress(blob).decode("utf-8"))
    return [Counter(body[f"k{k}"]) for k in K_SIZES], body["stats"]


def to_rows(counter):
    """Сортирует по убыванию частоты, затем по ключу, как в ДЗ-3."""
    return sorted(((tuple(key.split(SEP)), n) for key, n in counter.items()),
                  key=lambda kv: (-kv[1], kv[0]))


def canonical_digest(counters):
    """Считает sha256 результата тем же способом, что в ДЗ-3 (контрольное число)."""
    h = hashlib.sha256()
    for k, c in zip(K_SIZES, counters):
        for key in sorted(tuple(x.split(SEP)) for x in c):
            h.update(f"{k}\t{SEP.join(key)}\t{c[SEP.join(key)]}\n".encode("utf-8"))
    return h.hexdigest()


def to_csv(k, counter):
    """Формирует CSV с частотами сочетаний размера k в формате ДЗ-3."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow([f"skill{j}" for j in range(1, k + 1)] + ["count"])
    for key, n in to_rows(counter):
        w.writerow(list(key) + [n])
    return buf.getvalue().encode("utf-8")
