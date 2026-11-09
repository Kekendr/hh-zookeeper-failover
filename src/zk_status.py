"""Состояние ансамбля: роль каждого сервера и проверка, обслуживает ли ансамбль клиентов.

Роль сервера берётся командой srvr (four-letter word) на клиентском порту каждого сервера.
Проверка ансамбля: клиент подключается к любому живому серверу, создаёт, читает и удаляет
тестовый узел. Без кворума подключение не устанавливается за отведённое время.

Запуск: python src/zk_status.py --save 01_three_servers
Проверка узла: python src/zk_status.py --node /eph_demo --save 05_ephemeral_exists
"""
import argparse
import logging
import socket
import sys
import time

from kazoo.client import KazooClient

from settings import ZK_HOSTS, ZK_SERVERS, Output, now


def four_letter(port, cmd="srvr"):
    """Отправляет серверу команду из четырёх букв и возвращает ответ или текст ошибки."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
            s.sendall(cmd.encode())
            chunks = []
            while chunk := s.recv(4096):
                chunks.append(chunk)
        return b"".join(chunks).decode("utf-8", "replace")
    except OSError as e:
        return f"ERROR {e.__class__.__name__}"


def server_line(sid, port):
    """Возвращает строку таблицы для одного сервера."""
    reply = four_letter(port)
    if reply.startswith("ERROR"):
        return f"  server.{sid}  :{port}  остановлен (порт не отвечает: {reply[6:]})"
    if "not currently serving" in reply:
        return f"  server.{sid}  :{port}  запущен, но НЕ обслуживает запросы (нет кворума)"
    fields = dict(line.split(": ", 1) for line in reply.splitlines() if ": " in line)
    return (f"  server.{sid}  :{port}  Mode: {fields.get('Mode', '?'):<9} "
            f"Zxid: {fields.get('Zxid', '?')}  узлов: {fields.get('Node count', '?')}")


def probe(out, hosts, wait):
    """Пробует записать и прочитать тестовый узел через ансамбль."""
    zk = KazooClient(hosts=hosts, timeout=10)
    t0 = time.perf_counter()
    try:
        zk.start(timeout=wait)
    except Exception as e:
        out(f"Ансамбль НЕ ОТВЕЧАЕТ: за {wait:g} с не удалось открыть сессию ({e.__class__.__name__})")
        zk.close()
        return
    try:
        path = zk.create("/probe-", b"ok", ephemeral=True, sequence=True)
        data, _ = zk.get(path)
        zk.delete(path)
        out(f"Ансамбль работает: сессия через {zk._connection._socket.getpeername()[1]}, "
            f"запись и чтение {path} = {data.decode()} за {time.perf_counter() - t0:.2f} с")
    except Exception as e:
        out(f"Ансамбль НЕ ОТВЕЧАЕТ: операция не выполнена ({e.__class__.__name__})")
    finally:
        zk.stop()
        zk.close()


def node_info(out, hosts, path):
    """Показывает, существует ли узел и эфемерный ли он."""
    zk = KazooClient(hosts=hosts, timeout=10)
    zk.start(timeout=10)
    stat = zk.exists(path)
    if stat is None:
        out(f"Узел {path}: НЕ СУЩЕСТВУЕТ")
    else:
        data, _ = zk.get(path)
        kind = f"эфемерный, владелец сессия 0x{stat.ephemeralOwner:x}" if stat.ephemeralOwner \
            else "постоянный"
        out(f"Узел {path}: существует, {kind}, данные «{data.decode()}»")
    out(f"Дочерние узлы /: {sorted(zk.get_children('/'))}")
    zk.stop()
    zk.close()


def main():
    """Печатает роли серверов, проверяет ансамбль и, по желанию, узел."""
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--save", default="zk_status", help="имя файла вывода в outputs/")
    ap.add_argument("--node", help="путь узла для проверки, например /eph_demo")
    ap.add_argument("--wait", type=float, default=8, help="сколько ждать сессию, с")
    args = ap.parse_args()
    logging.getLogger("kazoo").setLevel(logging.CRITICAL)  # без сообщений о каждой попытке связи
    out = Output(args.save)
    out(f"{now()}  Состояние ансамбля ZooKeeper ({ZK_HOSTS})")
    for sid, port in ZK_SERVERS.items():
        out(server_line(sid, port))
    probe(out, ZK_HOSTS, args.wait)
    if args.node:
        node_info(out, ZK_HOSTS, args.node)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
