# Отказоустойчивый координатор расчёта навыков на Apache ZooKeeper

Домашнее задание №4 по курсу инженерии данных.
Автор: Шайкин Андрей Сергеевич, ИСУ 468044, группа ИИИ-3-ИнжДан-1-3.
Вариант 2 — «Программист, разработчик на Python».

Ансамбль из трёх серверов ZooKeeper 3.9.6 и две копии координатора расчёта навыков, пар и троек
навыков из ДЗ-3 ([hh-skills-mapreduce](https://github.com/Kekendr/hh-skills-mapreduce)).
Описание, скриншоты и выводы — в отчёте [`report/Шайкин_дз4.pdf`](report/Шайкин_дз4.pdf).

## Состав

- `conf/zk1..zk3/zoo.cfg` — конфигурации серверов ансамбля;
- `src/` — координатор и вспомогательные скрипты;
- `logs/` — журналы всех запусков координаторов и `pause_leader.py`;
- `outputs/` — выводы `zk_status.py` и `check_result.py`;
- `reference/dz3_mapreduce_summary.json` — итог MapReduce из ДЗ-3 для сверки.

## Запуск

Нужны Java 17, ZooKeeper 3.9.6, Python 3.11+ и RustFS с файлом `raw/it_vacancies_2024.csv`
в бакете `hh-lake` (загрузка — `src/upload.py` из ДЗ-3).

### Ансамбль

Архив ZooKeeper распакован в `D:\zk\zk1`, `D:\zk\zk2`, `D:\zk\zk3`, JDK — в `D:\zk\jdk-17.0.21+9`.
В каждую копию кладётся свой `conf/zoo.cfg` из репозитория и файл `D:\zk\zkN\data\myid` с номером
сервера. Каждый сервер запускается в своём окне:

```powershell
$env:JAVA_HOME='D:\zk\jdk-17.0.21+9'; D:\zk\zk1\bin\zkServer.cmd
```

### Скрипты

```powershell
pip install -r requirements.txt

# Роли серверов и проверка ансамбля; --node показывает узел, например эфемерный
python src/zk_status.py --save 01_roles
python src/zk_status.py --node /eph_demo --save 05_ephemeral_exists

# Этап 3: в трёх окнах по порядку, затем итог
python src/pause_leader.py --run run1_nofence
python src/coordinator.py --run run1_nofence --name coord-A
python src/coordinator.py --run run1_nofence --name coord-B
python src/check_result.py --run run1_nofence

# Этап 4: то же с run2_fencing, в окнах обоих координаторов перед запуском
$env:FENCING='1'

# Один координатор без паузы после сплитов
python src/coordinator.py --run run0_baseline --name coord-A --split-delay 0
```

Адреса задаются переменными `ZK_HOSTS`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`.
