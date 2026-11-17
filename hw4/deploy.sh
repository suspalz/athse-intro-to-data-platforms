#!/bin/bash
# HW4: работа с данными в Apache Spark под управлением YARN (team-17) по шагам из INSTRUCTION.md
# Установка Apache Spark 3.5.9 на team-17-en, выкладка архива JAR в HDFS (/apps/spark),
# интеграция с YARN и внешним Hive Metastore (:9083), чтение CSV из HDFS, трансформации,
# запись партиционированной Parquet-таблицы hw4.revenue_by_city и проверка чтения через Beeline.
#
# запуск: на edge (team-17-en) под пользователем team
#     bash deploy.sh
# скрипт можно запускать повторно: уже скачанный архив Spark и работающие демоны HDFS/YARN/Hive
# сохраняются без лишнего перезапуска, конфиги обновляются с бэкапом, данные в HDFS и таблица
# hw4.revenue_by_city перезаписываются через -f / overwrite без накопления дублей.

# e - останавливаемся при ошибке, u - неизвестная переменная тоже ошибка,
# pipefail - ошибка в середине трубы | тоже считается; E - сохраняем обработчик ERR.
set -Eeuo pipefail
[[ $# == 0 ]] || { echo "Запуск: bash deploy.sh (без аргументов)"; exit 2; }
[[ $(id -un) == team && $(hostname -s) == team-17-en ]] || {
  echo "Скрипт запускается на team-17-en под пользователем team."; exit 1;
}
sudo -n true || { echo "Нужен sudo без запроса пароля: перед запуском выполнить sudo -v."; exit 1; }
sudo -n -u hadoop true || { echo "Нет прав sudo для перехода в пользователя hadoop."; exit 1; }

if ! command -v tmux >/dev/null 2>&1 || ! command -v wget >/dev/null 2>&1; then
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y tmux wget
fi

mkdir -p "$HOME/hw4-deploy-logs"
LOG="$HOME/hw4-deploy-logs/$(date +%Y%m%d-%H%M%S)-$$.log"
exec > >(tee -a "$LOG") 2>&1
echo "Лог: $LOG"

STEP="подготовка"
step() { STEP=$1; printf '\n========== %s ==========\n' "$STEP"; }
trap 'rc=$?; echo "ОШИБКА на этапе: $STEP (строка $LINENO). Лог: $LOG"; exit "$rc"' ERR

TEAM_SSH=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new
          -o ServerAliveInterval=10 -o ServerAliveCountMax=3)
if [[ -f "$HOME/.ssh/team_internal" ]]; then
  TEAM_SSH=(-i "$HOME/.ssh/team_internal" "${TEAM_SSH[@]}")
fi

# Убеждаемся, что PostgreSQL 16 с БД metastore на team-17-01 запущен перед проверкой Metastore.
ssh "${TEAM_SSH[@]}" team@team-17-01 bash -s <<'CHECK_PG_01'
{
set -euo pipefail
pg_lsclusters | awk '$1 == "16" && $2 == "main" && $4 == "online" {found=1} END {exit !found}' || {
  sudo systemctl start postgresql@16-main
  pg_lsclusters
}
pg_lsclusters | awk '$1 == "16" && $2 == "main" && $4 == "online" {found=1} END {exit !found}'
} </dev/null
CHECK_PG_01

# заходим под нашим сервисным юзером hadoop, всю работу по кластеру делаем от него
sudo -n -i -u hadoop bash -s <<'HADOOP_SPARK_STEPS'
{
set -Eeuo pipefail

STEP="Шаг 1"
step() { STEP=$1; printf '\n========== %s ==========\n' "$STEP"; }
trap 'rc=$?; echo "ОШИБКА внутри блока hadoop на этапе: $STEP (строка $LINENO)"; exit "$rc"' ERR

HD=/home/hadoop/hadoop-3.4.0
HV=/home/hadoop/apache-hive-4.0.0-alpha-2-bin
NODES=(team-17-nn team-17-00 team-17-01)
S=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new
   -o ServerAliveInterval=10 -o ServerAliveCountMax=3)

step "Шаг 1. Проверить кластер HDFS, YARN и службы Hive [hadoop@edge]"
# ---------------------------------------------------------------------------
# проверяем базу из прошлых ДЗ перед установкой Spark: на месте ли Java 11 и Python 3.12 под PySpark, выключен ли Safe mode в HDFS (`OFF`), видны ли все 3 NodeManager в статусе `RUNNING`, слушают ли порты 9083 (Metastore) и 5433 (HiveServer2), плюс смотрим остаток памяти в `free -m`. если что-то отвалилось -> траблшутинг по прошлым ДЗ
set +u
source "$HOME/.profile"
set -u
hostname
java -version
python3 --version
"$HD/bin/hadoop" version
[[ -f "$HV/conf/hive-site.xml" ]] || {
  echo "ОШИБКА: не найден $HV/conf/hive-site.xml (сначала должно быть выполнено ДЗ3)."; exit 1;
}
ssh -n "${S[@]}" team-17-nn 'test -s /home/hadoop/hadoop_data/dfs/name/current/VERSION'

# автозапуска у этих демонов нет, поэтому после ребута поднимаем их вручную. живой Metastore на 9083 нужен самому Spark для записи метаданных таблицы, а HiveServer2 на 5433 пригодится в конце для проверки через beeline
# При повторном запуске поднимаем только отсутствующие службы, не трогая живые PID.
NEED_YARN=0
for h in "${NODES[@]}"; do
  P=$(ssh -n "${S[@]}" "$h" /usr/bin/jps)
  if [[ $h == team-17-nn ]]; then
    grep -q ' NameNode$' <<< "$P" || ssh -n "${S[@]}" "$h" "$HD/bin/hdfs --daemon start namenode"
    grep -q ' SecondaryNameNode$' <<< "$P" || ssh -n "${S[@]}" "$h" "$HD/bin/hdfs --daemon start secondarynamenode"
    grep -q ' ResourceManager$' <<< "$P" || NEED_YARN=1
  fi
  grep -q ' DataNode$' <<< "$P" || ssh -n "${S[@]}" "$h" "$HD/bin/hdfs --daemon start datanode"
  grep -q ' NodeManager$' <<< "$P" || NEED_YARN=1
done

if (( NEED_YARN == 1 )); then
  ssh -n "${S[@]}" team-17-nn "source ~/.profile && '$HD/sbin/start-yarn.sh'"
fi
P_NN=$(ssh -n "${S[@]}" team-17-nn /usr/bin/jps)
grep -q ' JobHistoryServer$' <<< "$P_NN" || ssh -n "${S[@]}" team-17-nn "source ~/.profile && '$HD/bin/mapred' --daemon start historyserver"

EXPECTED=$(printf '%s\n' "${NODES[@]}" | sort)
R="" SM="" YN="" RUNNING=""
for i in $(seq 1 24); do
  if R=$(timeout 30 "$HD/bin/hdfs" dfsadmin -report </dev/null 2>/dev/null) &&
     SM=$(timeout 30 "$HD/bin/hdfs" dfsadmin -safemode get </dev/null 2>/dev/null) &&
     YN=$(timeout 30 "$HD/bin/yarn" node -list -all </dev/null 2>/dev/null); then
    RUNNING=$(awk '$2 == "RUNNING" {sub(/:.*/, "", $1); print $1}' <<< "$YN" | sort)
    if grep -q 'Live datanodes (3)' <<< "$R" &&
       grep -q 'Safe mode is OFF' <<< "$SM" &&
       [[ "$RUNNING" == "$EXPECTED" ]]; then
      break
    fi
  fi
  echo "Ожидание готовности HDFS и YARN, попытка $i/24..."
  sleep 5
done

echo "$R" | grep -E 'Live datanodes|^Name:' || true
echo "$SM"
echo "$YN"
grep -q 'Live datanodes (3)' <<< "$R" || { echo "ОШИБКА: нет 3 живых DataNode."; exit 1; }
grep -q 'Safe mode is OFF' <<< "$SM" || { echo "ОШИБКА: Safe mode не выключился."; exit 1; }
[[ "$RUNNING" == "$EXPECTED" ]] || { echo "ОШИБКА: не все 3 NodeManager в статусе RUNNING."; exit 1; }

mkdir -p "$HOME/hive-logs"
if ! ss -ltn 'sport = :9083' | grep -q ':9083'; then
  tmux kill-session -t hive-metastore 2>/dev/null || true
  pkill -u hadoop -f '[o]rg\.apache\.hadoop\.hive\.metastore\.HiveMetaStore' 2>/dev/null || true
  for i in $(seq 1 15); do
    pgrep -u hadoop -f '[o]rg\.apache\.hadoop\.hive\.metastore\.HiveMetaStore' >/dev/null 2>&1 || break
    sleep 1
  done
  tmux new -d -s hive-metastore '. ~/.profile && hive --service metastore -p 9083 >> ~/hive-logs/metastore.log 2>&1'
  for i in $(seq 1 45); do
    ss -ltn 'sport = :9083' | grep -q ':9083' && break
    sleep 2
  done
fi
ss -ltn 'sport = :9083' | grep -q ':9083' || {
  echo "ОШИБКА: Hive Metastore не открыл порт 9083. Хвост лога:"
  tail -n 60 "$HOME/hive-logs/metastore.log" || true
  exit 1
}

if ! ss -ltn 'sport = :5433' | grep -q ':5433' || ! ss -ltn 'sport = :10002' | grep -q ':10002'; then
  tmux kill-session -t hive-server2 2>/dev/null || true
  pkill -u hadoop -f '[o]rg\.apache\.hive\.service\.server\.HiveServer2' 2>/dev/null || true
  for i in $(seq 1 15); do
    pgrep -u hadoop -f '[o]rg\.apache\.hive\.service\.server\.HiveServer2' >/dev/null 2>&1 || break
    sleep 1
  done
  tmux new -d -s hive-server2 '. ~/.profile && hive --service hiveserver2 >> ~/hive-logs/hiveserver2.log 2>&1'
  for i in $(seq 1 60); do
    if ss -ltn 'sport = :5433' | grep -q ':5433' && ss -ltn 'sport = :10002' | grep -q ':10002'; then
      break
    fi
    sleep 2
  done
fi
ss -ltn | grep -E ':(9083|5433|10002)\b'
if ! ss -ltn 'sport = :5433' | grep -q ':5433' || ! ss -ltn 'sport = :10002' | grep -q ':10002'; then
  echo "ОШИБКА: HiveServer2 не открыл порты 5433 и 10002. Хвост лога:"
  tail -n 80 "$HOME/hive-logs/hiveserver2.log" || true
  exit 1
fi
free -m


step "Шаг 2. Скачать и распаковать Apache Spark 3.5.9 [hadoop@edge]"
# ---------------------------------------------------------------------------
cd "$HOME"
# сразу бэкапим ~/.profile перед правкой, качаем с зеркала Apache сборку Spark 3.5.9 (подходит под наши Java 11 и Python 3.12 на Ubuntu 24.04) и сверяем sha512 до статуса OK (-> архив не битый)
PROFILE_BACKUP="$HOME/.profile.before-hw4-$(date +%Y%m%d-%H%M%S)-$$"
cp -a "$HOME/.profile" "$PROFILE_BACKUP"
echo "Бэкап профиля: $PROFILE_BACKUP"

SPARK_TAR="spark-3.5.9-bin-hadoop3.tgz"
SPARK_SHA="${SPARK_TAR}.sha512"
SPARK_CDN="https://dlcdn.apache.org/spark/spark-3.5.9"
SPARK_ARC="https://archive.apache.org/dist/spark/spark-3.5.9"

if [[ ! -s "$SPARK_SHA" ]]; then
  wget -O "$SPARK_SHA" "${SPARK_CDN}/${SPARK_SHA}" || wget -O "$SPARK_SHA" "${SPARK_ARC}/${SPARK_SHA}"
fi
if [[ ! -f "$SPARK_TAR" ]] || ! sha512sum -c "$SPARK_SHA" >/dev/null 2>&1; then
  wget -c --progress=bar:force:noscroll "${SPARK_CDN}/${SPARK_TAR}" || \
    wget -c --progress=bar:force:noscroll "${SPARK_ARC}/${SPARK_TAR}"
  if ! sha512sum -c "$SPARK_SHA" >/dev/null 2>&1; then
    rm -f "$SPARK_TAR"
    wget --progress=bar:force:noscroll "${SPARK_CDN}/${SPARK_TAR}" || \
      wget --progress=bar:force:noscroll "${SPARK_ARC}/${SPARK_TAR}"
  fi
fi
sha512sum -c "$SPARK_SHA"

# распаковываем архив прямо в /home/hadoop на edge, на рабочие ноды копировать его не надо; exit закрывает временное окно tmux
if [[ ! -x spark-3.5.9-bin-hadoop3/bin/spark-submit || ! -d spark-3.5.9-bin-hadoop3/jars ]]; then
  rm -rf spark-3.5.9-bin-hadoop3
  tar -xzf "$SPARK_TAR"
fi
ls -ld "$HOME/spark-3.5.9-bin-hadoop3"


step "Шаг 3. Настроить переменные окружения Spark [hadoop@edge]"
# ---------------------------------------------------------------------------
# добавляем SPARK_HOME и его bin в конец PATH, чтобы не затереть старый. после source проверяем, что бинарники нашлись и выводится версия 3.5.9
add_profile() { grep -qxF "$1" "$HOME/.profile" || echo "$1" >> "$HOME/.profile"; }
add_profile 'export SPARK_HOME=/home/hadoop/spark-3.5.9-bin-hadoop3'
add_profile 'export PATH=$PATH:$SPARK_HOME/bin'

set +u
source "$HOME/.profile"
set -u
which spark-submit pyspark spark-sql
SPARK_VER=$(spark-submit --version </dev/null 2>&1)
echo "$SPARK_VER"
grep -Fq 'version 3.5.9' <<< "$SPARK_VER" || { echo "ОШИБКА: не подтверждена версия Spark 3.5.9."; exit 1; }


step "Шаг 4. Собрать архив JAR-библиотек Spark и загрузить в HDFS [hadoop@edge]"
# ---------------------------------------------------------------------------
# пакуем все джарники из $SPARK_HOME/jars в один zip. обычной утилиты zip в чистой убунте нет, а у hadoop нет sudo, поэтому собираем архив штатным jar из нашей Java 11 (-c0Mf делает обычный zip без сжатия и лишнего MANIFEST.MF)
(
  cd "$SPARK_HOME/jars"
  jar -c0Mf "$HOME/spark-3.5.9-jars.zip.tmp" ./*.jar
)
mv -f "$HOME/spark-3.5.9-jars.zip.tmp" "$HOME/spark-3.5.9-jars.zip"
ls -lh "$HOME/spark-3.5.9-jars.zip"
[[ -s "$HOME/spark-3.5.9-jars.zip" ]] || { echo "ОШИБКА: архив spark-3.5.9-jars.zip пуст."; exit 1; }

# закидываем этот архив в HDFS (/apps/spark). так мы избавляемся от ручной установки Spark на nn, 00 и 01: когда YARN будет поднимать там контейнеры, он сам стянет архив из HDFS и распакует, а за счет репликации 3 каждая нода возьмет его локально у себя
hdfs dfs -mkdir -p /apps/spark </dev/null
hdfs dfs -put -f "$HOME/spark-3.5.9-jars.zip" /apps/spark/spark-3.5.9-jars.zip </dev/null
HDFS_JARS=$(hdfs dfs -ls -h /apps/spark </dev/null)
echo "$HDFS_JARS"
grep -Fq '/apps/spark/spark-3.5.9-jars.zip' <<< "$HDFS_JARS" || {
  echo "ОШИБКА: /apps/spark/spark-3.5.9-jars.zip не найден в HDFS."; exit 1;
}


step "Шаг 5. Сконфигурировать spark-env.sh, hive-site.xml и spark-defaults.conf [hadoop@edge]"
# ---------------------------------------------------------------------------
if [[ -f "$SPARK_HOME/conf/spark-env.sh" || -f "$SPARK_HOME/conf/hive-site.xml" || -f "$SPARK_HOME/conf/spark-defaults.conf" ]]; then
  SPARK_CFG_BACKUP="$HOME/spark-config-before-hw4-$(date +%Y%m%d-%H%M%S)-$$"
  mkdir -p "$SPARK_CFG_BACKUP"
  for f in spark-env.sh hive-site.xml spark-defaults.conf; do
    [[ -f "$SPARK_HOME/conf/$f" ]] && cp -a "$SPARK_HOME/conf/$f" "$SPARK_CFG_BACKUP/"
  done
  echo "Бэкап прежних конфигов Spark: $SPARK_CFG_BACKUP"
fi

# в spark-env.sh показываем Spark где лежат Java 11, конфиги нашего Hadoop (оттуда он подтянет адреса NameNode и ResourceManager) и системный python3 под PySpark
cat > "$SPARK_HOME/conf/spark-env.sh" <<'SPARK_ENV'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=/home/hadoop/hadoop-3.4.0/etc/hadoop
export PYSPARK_PYTHON=python3
SPARK_ENV
chmod +x "$SPARK_HOME/conf/spark-env.sh"

# связываем Spark с нашим внешним Hive Metastore на порту 9083, иначе он поднимет локальный metastore_db на Derby и Hive потом наши таблицы не увидит. пароль от Postgres тут не пишем, потому что в саму БД ходит только сервис Metastore
cat > "$SPARK_HOME/conf/hive-site.xml" <<'SPARK_HIVE_XML'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://team-17-en:9083</value>
  </property>
</configuration>
SPARK_HIVE_XML

# прописываем основные настройки сессии по дефолту:
# - `spark.master=yarn`, `deployMode=client`: драйвер работает на edge (удобно для интерактивного pyspark), а ApplicationMaster и 2 экзекьютора по 1 ядру запускаются в контейнерах YARN на рабочих нодах
# - память по `512m` на драйвер, AM и экзекьюторы: умещается в бюджет памяти наших виртуалок и не роняет слабую edge-ноду по OOM
# - `shuffle.partitions=8` и `adaptive.enabled=true`: чтобы на небольших таблицах не дробить shuffle при агрегациях на дефолтные 200 партиций и пустых тасок
# - `spark.sql.warehouse.dir`: указываем общий с Hive каталог таблиц в HDFS (`/user/hive/warehouse`)
# - `spark.yarn.archive`: путь к нашему zip с .jar файлами из шага 4
cat > "$SPARK_HOME/conf/spark-defaults.conf" <<'SPARK_DEFAULTS'
spark.master                     yarn
spark.submit.deployMode          client
spark.driver.memory              512m
spark.yarn.am.memory             512m
spark.executor.instances         2
spark.executor.cores             1
spark.executor.memory            512m
spark.sql.adaptive.enabled       true
spark.sql.shuffle.partitions     8
spark.sql.warehouse.dir          hdfs://team-17-nn:9000/user/hive/warehouse
spark.yarn.archive               hdfs://team-17-nn:9000/apps/spark/spark-3.5.9-jars.zip
spark.ui.showConsoleProgress     true
SPARK_DEFAULTS

cd "$HOME"
head -n 30 "$SPARK_HOME"/conf/spark-env.sh "$SPARK_HOME"/conf/hive-site.xml "$SPARK_HOME"/conf/spark-defaults.conf


step "Шаг 6. Проверить связку Spark + YARN + Hive Metastore [hadoop@edge]"
# ---------------------------------------------------------------------------
# делаем быструю проверку перед основной работой: в выводе должны увидеть `Spark master: yarn`, базу `hw3` из прошлого ДЗ (значит внешний Metastore подцепился), а в списке завершенных задач YARN появится приложение с типом SPARK
SPARK_SQL_OUT=$(spark-sql -e "SHOW DATABASES;" </dev/null 2>&1)
echo "$SPARK_SQL_OUT"
grep -Fq 'Spark master: yarn' <<< "$SPARK_SQL_OUT" || { echo "ОШИБКА: spark-sql не запустился с master=yarn."; exit 1; }
grep -qx 'default' <<< "$SPARK_SQL_OUT" || { echo "ОШИБКА: spark-sql не вывел базу default."; exit 1; }

YARN_APPS=$(yarn application -list -appStates FINISHED </dev/null 2>&1)
echo "$YARN_APPS" | tail -n 8
grep -Fq 'SPARK' <<< "$YARN_APPS" || { echo "ОШИБКА: в истории YARN не найдено приложение типа SPARK."; exit 1; }


step "Шаг 7. Подготовить данные и загрузить их в HDFS [hadoop@edge]"
# ---------------------------------------------------------------------------
mkdir -p "$HOME/hw4-data/customers" "$HOME/hw4-data/orders"

cat > "$HOME/hw4-data/customers/customers.csv" <<'CSV_CUSTOMERS'
customer_id,name,city,segment
101,customer_101,Moscow,vip
102,customer_102,Moscow,regular
103,customer_103,Saint-Petersburg,vip
104,customer_104,Kazan,new
105,customer_105,Novosibirsk,regular
106,customer_106,Saint-Petersburg,new
CSV_CUSTOMERS

cat > "$HOME/hw4-data/orders/orders_2026-10-01.csv" <<'CSV_ORDERS_1'
order_id,order_ts,customer_id,category,quantity,price
1001,2026-10-01 10:15:00,101,electronics,1,45000.00
1002,2026-10-01 11:20:00,102,books,2,750.50
1003,2026-10-01 13:05:00,103,clothes,3,3200.00
1004,2026-10-01 16:40:00,101,books,1,1200.00
bad_row_without_commas
1005,2026-10-01 18:10:00,104,electronics,two,9900.00
CSV_ORDERS_1

cat > "$HOME/hw4-data/orders/orders_2026-10-02.csv" <<'CSV_ORDERS_2'
order_id,order_ts,customer_id,category,quantity,price
2001,2026-10-02 09:30:00,103,electronics,1,28000.00
2002,2026-10-02 12:00:00,104,food,4,450.25
2003,2026-10-02 15:45:00,105,clothes,2,4100.00
2004,2026-10-02 19:15:00,106,books,3,600.00
2005,2026-10-02 20:50:00,102,electronics,1,15000.00
CSV_ORDERS_2

# кладем сырые CSV в HDFS `/hw4/raw`, откуда их дальше будет читать Spark; ключ `-f` перезаписывает файлы без ошибки `File exists`, если запускаем шаг повторно
hdfs dfs -mkdir -p /hw4/raw/customers /hw4/raw/orders </dev/null
hdfs dfs -put -f "$HOME/hw4-data/customers/customers.csv" /hw4/raw/customers/ </dev/null
hdfs dfs -put -f "$HOME"/hw4-data/orders/orders_*.csv /hw4/raw/orders/ </dev/null
HDFS_RAW=$(hdfs dfs -ls -R /hw4/raw </dev/null)
echo "$HDFS_RAW"
grep -Fq '/hw4/raw/customers/customers.csv' <<< "$HDFS_RAW" || { echo "ОШИБКА: нет customers.csv в HDFS."; exit 1; }
grep -Fq '/hw4/raw/orders/orders_2026-10-01.csv' <<< "$HDFS_RAW" || { echo "ОШИБКА: нет orders_2026-10-01.csv в HDFS."; exit 1; }
grep -Fq '/hw4/raw/orders/orders_2026-10-02.csv' <<< "$HDFS_RAW" || { echo "ОШИБКА: нет orders_2026-10-02.csv в HDFS."; exit 1; }


step "Шаг 8. Прочитать данные из HDFS, трансформировать и сохранить таблицу в Spark [hadoop@edge]"
# ---------------------------------------------------------------------------
# стартуем интерактивный шелл PySpark на YARN (в баннере видим `master = yarn` и `app id`)
# читаем обе папки из HDFS со строгой схемой. опция `columnNameOfCorruptRecord` складывает кривые строки в отдельную колонку `_corrupt`, сразу выводим их и видим наши две битые записи за 2026-10-01
# чистим и трансформируем данные: выкидываем битые строки, кастим время `order_ts` из строки в `TIMESTAMP`, вытаскиваем дату `dt` под партиционирование, считаем сумму позиции `line_amount`, джойним заказы со справочником клиентов по `customer_id` и собираем агрегаты по дате, городу и сегменту (число заказов, количество товаров и выручка)
# создаем базу `hw4` и сохраняем витрину как таблицу `hw4.revenue_by_city` в формате `parquet` с партиционированием по дате (`partitionBy("dt")`), `overwrite` перезаписывает таблицу без накопления дублей при перезапуске. в конце обязательно выходим через `exit()`, чтобы освободить контейнеры YARN и память на edge: иначе следующий запрос в beeline повиснет в очереди YARN за ресурсами ApplicationMaster
# В неинтерактивном скрипте выполняем ровно тот же PySpark-код через spark-submit и штатно закрываем сессию spark.stop().
cat > "$HOME/hw4-data/hw4_spark_job.py" <<'PYSPARK_JOB'
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("hw4-revenue-by-city")
    .enableHiveSupport()
    .getOrCreate()
)

try:
    sc = spark.sparkContext
    print("Master:", sc.master, "App ID:", sc.applicationId)
    if sc.master != "yarn":
        raise RuntimeError(f"Expected master='yarn', got {sc.master!r}")

    customers = (
        spark.read.option("header", True)
        .schema("customer_id INT, name STRING, city STRING, segment STRING")
        .csv("hdfs://team-17-nn:9000/hw4/raw/customers")
    )
    customers.show(truncate=False)

    orders_schema = (
        "order_id BIGINT, order_ts STRING, customer_id INT, "
        "category STRING, quantity INT, price DOUBLE, _corrupt STRING"
    )
    orders_raw = (
        spark.read.option("header", True)
        .schema(orders_schema)
        .option("columnNameOfCorruptRecord", "_corrupt")
        .csv("hdfs://team-17-nn:9000/hw4/raw/orders")
    )
    orders_raw.cache()
    corrupt_df = orders_raw.where("_corrupt IS NOT NULL")
    corrupt_df.show(truncate=False)
    corrupt_cnt = len(corrupt_df.collect())
    if corrupt_cnt != 2:
        raise RuntimeError(f"Expected 2 corrupt rows, got {corrupt_cnt}")

    orders_clean = (
        orders_raw
        .where("_corrupt IS NULL AND order_id IS NOT NULL AND quantity > 0 AND price IS NOT NULL")
        .withColumn("order_ts", F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("dt", F.date_format("order_ts", "yyyy-MM-dd"))
        .withColumn("line_amount", F.round(F.col("quantity") * F.col("price"), 2))
        .drop("_corrupt")
    )

    revenue_by_city = (
        orders_clean.join(customers, on="customer_id", how="inner")
        .groupBy("dt", "city", "segment")
        .agg(
            F.count("*").alias("orders_cnt"),
            F.sum("quantity").cast("bigint").alias("items_cnt"),
            F.round(F.sum("line_amount"), 2).alias("revenue"),
        )
    )

    revenue_by_city.orderBy("dt", F.desc("revenue")).show(truncate=False)

    spark.sql("CREATE DATABASE IF NOT EXISTS hw4")

    (
        revenue_by_city.write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("dt")
        .saveAsTable("hw4.revenue_by_city")
    )

    spark.sql("SHOW PARTITIONS hw4.revenue_by_city").show(truncate=False)
    saved_df = spark.table("hw4.revenue_by_city").orderBy("dt", F.desc("revenue"))
    saved_df.show(truncate=False)
    total_saved = saved_df.count()
    if total_saved != 8:
        raise RuntimeError(f"Expected 8 rows in hw4.revenue_by_city, got {total_saved}")
finally:
    spark.stop()
PYSPARK_JOB

spark-submit "$HOME/hw4-data/hw4_spark_job.py" </dev/null


step "Шаг 9. Проверить партиции в HDFS [hadoop@edge]"
# ---------------------------------------------------------------------------
# после выхода из pyspark смотрим в сам HDFS: внутри `/user/hive/warehouse/hw4.db/revenue_by_city` лежат две папки `dt=2026-10-01` и `dt=2026-10-02` с файлами `part-*.snappy.parquet` и флагом `_SUCCESS`
HDFS_TBL=$(hdfs dfs -ls -R /user/hive/warehouse/hw4.db/revenue_by_city </dev/null)
echo "$HDFS_TBL"
grep -Fq 'dt=2026-10-01/' <<< "$HDFS_TBL" || { echo "ОШИБКА: нет папки dt=2026-10-01 в HDFS."; exit 1; }
grep -Fq 'dt=2026-10-02/' <<< "$HDFS_TBL" || { echo "ОШИБКА: нет папки dt=2026-10-02 в HDFS."; exit 1; }
grep -Fq '.parquet' <<< "$HDFS_TBL" || { echo "ОШИБКА: не найдены Parquet-файлы в HDFS."; exit 1; }
grep -Fq '_SUCCESS' <<< "$HDFS_TBL" || { echo "ОШИБКА: нет маркера _SUCCESS в HDFS."; exit 1; }


step "Шаг 10. Проверить чтение таблицы клиентом Hive (Beeline) [hadoop@edge]"
# ---------------------------------------------------------------------------
# подключаемся обычным клиентом Hive `beeline` к `HiveServer2:5433`. за счет общего Metastore таблица `hw4.revenue_by_city` и обе ее партиции сразу на месте; проверяем чтение и простым `SELECT *` по партиции, и запросом с `ORDER BY` (он прогоняет задачу через MapReduce поверх записанных Spark Parquet-файлов) -- данные сходятся, выходим через `!quit`
DBS=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SHOW DATABASES;" </dev/null)
TBLS=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SHOW TABLES IN hw4;" </dev/null)
PARTS=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SHOW PARTITIONS hw4.revenue_by_city;" </dev/null)
DESC_TBL=$(beeline -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "DESCRIBE FORMATTED hw4.revenue_by_city;" </dev/null)
DAY1_FETCH=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SELECT * FROM hw4.revenue_by_city WHERE dt='2026-10-01';" </dev/null)
DAY1_MR=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SELECT city, segment, orders_cnt, items_cnt, revenue, dt FROM hw4.revenue_by_city WHERE dt='2026-10-01' ORDER BY revenue DESC;" </dev/null)
DAY2_FETCH=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SELECT * FROM hw4.revenue_by_city WHERE dt='2026-10-02';" </dev/null)

echo "--- SHOW DATABASES ---"
echo "$DBS"
echo "--- SHOW TABLES IN hw4 ---"
echo "$TBLS"
echo "--- SHOW PARTITIONS hw4.revenue_by_city ---"
echo "$PARTS"
echo "--- DESCRIBE FORMATTED hw4.revenue_by_city ---"
echo "$DESC_TBL"
echo "--- SELECT * (dt=2026-10-01, FetchTask) ---"
echo "$DAY1_FETCH"
echo "--- SELECT ... ORDER BY revenue DESC (dt=2026-10-01, MapReduce) ---"
echo "$DAY1_MR"
echo "--- SELECT * (dt=2026-10-02) ---"
echo "$DAY2_FETCH"

grep -qx 'hw4' <<< "$DBS" || { echo "ОШИБКА: база hw4 не видна в Beeline."; exit 1; }
grep -qx 'revenue_by_city' <<< "$TBLS" || { echo "ОШИБКА: таблица revenue_by_city не видна в Beeline."; exit 1; }
grep -qx 'dt=2026-10-01' <<< "$PARTS" || { echo "ОШИБКА: нет партиции dt=2026-10-01 в Beeline."; exit 1; }
grep -qx 'dt=2026-10-02' <<< "$PARTS" || { echo "ОШИБКА: нет партиции dt=2026-10-02 в Beeline."; exit 1; }
[[ $(grep -c '^dt=' <<< "$PARTS") -eq 2 ]] || { echo "ОШИБКА: неожидаемое число партиций в Beeline."; exit 1; }
grep -Fq 'ParquetHiveSerDe' <<< "$DESC_TBL" || { echo "ОШИБКА: таблица не в формате ParquetHiveSerDe."; exit 1; }
grep -Fq 'hdfs://team-17-nn:9000/user/hive/warehouse/hw4.db/revenue_by_city' <<< "$DESC_TBL" || {
  echo "ОШИБКА: неверный Location таблицы в DESCRIBE FORMATTED."; exit 1;
}
grep -Fq $'Moscow\tvip\t2\t2\t46200.0\t2026-10-01' <<< "$DAY1_MR" || {
  echo "ОШИБКА: не найдена ожидаемая строка Moscow|vip|46200.0 в выдаче Beeline."; exit 1;
}
grep -Fq $'Saint-Petersburg\tvip\t1\t3\t9600.0\t2026-10-01' <<< "$DAY1_MR" || {
  echo "ОШИБКА: не найдена ожидаемая строка Saint-Petersburg|vip|9600.0 в выдаче Beeline."; exit 1;
}
grep -Fq $'Moscow\tregular\t1\t2\t1501.0\t2026-10-01' <<< "$DAY1_MR" || {
  echo "ОШИБКА: не найдена ожидаемая строка Moscow|regular|1501.0 в выдаче Beeline."; exit 1;
}
[[ $(grep -c $'\t2026-10-01$' <<< "$DAY1_FETCH") -eq 3 ]] || {
  echo "ОШИБКА: в партиции dt=2026-10-01 должно быть ровно 3 строки."; exit 1;
}
[[ $(grep -c $'\t2026-10-02$' <<< "$DAY2_FETCH") -eq 5 ]] || {
  echo "ОШИБКА: в партиции dt=2026-10-02 должно быть ровно 5 строк."; exit 1;
}


step "Шаг 11. Проверить Web UI и напомнить про SSH-туннель [hadoop@edge]"
# ---------------------------------------------------------------------------
# открываем туннели к веб-интерфейсам NameNode (`9870`), ResourceManager (`8088`), драйвера Spark (`4040`, работает пока запущен сам `pyspark`; при редиректе браузера на `http://team-17-nn:8088/proxy/...` меняем в адресной строке `team-17-nn` на `127.0.0.1`) и HiveServer2 (`10002`). на `http://127.0.0.1:8088/cluster` видны наши отработавшие приложения типа `SPARK` и запрос из Beeline типа `MAPREDUCE`
fetch_ui() {
  if command -v curl >/dev/null 2>&1; then
    curl --noproxy '*' -fsSL --connect-timeout 3 --max-time 10 "$1"
  else
    wget --no-proxy -qO- --timeout=10 "$1"
  fi
}
RM_HTML=$(fetch_ui http://team-17-nn:8088/cluster)
grep -Fq 'All Applications' <<< "$RM_HTML" || { echo "ОШИБКА: YARN Web UI :8088 не вернул страницу кластера."; exit 1; }
HS2_HTML=$(fetch_ui http://127.0.0.1:10002/)
grep -Fq 'HiveServer2' <<< "$HS2_HTML" || { echo "ОШИБКА: Web UI :10002 не вернул страницу HiveServer2."; exit 1; }
echo "OK Web UI: YARN ResourceManager (team-17-nn:8088) и HiveServer2 (127.0.0.1:10002) ответили HTTP 200."
} </dev/null
HADOOP_SPARK_STEPS

echo
echo "Готово: Apache Spark 3.5.9 на YARN, чтение данных с HDFS, трансформации, партиционированная Parquet-таблица"
echo "hw4.revenue_by_city и ее чтение стандартным клиентом Hive (Beeline) успешно проверены."
echo "SSH-туннель для Web UI запускается на своем компьютере по шагу 11 INSTRUCTION.md:"
echo "  стенд Vagrant:     ssh -i keys\\user_key -N -o ExitOnForwardFailure=yes -L 127.0.0.1:9870:team-17-nn:9870 -L 127.0.0.1:8088:team-17-nn:8088 -L 127.0.0.1:4040:team-17-en:4040 -L 127.0.0.1:10002:127.0.0.1:10002 team@10.17.0.10"
echo "  выданный кластер:  ssh -i \"<private_ssh>\" -N -o ExitOnForwardFailure=yes -L 127.0.0.1:9870:team-17-nn:9870 -L 127.0.0.1:8088:team-17-nn:8088 -L 127.0.0.1:4040:team-17-en:4040 -L 127.0.0.1:10002:127.0.0.1:10002 team@178.236.27.243"
echo "Лог сохранен: $LOG"
