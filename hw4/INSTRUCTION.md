# HW4: работа с данными в Apache Spark под управлением YARN (team-17)

**Стенд:** четыре ВМ Vagrant из ДЗ1-ДЗ3 (папка `hw1/lab`) с теми же именами хостов, внутренними адресами и пользователями (`team`, `hadoop`), что и на выданном кластере. На машинах уже стоят Java 11, Hadoop 3.4.0 (HDFS + YARN + JobHistoryServer) и Apache Hive `4.0.0-alpha-2` с внешним Metastore на PostgreSQL 16 (`team-17-01:5432`). На выданном кластере команды внутри нод те же самые, отличаются только адрес и ключ при входе на `edge` (шаг 0 и туннель в шаге 11).

| Хост | IP | Роли после ДЗ4 |
|---|---|---|
| `team-17-en` | `10.17.0.10` | edge: клиент Hadoop, Hive Metastore (`:9083`), HiveServer2 (`:5433`, Web UI `:10002`), Beeline, **Apache Spark `3.5.9` (Driver в `client`-режиме, Web UI `:4040`)** |
| `team-17-nn` | `10.17.0.11` | NameNode (`:9000`, UI `:9870`), SecondaryNameNode, DataNode, ResourceManager (`:8032`, UI `:8088`), NodeManager (**контейнеры Spark AM / Executor**), JobHistoryServer (`:19888`) |
| `team-17-00` | `10.17.0.12` | DataNode, NodeManager (**контейнеры Spark AM / Executor**) |
| `team-17-01` | `10.17.0.13` | DataNode, NodeManager (**контейнеры Spark AM / Executor**), PostgreSQL 16 (`:5432`, БД `metastore`) |

В заголовках шагов указаны пользователь и нода. После интерактивных переходов (`ssh`, `sudo -i -u hadoop`, `tmux`, `pyspark`, `beeline`, `exit()`, `!quit`) дожидаемся нового приглашения и только потом вводим следующий блок.

---

## Шаг 0. Зайти на edge — [свой компьютер]

- Локальный стенд Vagrant (из папки `hw1/lab`): `ssh -i keys\user_key team@10.17.0.10`
- Выданный кластер: `ssh -i "<private_ssh>" team@178.236.27.243`

`<private_ssh>` меняем на путь к своему приватному SSH-ключу. Дальше работаем после появления `team@team-17-en`.

## Шаг 1. Проверить кластер HDFS, YARN и службы Hive — [team@edge] → [hadoop@edge]

```bash
sudo -i -u hadoop
```

-> заходим под нашим сервисным юзером hadoop, всю работу по кластеру делаем от него

Дожидаемся `hadoop@team-17-en`.

```bash
source ~/.profile
java -version
python3 --version
hdfs dfsadmin -safemode get
yarn node -list -all
ss -ltn | grep -E ':(9083|5433)\b'
free -m
```

-> проверяем базу из прошлых ДЗ перед установкой Spark: на месте ли Java 11 и Python 3.12 под PySpark, выключен ли Safe mode в HDFS (`OFF`), видны ли все 3 NodeManager в статусе `RUNNING`, слушают ли порты 9083 (Metastore) и 5433 (HiveServer2), плюс смотрим остаток памяти в `free -m`. если что-то отвалилось -> траблшутинг по прошлым ДЗ

Если виртуалки выключались и сервисы сейчас остановлены, поднимаем HDFS + YARN + HistoryServer на `nn`, а на `edge` стартуем `HiveMetaStore` и `HiveServer2` в `tmux`:

```bash
ssh team-17-nn 'source ~/.profile && /home/hadoop/hadoop-3.4.0/sbin/start-dfs.sh && /home/hadoop/hadoop-3.4.0/sbin/start-yarn.sh && /home/hadoop/hadoop-3.4.0/bin/mapred --daemon start historyserver'
```

```bash
mkdir -p ~/hive-logs
tmux new -d -s hive-metastore '. ~/.profile && hive --service metastore -p 9083 >> ~/hive-logs/metastore.log 2>&1'
sleep 10
tmux new -d -s hive-server2 '. ~/.profile && hive --service hiveserver2 >> ~/hive-logs/hiveserver2.log 2>&1'
```

Даем сервисам Hive ~30-40 секунд на инициализацию и проверяем порты:

```bash
ss -ltn | grep -E ':(9083|5433)\b'
```

-> автозапуска у этих демонов нет, поэтому после ребута поднимаем их вручную. живой Metastore на 9083 нужен самому Spark для записи метаданных таблицы, а HiveServer2 на 5433 пригодится в конце для проверки через beeline

## Шаг 2. Скачать и распаковать Apache Spark 3.5.9 — [hadoop@edge]

Ставим сборку **Spark `3.5.9` под Hadoop 3** только на `edge`. Качаем внутри `tmux`, чтобы случайный обрыв SSH не сбросил загрузку:

```bash
tmux new -s spark-download
```

```bash
source ~/.profile
cd ~
cp -a ~/.profile ~/.profile.before-hw4-$(date +%Y%m%d-%H%M%S)
wget -c https://dlcdn.apache.org/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
wget -O spark-3.5.9-bin-hadoop3.tgz.sha512 https://dlcdn.apache.org/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz.sha512
sha512sum -c spark-3.5.9-bin-hadoop3.tgz.sha512
```

-> сразу бэкапим ~/.profile перед правкой, качаем с зеркала Apache сборку Spark 3.5.9 (подходит под наши Java 11 и Python 3.12 на Ubuntu 24.04) и сверяем sha512 до статуса OK (-> архив не битый)

```bash
tar -xzf spark-3.5.9-bin-hadoop3.tgz
ls -ld ~/spark-3.5.9-bin-hadoop3
exit
```

-> распаковываем архив прямо в /home/hadoop на edge, на рабочие ноды копировать его не надо; exit закрывает временное окно tmux

## Шаг 3. Настроить переменные окружения Spark — [hadoop@edge]

```bash
nano ~/.profile
```

В конец файла (не трогая старые строки Hadoop и Hive) дописываем:

```bash
export SPARK_HOME=/home/hadoop/spark-3.5.9-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

Сохраняем (`Ctrl+O`, `Enter`, `Ctrl+X`) и подгружаем в текущем шелле:

```bash
source ~/.profile
which spark-submit pyspark spark-sql
spark-submit --version
```

-> добавляем SPARK_HOME и его bin в конец PATH, чтобы не затереть старый. после source проверяем, что бинарники нашлись и выводится версия 3.5.9

## Шаг 4. Собрать архив JAR-библиотек Spark и загрузить в HDFS — [hadoop@edge]

```bash
cd "$SPARK_HOME/jars"
jar -c0Mf ~/spark-3.5.9-jars.zip *.jar
cd ~
ls -lh ~/spark-3.5.9-jars.zip
```

-> пакуем все джарники из $SPARK_HOME/jars в один zip. обычной утилиты zip в чистом Ubuntu нет, а у hadoop нет sudo, поэтому собираем архив штатным jar из нашей Java 11 (-c0Mf делает обычный zip без сжатия и лишнего MANIFEST.MF)

```bash
hdfs dfs -mkdir -p /apps/spark
hdfs dfs -put -f ~/spark-3.5.9-jars.zip /apps/spark/spark-3.5.9-jars.zip
hdfs dfs -ls -h /apps/spark
```

-> закидываем этот архив в HDFS (/apps/spark). так мы избавляемся от ручной установки Spark на nn, 00 и 01: когда YARN будет поднимать там контейнеры, он сам стянет архив из HDFS и распакует, а за счет репликации 3 каждая нода возьмет его локально у себя

## Шаг 5. Сконфигурировать `spark-env.sh`, `hive-site.xml` и `spark-defaults.conf` — [hadoop@edge]

```bash
cd "$SPARK_HOME/conf"
nano "$SPARK_HOME/conf/spark-env.sh"
```

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_CONF_DIR=/home/hadoop/hadoop-3.4.0/etc/hadoop
export PYSPARK_PYTHON=python3
```

```bash
chmod +x "$SPARK_HOME/conf/spark-env.sh"
```

-> в spark-env.sh показываем Spark где лежат Java 11, конфиги нашего Hadoop (оттуда он подтянет адреса NameNode и ResourceManager) и системный python3 под PySpark

Дальше кладем в `$SPARK_HOME/conf/` короткий `hive-site.xml`:

```bash
nano "$SPARK_HOME/conf/hive-site.xml"
```

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://team-17-en:9083</value>
  </property>
</configuration>
```

-> связываем Spark с нашим внешним Hive Metastore на порту 9083, иначе он поднимет локальный metastore_db на Derby и Hive потом наши таблицы не увидит. пароль от Postgres тут не пишем, потому что в саму БД ходит только сервис Metastore

И заполняем `spark-defaults.conf`:

```bash
nano "$SPARK_HOME/conf/spark-defaults.conf"
```

```properties
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
```

```bash
cd ~
head -n 30 "$SPARK_HOME"/conf/spark-env.sh "$SPARK_HOME"/conf/hive-site.xml "$SPARK_HOME"/conf/spark-defaults.conf
```

-> прописываем основные настройки сессии по дефолту:
- `spark.master=yarn`, `deployMode=client`: драйвер работает на edge (удобно для интерактивного pyspark), а ApplicationMaster и 2 экзекьютора по 1 ядру запускаются в контейнерах YARN на рабочих нодах
- память по `512m` на драйвер, AM и экзекьюторы: умещается в бюджет памяти наших виртуалок и не роняет слабую edge-ноду по OOM
- `shuffle.partitions=8` и `adaptive.enabled=true`: чтобы на небольших таблицах не дробить shuffle при агрегациях на дефолтные 200 партиций и пустых тасок
- `spark.sql.warehouse.dir`: указываем общий с Hive каталог таблиц в HDFS (`/user/hive/warehouse`)
- `spark.yarn.archive`: путь к нашему zip с .jar файлами из шага 4

## Шаг 6. Проверить связку Spark + YARN + Hive Metastore — [hadoop@edge]

```bash
spark-sql -e "SHOW DATABASES;"
yarn application -list -appStates FINISHED | tail -n 5
```

-> делаем быструю проверку перед основной работой: в выводе должны увидеть `Spark master: yarn`, базу `hw3` из прошлого ДЗ (значит внешний Metastore подцепился), а в списке завершенных задач YARN появится приложение с типом SPARK

## Шаг 7. Подготовить данные и загрузить их в HDFS — [hadoop@edge]

Создаем небольшой датасет магазина: клиентов `customers.csv` и заказы за два дня `orders_2026-10-01.csv`, `orders_2026-10-02.csv`:

```bash
mkdir -p ~/hw4-data/customers ~/hw4-data/orders

cat > ~/hw4-data/customers/customers.csv <<'CSV'
customer_id,name,city,segment
101,customer_101,Moscow,vip
102,customer_102,Moscow,regular
103,customer_103,Saint-Petersburg,vip
104,customer_104,Kazan,new
105,customer_105,Novosibirsk,regular
106,customer_106,Saint-Petersburg,new
CSV

cat > ~/hw4-data/orders/orders_2026-10-01.csv <<'CSV'
order_id,order_ts,customer_id,category,quantity,price
1001,2026-10-01 10:15:00,101,electronics,1,45000.00
1002,2026-10-01 11:20:00,102,books,2,750.50
1003,2026-10-01 13:05:00,103,clothes,3,3200.00
1004,2026-10-01 16:40:00,101,books,1,1200.00
bad_row_without_commas
1005,2026-10-01 18:10:00,104,electronics,two,9900.00
CSV

cat > ~/hw4-data/orders/orders_2026-10-02.csv <<'CSV'
order_id,order_ts,customer_id,category,quantity,price
2001,2026-10-02 09:30:00,103,electronics,1,28000.00
2002,2026-10-02 12:00:00,104,food,4,450.25
2003,2026-10-02 15:45:00,105,clothes,2,4100.00
2004,2026-10-02 19:15:00,106,books,3,600.00
2005,2026-10-02 20:50:00,102,electronics,1,15000.00
CSV
```

```bash
hdfs dfs -mkdir -p /hw4/raw/customers /hw4/raw/orders
hdfs dfs -put -f ~/hw4-data/customers/customers.csv /hw4/raw/customers/
hdfs dfs -put -f ~/hw4-data/orders/orders_*.csv /hw4/raw/orders/
hdfs dfs -ls -R /hw4/raw
```

-> кладем сырые CSV в HDFS `/hw4/raw`, откуда их дальше будет читать Spark; ключ `-f` перезаписывает файлы без ошибки `File exists`, если запускаем шаг повторно

## Шаг 8. Прочитать данные из HDFS, трансформировать и сохранить таблицу в Spark — [hadoop@edge] → [PySpark]

```bash
pyspark
```

-> стартуем интерактивный шелл PySpark на YARN (в баннере видим `master = yarn` и `app id`)

После появления `>>>`:

```python
from pyspark.sql import functions as F

print("Master:", sc.master, "App ID:", sc.applicationId)

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
orders_raw.where("_corrupt IS NOT NULL").show(truncate=False)
```

-> читаем обе папки из HDFS со строгой схемой. опция `columnNameOfCorruptRecord` складывает кривые строки в отдельную колонку `_corrupt`, сразу выводим их и видим наши две битые записи за 2026-10-01

```python
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
```

-> чистим и трансформируем данные: выкидываем битые строки, кастим время `order_ts` из строки в `TIMESTAMP`, вытаскиваем дату `dt` под партиционирование, считаем сумму позиции `line_amount`, джойним заказы со справочником клиентов по `customer_id` и собираем агрегаты по дате, городу и сегменту (число заказов, количество товаров и выручка)

```python
spark.sql("CREATE DATABASE IF NOT EXISTS hw4")

(
    revenue_by_city.write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("dt")
    .saveAsTable("hw4.revenue_by_city")
)

spark.sql("SHOW PARTITIONS hw4.revenue_by_city").show(truncate=False)
spark.table("hw4.revenue_by_city").orderBy("dt", F.desc("revenue")).show(truncate=False)
exit()
```

-> создаем базу `hw4` и сохраняем витрину как таблицу `hw4.revenue_by_city` в формате `parquet` с партиционированием по дате (`partitionBy("dt")`), `overwrite` перезаписывает таблицу без накопления дублей при перезапуске. в конце обязательно выходим через `exit()`, чтобы освободить контейнеры YARN и память на edge: иначе следующий запрос в beeline повиснет в очереди YARN за ресурсами ApplicationMaster

## Шаг 9. Проверить партиции в HDFS — [hadoop@edge]

```bash
hdfs dfs -ls -R /user/hive/warehouse/hw4.db/revenue_by_city
```

-> после выхода из pyspark смотрим в сам HDFS: внутри `/user/hive/warehouse/hw4.db/revenue_by_city` лежат две папки `dt=2026-10-01` и `dt=2026-10-02` с файлами `part-*.snappy.parquet` и флагом `_SUCCESS`

## Шаг 10. Проверить чтение таблицы клиентом Hive (Beeline) — [hadoop@edge] → [Beeline]

```bash
beeline -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p ''
```

```sql
SHOW DATABASES;
SHOW TABLES IN hw4;
SHOW PARTITIONS hw4.revenue_by_city;
DESCRIBE FORMATTED hw4.revenue_by_city;
SELECT * FROM hw4.revenue_by_city WHERE dt='2026-10-01';
SELECT city, segment, orders_cnt, items_cnt, revenue, dt
FROM hw4.revenue_by_city
WHERE dt='2026-10-01'
ORDER BY revenue DESC;
!quit
```

-> подключаемся обычным клиентом Hive `beeline` к `HiveServer2:5433`. за счет общего Metastore таблица `hw4.revenue_by_city` и обе ее партиции сразу на месте; проверяем чтение и простым `SELECT *` по партиции, и запросом с `ORDER BY` (он прогоняет задачу через MapReduce поверх записанных Spark Parquet-файлов) -- данные сходятся, выходим через `!quit`

## Шаг 11. Открыть SSH-туннель и проверить Web UI — [свой компьютер]

В отдельном терминале на своем компьютере прокидываем порты до `edge`:

**Локальный стенд Vagrant, из папки `hw1/lab` (PowerShell):**

```powershell
ssh -i keys\user_key -N -o ExitOnForwardFailure=yes `
  -L 127.0.0.1:9870:team-17-nn:9870 `
  -L 127.0.0.1:8088:team-17-nn:8088 `
  -L 127.0.0.1:4040:team-17-en:4040 `
  -L 127.0.0.1:10002:127.0.0.1:10002 `
  team@10.17.0.10
```

**Выданный кластер -- Bash:**

```bash
ssh -i "<private_ssh>" -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:9870:team-17-nn:9870 \
  -L 127.0.0.1:8088:team-17-nn:8088 \
  -L 127.0.0.1:4040:team-17-en:4040 \
  -L 127.0.0.1:10002:127.0.0.1:10002 \
  team@178.236.27.243
```

-> открываем туннели к веб-интерфейсам NameNode (`9870`), ResourceManager (`8088`), драйвера Spark (`4040`, работает пока запущен сам `pyspark`; при редиректе браузера на `http://team-17-nn:8088/proxy/...` меняем в адресной строке `team-17-nn` на `127.0.0.1`) и HiveServer2 (`10002`). на `http://127.0.0.1:8088/cluster` видны наши отработавшие приложения типа `SPARK` и запрос из Beeline типа `MAPREDUCE`
