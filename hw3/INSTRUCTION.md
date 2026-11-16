# HW3: развертывание Apache Hive (team-17)

**Стенд:** четыре ВМ Vagrant из ДЗ1 и ДЗ2 (папка `hw1/lab`) с теми же именами хостов, внутренними адресами и пользователями (`team`, `hadoop`), что и на выданном кластере. На кластере уже установлены Java 11 и Hadoop 3.4.0 в `/home/hadoop/hadoop-3.4.0`, настроены и проверены HDFS, YARN и JobHistoryServer. На выданном удаленном кластере все внутренние команды точно такие же; отличаются только адрес и ключ при подключении с локального компьютера к `edge` (шаг 0, шаг 13 и SSH-туннель в шаге 14).

| Хост | IP | Роли после ДЗ3 |
|---|---|---|
| `team-17-en` | `10.17.0.10` | edge: клиент Hadoop, **Hive Metastore (`:9083`)**, **HiveServer2 (`:5433`, Web UI `:10002`)**, Beeline |
| `team-17-nn` | `10.17.0.11` | NameNode, SecondaryNameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer |
| `team-17-00` | `10.17.0.12` | DataNode, NodeManager |
| `team-17-01` | `10.17.0.13` | DataNode, NodeManager, **PostgreSQL 16 (`:5432`, БД `metastore`)** |

В заголовках шагов указаны пользователь и нода. После интерактивных переходов (`ssh`, `sudo -i -u hadoop`, `tmux`, `beeline`, `exit`, `!quit`) дожидаемся нового приглашения и только потом вводим следующий блок команд.

---

## Шаг 0. Зайти на edge — [свой компьютер]

- Локальный стенд Vagrant (из папки `hw1/lab`): `ssh -i keys\user_key team@10.17.0.10`
- Выданный кластер: `ssh -i "<private_ssh>" team@178.236.27.243`

`<private_ssh>` заменяется путем к своему приватному SSH-ключу. Следующие команды выполняются после появления приглашения `team@team-17-en`.

## Шаг 1. Проверить кластер и поднять HDFS + YARN — [team@edge] → [hadoop@edge]

```bash
sudo -i -u hadoop
```

-> переключаемся в нашего сервисного юзера hadoop -- всю работа по hadoop всегда делаем от его имени, хороший тон

Дожидаемся приглашения `hadoop@team-17-en`.

```bash
source ~/.profile
hostname
java -version
hadoop version
hdfs dfsadmin -report
hdfs dfsadmin -safemode get
yarn node -list -all
```

-> подгружаем переменные окружения и проверяем базу после ДЗ1 и ДЗ2 -- Java 11, Hadoop 3.4.0, живы ли все 3 DataNode, выключен ли Safe mode и видны ли 3 NodeManager в статусе RUNNING

Если после перезапуска ВМ службы Hadoop остановлены (`Connection refused` на `nn:9000` / `nn:8032`), поднимаем их одной командой на `nn` и повторяем проверку выше:

```bash
ssh team-17-nn 'source ~/.profile && /home/hadoop/hadoop-3.4.0/sbin/start-dfs.sh && /home/hadoop/hadoop-3.4.0/sbin/start-yarn.sh && /home/hadoop/hadoop-3.4.0/bin/mapred --daemon start historyserver'
```

-> у демонов hadoop нет автозапуска через systemd, поэтому после выключения виртуалок стартуем HDFS, YARN и HistoryServer через ssh на управляющей ноде nn; ждем пока Safe mode станет OFF

Возвращаемся в `team@team-17-en`, так как установка системного пакета PostgreSQL на следующей ноде требует прав `sudo`:

```bash
exit
```

## Шаг 2. Установить PostgreSQL 16 под внешний Metastore — [team@edge] → [team@01]

Чтобы не использовать однопользовательский embedded Derby (который блокирует базу метаданных в папке запуска и не дает работать нескольким клиентам), выносим хранение метаданных Hive во внешнюю СУБД PostgreSQL на ноду `team-17-01`.

Переходим с `edge` на `team-17-01` (на локальном стенде ключ лежит в `~/.ssh/team_internal`; на выданном кластере используется внутренний ключ `team` к рабочим нодам):

```bash
ssh -i ~/.ssh/team_internal team@team-17-01
```

Дожидаемся приглашения `team@team-17-01`.

```bash
sudo apt update
sudo apt install -y postgresql-16
pg_lsclusters
```

-> обновляем списки пакетов и ставим PostgreSQL 16 на ноду 01 -- так разгружаем память слабой edge-ноды; через pg_lsclusters проверяем, что кластер 16/main поднялся на порту 5432 в статусе online

## Шаг 3. Открыть сетевой доступ к PostgreSQL с edge — [team@01]

Перед правкой конфигов сохраняем резервную копию настроек кластера PostgreSQL:

```bash
BACKUP="$HOME/postgresql-before-hw3-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP"
sudo cp -a /etc/postgresql/16/main "$BACKUP/"
```

-> бэкапим дефолтные конфиги постгреса с таймстампом, чтобы легко откатиться если ошибемся в синтаксисе и служба не рестартанет

Узнаем внутренний IP `edge`-ноды:

```bash
getent ahostsv4 team-17-en
```

Открываем основной конфиг сервера:

```bash
sudo nano /etc/postgresql/16/main/postgresql.conf
```

Находим параметр `listen_addresses` (по умолчанию закомментирован и слушает только `localhost`) и приводим к виду:

```ini
listen_addresses = 'localhost,team-17-01'
port = 5432
```

-> по дефолту постгрес слушает только локальную петлю 127.0.0.1 и снаружи к нему не достучаться; добавляем имя/адрес ноды team-17-01, чтобы принимать подключения внутри сети кластера на порту 5432, оставляя и localhost для локального psql

Теперь настраиваем правила клиентской аутентификации:

```bash
sudo nano /etc/postgresql/16/main/pg_hba.conf
```

В конец файла добавляем правило для подключения будущего Metastore с `edge` (`10.17.0.10/32`):

```text
host    metastore       hive            10.17.0.10/32           scram-sha-256
```

-> listen_addresses только открыл сетевой порт, а pg_hba.conf решает кого пускать; по принципу минимальных прав разрешаем TCP вход только к базе metastore, только роли hive, только с конкретного IP нашей edge-ноды /32 и с современным хешированием пароля scram-sha-256

Перезапускаем PostgreSQL и проверяем сокеты:

```bash
sudo systemctl restart postgresql@16-main
sudo systemctl status postgresql@16-main --no-pager
ss -ltn 'sport = :5432'
```

-> смена listen_addresses требует полного рестарта службы, проверяем что сервис active (running), а в ss -ltn порт 5432 слушается сразу на двух адресах -- 127.0.0.1:5432 и 10.17.0.13:5432

## Шаг 4. Создать роль `hive` и базу `metastore` — [team@01]

Заходим в консоль PostgreSQL от системного суперпользователя `postgres`:

```bash
sudo -u postgres psql -v ON_ERROR_STOP=1
```

Внутри приглашения `postgres=#` создаем пользователя, задаем ему пароль интерактивно и создаем базу:

```sql
CREATE USER hive;
\password hive
CREATE DATABASE metastore OWNER hive;
\du hive
\l metastore
\q
```

-> создаем отдельную роль hive, а пароль задаем через \password hive -- так он вводится скрыто, сразу хешируется клиентом и не светится открытым текстом в истории psql и логах. следом создаем пустую БД metastore с владельцем hive, чтобы schematool сам смог накатить внутри таблицы без прав суперюзера. команды \du и \l проверяют результат, \q выходит из psql

Возвращаемся с `team-17-01` обратно на `edge`:

```bash
exit
```

## Шаг 5. Проверить доступность PostgreSQL с edge — [team@edge]

Дожидаемся приглашения `team@team-17-en`. Ставим клиент `postgresql-client` (и заодно `tmux` для следующих шагов) и проверяем вход по сети под ролью `hive` (команда попросит ввести пароль из шага 4):

```bash
sudo apt update
sudo apt install -y postgresql-client tmux
psql "host=team-17-01 port=5432 dbname=metastore user=hive sslmode=prefer" -c "SELECT current_database(), current_user;"
```

-> ставим psql и tmux на edge и до установки самого Hive убеждаемся, что сеть, порт 5432, правило в pg_hba.conf и наш пароль роли hive реально работают -- в ответ должны увидеть metastore и hive

## Шаг 6. Скачать дистрибутив Hive 4.0.0-alpha-2 и JDBC-драйвер — [team@edge] → [hadoop@edge]

Переключаемся обратно в пользователя `hadoop`:

```bash
sudo -i -u hadoop
```

Дожидаемся приглашения `hadoop@team-17-en`. Так как архив Apache может отдавать старые релизы небыстро, запускаем загрузку внутри `tmux`, чтобы обрыв SSH не прервал скачивание:

```bash
tmux new -s hive-download
```

Внутри сессии `tmux`:

```bash
source ~/.profile
cd ~
cp -a ~/.profile ~/.profile.before-hw3-$(date +%Y%m%d-%H%M%S)
wget -c https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2/apache-hive-4.0.0-alpha-2-bin.tar.gz
wget -O apache-hive-4.0.0-alpha-2-bin.tar.gz.sha256 https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2/apache-hive-4.0.0-alpha-2-bin.tar.gz.sha256
sha256sum -c apache-hive-4.0.0-alpha-2-bin.tar.gz.sha256
```

-> сразу бэкапим ~/.profile перед будущими правками, качаем рекомендованный в задании бинарный архив Hive 4.0.0-alpha-2 с ключом -c для докачки при сбое и сверяем контрольную сумму sha256 -- ждем статус OK, чтобы не распаковать битый архив

Распаковываем архив и обновляем JDBC-драйвер PostgreSQL в `lib/`:

```bash
tar -xzf apache-hive-4.0.0-alpha-2-bin.tar.gz
mkdir -p ~/hive-disabled-jars
if [ -f apache-hive-4.0.0-alpha-2-bin/lib/postgresql-42.4.1.jar ]; then
  mv apache-hive-4.0.0-alpha-2-bin/lib/postgresql-42.4.1.jar ~/hive-disabled-jars/
fi
wget -O apache-hive-4.0.0-alpha-2-bin/lib/postgresql-42.7.14.jar https://jdbc.postgresql.org/download/postgresql-42.7.14.jar
exit
```

-> распаковываем Hive; сам он написан на Java и общается с Postgres через JDBC-драйвер -- старый встроенный postgresql-42.4.1.jar (с ним PostgreSQL нашей верси работать не будет) убираем из lib/ в отдельную папку, чтобы в classpath не конфликтовали две версии сразу, и докачиваем свежий postgresql-42.7.14.jar под наш PostgreSQL 16; exit закрывает временное окно tmux

Проверяем, что в `lib/` остался только новый драйвер:

```bash
ls -lh ~/apache-hive-4.0.0-alpha-2-bin/lib/postgresql* ~/hive-disabled-jars/
```

## Шаг 7. Настроить переменные окружения Hive — [hadoop@edge]

Открываем профиль пользователя `hadoop`:

```bash
nano ~/.profile
```

В конец файла (сохраняя строки `HADOOP_HOME` и `JAVA_HOME` из прошлых ДЗ) добавляем блок для Hive:

```bash
export HIVE_HOME=/home/hadoop/apache-hive-4.0.0-alpha-2-bin
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HADOOP_CONF_DIR=/home/hadoop/hadoop-3.4.0/etc/hadoop
export PATH=$PATH:$HIVE_HOME/bin
```

Сохраняем файл (`Ctrl+O`, `Enter`, `Ctrl+X`), применяем в текущей оболочке и проверяем версию:

```bash
source ~/.profile
which hive beeline schematool
hive --version
```

-> прописываем пути до самого Hive, его папки конфигов и конфигов Hadoop, чтобы Hive знал адреса нашего HDFS и YARN, плюс добавляем bin в конец PATH не затирая старый. после source проверяем, что бинарники находятся и выводится версия Hive 4.0.0-alpha-2

## Шаг 8. Сконфигурировать `hive-site.xml` — [hadoop@edge]

Создаем рабочий конфиг с ограниченными правами доступа (внутри будет пароль от БД `metastore`):

```bash
touch "$HIVE_CONF_DIR/hive-site.xml"
chmod 600 "$HIVE_CONF_DIR/hive-site.xml"
nano "$HIVE_CONF_DIR/hive-site.xml"
```

Заполняем файл (подставив вместо `<POSTGRES_PASSWORD>` пароль роли `hive` из шага 4):

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>hive.server2.authentication</name>
    <value>NONE</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>team-17-en</value>
  </property>
  <property>
    <name>hive.server2.thrift.port</name>
    <value>5433</value>
  </property>
  <property>
    <name>hive.server2.enable.doAs</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.security.authorization.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.server2.webui.host</name>
    <value>127.0.0.1</value>
  </property>
  <property>
    <name>hive.server2.webui.port</name>
    <value>10002</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://team-17-en:9083</value>
  </property>
  <property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://team-17-01:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value><POSTGRES_PASSWORD></value>
  </property>
</configuration>
```

-> права 600 ставим, чтобы чужие юзеры не прочитали пароль БД
- `hive.server2.thrift.bind.host` / `port` (`team-17-en:5433`): сетевой адрес и порт HiveServer2, куда по JDBC подключаются клиенты Beeline
- `authentication=NONE`, `authorization.enabled=false`, `enable.doAs=false`: учебный режим без Kerberos, а операции в HDFS идут от имени запустившего сервис юзера `hadoop`
- `webui.host` / `port` (`127.0.0.1:10002`): веб-интерфейс HiveServer2 слушает только 127.0.0.1 на самой edge, смотреть будем через SSH-туннель
- `hive.metastore.uris` (`thrift://team-17-en:9083`): ключевая настройка non-embedded режима -- явно заставляет HiveServer2 ходить в отдельный сервис Metastore по Thrift, а не поднимать локальный встроенный metastore
- `hive.metastore.event.db.notification.api.auth=false`: разрешает HiveServer2 при старте опрашивать события удаленного Metastore от имени `hadoop` без настройки proxyuser в `core-site.xml` (иначе HS2 застревает в цикле инициализации и не открывает порт 5433)
- `hive.metastore.warehouse.dir` (`/user/hive/warehouse`): корневой каталог таблиц Hive в HDFS
- `javax.jdo.option.*`: JDBC-реквизиты подключения самого Metastore к нашей БД PostgreSQL на `team-17-01:5432/metastore`

## Шаг 9. Подготовить каталоги в HDFS и инициализировать схему Metastore — [hadoop@edge]

Создаем рабочие директории Hive в HDFS и выставляем права:

```bash
hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod 1777 /tmp
hdfs dfs -chmod 775 /user/hive/warehouse
hdfs dfs -ls -d /tmp /user/hive/warehouse
```

-> создаем в HDFS временную папку /tmp с правами 1777 (1 - sticky bit) под промежуточные файлы запросов и каталог хранилища таблиц /user/hive/warehouse с правами 775; проверяем права через ls -d

Инициализируем схему таблиц Hive в базе `metastore` (делается один раз) и проверяем результат:

```bash
schematool -dbType postgres -initSchema
schematool -dbType postgres -info
```

-> пустой базы в Postgres мало -- schematool -initSchema накатывает в нее десятки системных таблиц Hive для хранения описаний баз, колонок и партиций; вторая команда с -info сверяет, что и версия дистрибутива, и версия схемы в БД совпадают -- 4.0.0-alpha-2; повторно -initSchema на живой базе делать не нужно (и не желательно)

## Шаг 10. Запустить сервисы Metastore и HiveServer2 — [hadoop@edge]

Подготавливаем каталог под логи и убеждаемся, что порты свободны:

```bash
mkdir -p ~/hive-logs
jps -lm
ss -ltn
```

-> создаем папку для логов и перед первым стартом проверяем, что старые экземпляры служб еще не висят в памяти и порты 9083, 5433, 10002 свободны

Запускаем отдельный сервис **Hive Metastore** в первой именованной сессии `tmux`:

```bash
tmux new -s hive-metastore
```

Внутри окна `tmux`:

```bash
source ~/.profile
hive --service metastore -p 9083 2>&1 | tee -a ~/hive-logs/metastore.log
```

Оставляем процесс работать и сворачиваем сессию комбинацией **Ctrl+B**, затем **D**. Проверяем, что порт `9083` поднялся:

```bash
ss -ltnp 'sport = :9083'
```

-> запускаем самостоятельный процесс Metastore на порту 9083 внутри tmux, чтобы он не упал при закрытии SSH, а вывод дублируем через tee в лог-файл; отцепляемся от окна и ждем пока java начнет слушать порт 9083

Теперь запускаем **HiveServer2** во второй сессии `tmux`:

```bash
tmux new -s hive-server2
```

Внутри окна `tmux`:

```bash
source ~/.profile
hive --service hiveserver2 2>&1 | tee -a ~/hive-logs/hiveserver2.log
```

Снова отсоединяемся через **Ctrl+B**, затем **D**. Даем серверу ~30-40 секунд на инициализацию и проверяем оба процесса и все три порта:

```bash
tmux ls
jps -lm
ss -ltnp | grep -E ':(5433|9083|10002)\b'
```

-> аналогично во втором окне tmux поднимаем HiveServer2, который принимает SQL-клиентов и общается с нашим Metastore; проверяем что живы обе сессии tmux, два процесса RunJar в jps -lm и открыты все три порта -- 9083 у Metastore, 5433 под JDBC и 10002 под Web UI

## Шаг 11. Проверить подключение через Beeline — [hadoop@edge]

Подключаемся клиентом `beeline` к запущенному `HiveServer2`:

```bash
beeline -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p ''
```

После появления приглашения `0: jdbc:hive2://team-17-en:5433/default>` проверяем работу метастора и настройки сессии:

```sql
SHOW DATABASES;
SET hive.metastore.uris;
SET hive.execution.engine;
!quit
```

-> подключаемся к HiveServer2 по JDBC на порт 5433 в базу default (пароль пустой из-за учебного режима NONE). запрос SHOW DATABASES проходит всю цепочку Beeline -> HS2 -> Metastore -> Postgres и возвращает базу default. два SET подтверждают работу через удаленный thrift://team-17-en:9083 и штатный движок mr, дальше выходим через !quit

## Шаг 12. Подготовить CSV-данные и загрузить их в партиционированную таблицу — [hadoop@edge]

Создаем на `edge` два небольших CSV-файла с продажами за две разные даты (по 3 строки в каждом, без заголовка и без столбца даты -- сама дата станет ключом партиции):

```bash
mkdir -p ~/hw3-data
cat > ~/hw3-data/sales-2026-10-01.csv <<'CSV'
1,book,2,500.00
2,pen,5,30.00
3,notebook,1,120.00
CSV

cat > ~/hw3-data/sales-2026-10-02.csv <<'CSV'
4,book,1,500.00
5,pen,3,30.00
6,notebook,2,120.00
CSV

wc -l ~/hw3-data/sales-*.csv
```

-> готовим тестовый датасет за два дня прямо на диске edge-ноды, потому что для команды LOAD DATA LOCAL через HiveServer2 локальной считается файловая система самого сервера HS2, wc -l подтверждает по 3 строки в каждом файле

Снова открываем `beeline`:

```bash
beeline -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p ''
```

Создаем базу `hw3`, внешнюю партиционированную таблицу `sales` и загружаем в нее оба файла по разным партициям:

```sql
CREATE DATABASE IF NOT EXISTS hw3;
USE hw3;

CREATE EXTERNAL TABLE IF NOT EXISTS sales (
  sale_id INT,
  product STRING,
  quantity INT,
  unit_price DECIMAL(10,2)
)
PARTITIONED BY (sale_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/hw3_sales';

LOAD DATA LOCAL INPATH '/home/hadoop/hw3-data/sales-2026-10-01.csv'
OVERWRITE INTO TABLE hw3.sales PARTITION (sale_date='2026-10-01');

LOAD DATA LOCAL INPATH '/home/hadoop/hw3-data/sales-2026-10-02.csv'
OVERWRITE INTO TABLE hw3.sales PARTITION (sale_date='2026-10-02');
```

-> создаем ДБ hw3 и внешнюю таблицу EXTERNAL TABLE с разделителем `,` в каталоге HDFS /user/hive/warehouse/hw3_sales и партиционированием PARTITIONED BY (sale_date) -- так Hive раскладывает данные разных дней по отдельным подпапкам и при фильтрации по дате не читает лишние файлы; двумя командами LOAD DATA LOCAL ... OVERWRITE загружаем каждый CSV в свою партицию без накопления дублей при перезапуске

Тут же в `beeline` проверяем зарегистрированные партиции, чтение строк и метаданные таблицы:

```sql
SHOW PARTITIONS hw3.sales;
SELECT * FROM hw3.sales WHERE sale_date='2026-10-01' LIMIT 20;
SELECT * FROM hw3.sales WHERE sale_date='2026-10-02' LIMIT 20;
DESCRIBE FORMATTED hw3.sales;
!quit
```

-> убеждаемся, что видны обе партиции sale_date=2026-10-01 и sale_date=2026-10-02, запросы по каждой дате возвращают свои 3 записи с корректными типами без NULL, а в DESCRIBE FORMATTED указаны EXTERNAL_TABLE, путь в HDFS и numPartitions = 2

После выхода из `beeline` проверяем физическое расположение партиций напрямую в HDFS:

```bash
hdfs dfs -ls -R /user/hive/warehouse/hw3_sales
hdfs dfs -cat '/user/hive/warehouse/hw3_sales/sale_date=2026-10-01/*'
hdfs dfs -cat '/user/hive/warehouse/hw3_sales/sale_date=2026-10-02/*'
```

-> смотрим со стороны самого HDFS -- внутри /user/hive/warehouse/hw3_sales реально появились две папки sale_date=2026-10-01 и sale_date=2026-10-02 с нашими CSV-файлами внутри

## Шаг 13. Проверить одновременную работу двух клиентов — [два терминала на своем компьютере]

Открываем **два отдельных окна терминала** на своем компьютере и в обоих заходим на `edge`:

- Локальный стенд Vagrant (из папки `hw1/lab`): `ssh -i keys\user_key team@10.17.0.10`
- Выданный кластер: `ssh -i "<private_ssh>" team@178.236.27.243`

В каждом из двух окон переключаемся в `hadoop` и открываем живую сессию `beeline` к базе `hw3`:

```bash
sudo -i -u hadoop
```

```bash
source ~/.profile
beeline -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p ''
```

Не закрывая ни одно из окон, выполняем запрос в первом клиенте:

```sql
SELECT * FROM sales WHERE sale_date='2026-10-01' LIMIT 20;
```

И параллельно во втором клиенте:

```sql
SELECT * FROM sales WHERE sale_date='2026-10-02' LIMIT 20;
SHOW PARTITIONS sales;
```

-> проверяем главное требование задания -- многопользовательский non-embedded режим. обе сессии Beeline висят подключенными одновременно, параллельно читают данные и метаданные через общие HiveServer2, Metastore и PostgreSQL и не блокируют друг друга (в отличие от встроенного Derby). оставляем оба окна открытыми, чтобы увидеть обе сессии в Web UI на следующем шаге

## Шаг 14. Открыть SSH-туннель и проверить Web UI HiveServer2 — [свой компьютер]

В третьем отдельном терминале на своем компьютере пробрасываем порт `10002` до `edge`:

**Локальный стенд Vagrant, из папки `hw1/lab` (PowerShell):**

```powershell
ssh -i keys\user_key -N -o ExitOnForwardFailure=yes `
  -L 127.0.0.1:10002:127.0.0.1:10002 `
  team@10.17.0.10
```

**Выданный кластер -- Bash:**

```bash
ssh -i "<private_ssh>" -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:10002:127.0.0.1:10002 \
  team@178.236.27.243
```

-> так как Web UI HiveServer2 привязан к 127.0.0.1:10002 на самой edge-ноде, прокидываем к нему локальный порт 10002 через SSH-туннель, флаг -N держит только проброс порта без открытия удаленного шелла

Пока окно с туннелем открыто, переходим в браузере на `http://127.0.0.1:10002` и проверяем:

- страница **HiveServer2** открывается и показывает версию `4.0.0-alpha-2`
- в разделе **Active Sessions** отображаются две наши одновременные клиентские сессии Beeline из шага 13
- в списке запросов видны выполненные `SELECT` и `SHOW PARTITIONS`

После проверки закрываем сессии Beeline командой `!quit` в обоих окнах и останавливаем туннель через `Ctrl+C`.
