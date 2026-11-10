# HW2: развертывание YARN (team-17)

**Стенд:** четыре ВМ Vagrant из ДЗ1 с теми же именами, адресами и пользователями, что на выданном кластере. На ВМ уже установлены Hadoop 3.4.0 и Java 11, настроены HDFS и SSH между нодами. Hadoop находится в `/home/hadoop/hadoop-3.4.0`. На выданном кластере команды те же; отличаются вход на edge (шаг 0) и SSH-туннель (шаг 8).

| Хост | IP | Роли после ДЗ2 |
|---|---|---|
| `team-17-en` | `10.17.0.10` | edge: клиент Hadoop, настройка и доступ по SSH |
| `team-17-nn` | `10.17.0.11` | NameNode, SecondaryNameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer |
| `team-17-00` | `10.17.0.12` | DataNode, NodeManager |
| `team-17-01` | `10.17.0.13` | DataNode, NodeManager |

В заголовках указаны пользователь и нода. После входа через `ssh`, смены пользователя или `exit` дожидаемся нового приглашения, затем вводим следующий блок.

## Шаг 0. Зайти на edge — [свой компьютер]

- Локальный стенд Vagrant (из папки `hw1/lab`): `ssh -i keys\user_key team@10.17.0.10`
- Выданный кластер: `ssh -i "<private_ssh>" team@178.236.27.243`

`<private_ssh>` заменяется путем к приватному SSH-ключу. Следующие команды выполняются после появления приглашения `team@team-17-en`.


## Шаг 1. Проверить основу и запустить HDFS — [team@edge] → [hadoop@edge]

```bash
sudo -i -u hadoop
```

-> заходим под пользователем hadoop, соблюдаем порядок проекта и правильное распределение ролей; все что связанное с hadoop -- делаем под пользователем hadoop

Дожидаемся приглашения `hadoop@team-17-en`.

```bash
whoami
hostname
source ~/.profile
hadoop version
java -version
```

-> проверяем наши результаты из дз1, установленный hadoop; что-то не так или не установлено -> troubleshooting и/или инструкция или скрипт из дз1

Если HDFS остановлен, запускаем уже установленный кластер:

```bash
ssh team-17-nn '/home/hadoop/hadoop-3.4.0/sbin/start-dfs.sh'
```

```bash
hdfs dfsadmin -report
hdfs dfsadmin -safemode get
```

-> проверяем запуск; все наши датаноды должны быть живы (3 штуки); Safe mode is OFF - ожидаем такой вывод 2-ой команды -- HDFS вышел из режима только для чтения. если после запуска еще ON, ждем и повторяем проверку; после OFF идем дальше, тогда сможем продолжить установку

## Шаг 2. Сохранить конфиги и проверить workers — [hadoop@edge]

```bash
cp -a ~/hadoop-3.4.0/etc/hadoop ~/hadoop-config-before-hw2-$(date +%Y%m%d-%H%M%S)
```

-> бэкапим конфиги hadoop, чтобы при неудачных изменениях вернуть предыдущие настройки

```bash
for h in team-17-nn team-17-00 team-17-01; do
  ssh "$h" 'cp -a ~/hadoop-3.4.0/etc/hadoop ~/hadoop-config-before-hw2-$(date +%Y%m%d-%H%M%S)'
done
```

-> бэкапим на остальных нодах тоже

```bash
cd ~/hadoop-3.4.0/etc/hadoop
cat workers
```

-> проверяем, что workers настроен правильно, он нужен будет YARN

Ожидаем уже настроенный в ДЗ1 список:

```text
team-17-nn
team-17-00
team-17-01
```

## Шаг 3. Настроить YARN — [hadoop@edge]

```bash
nano yarn-site.xml
```
(или vim, если вам так удобнее)
Заполняем следующим образом:

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>team-17-nn</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
```

Сохраняем файл: (`Ctrl+O`, Enter, затем выходим: `Ctrl+X`)
Здесь мы канонично настроили важные для работы YARN параметры:
- `yarn.resourcemanager.hostname`: нода, на которой будет установлен ResourceManager (в нашем небольшом кластере размещаем его на team-17-nn вместе с NameNode)
- `yarn.nodemanager.aux-services`: выставляем в aux-services (доп. сервисы) mapreduce_shuffle. Это необходимо выставить, чтобы подготовить кластер к MapReduce заданиям
- `yarn.nodemanager.env-whitelist`: используем список переменных окружения из официального примера настройки MapReduce. Тоже необходимо выставить
## Шаг 4. Настроить MapReduce и HistoryServer — [hadoop@edge]

```bash
nano mapred-site.xml
```

Вставляем следующее:

```xml
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*,/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>team-17-nn:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>team-17-nn:19888</value>
  </property>
</configuration>
```

Сохраняем файл и выходим из nano.
Здесь мы настроили непосредственно MapReduce:
- `mapreduce.framework.name`: фреймворк ответственный за MapReduce -- YARN. Он выделяет ресурсы и организует запуск процессов задания
- `mapreduce.application.classpath`: указываем, где Java искать библиотеки MapReduce
- `mapreduce.jobhistory.address`: адрес HistoryServer который хранит информацию о завершенных MapReduce заданиях. Нода -- team-17-nn, порт для обращений программ -- 10020
- `mapreduce.jobhistory.webapp.address`: так же настраиваем адрес для веб-интерфейса HistoryServer на нашей NameNode.

## Шаг 5. Раздать конфиги — [hadoop@edge]

Рабочий каталог -- `/home/hadoop/hadoop-3.4.0/etc/hadoop`.

```bash
for h in team-17-nn team-17-00 team-17-01; do
  scp yarn-site.xml mapred-site.xml \
    "$h:/home/hadoop/hadoop-3.4.0/etc/hadoop/" || break
done
```

-> для стабильной работы кластера все вышеупомянтуые конфиги должны быть на всех нодах, копируем их туда. при ошибке копирования цикл прекращается; перед запуском службы конфиги должны попасть на все три ноды

`hadoop-env.sh`, `workers` и конфиги HDFS уже настроены и разложены после ДЗ1. В ДЗ2 меняем два XML

## Шаг 6. Запустить YARN и HistoryServer — [hadoop@edge] → [hadoop@nn]

```bash
ssh team-17-nn
```

-> заходим на неймноду, непосредственно на "управляющую" кластером ноду

**Следующий блок выполняем после появления приглашения `hadoop@team-17-nn`.**

```bash
cd ~/hadoop-3.4.0
sbin/start-yarn.sh
```

-> запускаем на ней YARN

```bash
bin/mapred --daemon start historyserver
```

-> запускаем так же на ней historyserver. он будет принимать обращения по адресам и портам из mapred-site.xml

## Шаг 7. Проверить службы — [hadoop@nn]

```bash
jps
ssh team-17-00 /usr/bin/jps
ssh team-17-01 /usr/bin/jps
```

-> проверяем, что все поднялось на всех рабочих нодах. На nn должны увидеть NameNode, SecondaryNameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer. На 00 и 01 -- DataNode и NodeManager. Также в каждом выводе будет Jps -- сама программа проверки. Порядок строк может отличаться от ноде к ноде

```bash
bin/yarn node -list -all
```

-> спрашиваем у ResourceManager список известных ему NodeManager. ожидаем наши три ноды -- nn, 00, 01 -- в состоянии RUNNING

## Шаг 8. Открыть SSH-туннель — [свой компьютер]

Туннель запускаем в отдельном терминале на своем компьютере

**Локальный стенд Vagrant, из папки `hw1/lab`:**

```powershell
ssh -i keys\user_key -N -o ExitOnForwardFailure=yes `
  -L 127.0.0.1:9870:10.17.0.11:9870 `
  -L 127.0.0.1:9868:10.17.0.11:9868 `
  -L 127.0.0.1:9864:10.17.0.11:9864 `
  -L 127.0.0.1:9865:10.17.0.12:9864 `
  -L 127.0.0.1:9866:10.17.0.13:9864 `
  -L 127.0.0.1:8088:10.17.0.11:8088 `
  -L 127.0.0.1:8042:10.17.0.11:8042 `
  -L 127.0.0.1:8043:10.17.0.12:8042 `
  -L 127.0.0.1:8044:10.17.0.13:8042 `
  -L 127.0.0.1:19888:10.17.0.11:19888 `
  team@10.17.0.10
```

**Выданный кластер -- Bash:**

```bash
ssh -i "<private_ssh>" -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:9870:10.17.0.11:9870 \
  -L 127.0.0.1:9868:10.17.0.11:9868 \
  -L 127.0.0.1:9864:10.17.0.11:9864 \
  -L 127.0.0.1:9865:10.17.0.12:9864 \
  -L 127.0.0.1:9866:10.17.0.13:9864 \
  -L 127.0.0.1:8088:10.17.0.11:8088 \
  -L 127.0.0.1:8042:10.17.0.11:8042 \
  -L 127.0.0.1:8043:10.17.0.12:8042 \
  -L 127.0.0.1:8044:10.17.0.13:8042 \
  -L 127.0.0.1:19888:10.17.0.11:19888 \
  team@178.236.27.243
```

`<private_ssh>` заменяется путем к приватному SSH-ключу. Для подключения к выданному кластеру используем первый блок, заменив `keys\user_key` на путь к ключу и последний адрес `team@10.17.0.10` на `team@178.236.27.243`. Внутренние адреса после `-L` остаются прежними.

-> здесь мы открываем SSH-туннели под выведенные нашим кластером адреса на различные веб-интерфейсы

Окно с туннелем оставляем открытым. В обоих случаях веб-интерфейсы открываются по локальным адресам из следующего шага

## Шаг 9. Проверить веб-интерфейсы — [браузер на своем компьютере]

| Служба | Адрес в браузере | Куда передается запрос |
|---|---|---|
| NameNode | `http://127.0.0.1:9870` | nn:9870 |
| SecondaryNameNode | `http://127.0.0.1:9868` | nn:9868 |
| DataNode на nn | `http://127.0.0.1:9864` | nn:9864 |
| DataNode на 00 | `http://127.0.0.1:9865` | 00:9864 |
| DataNode на 01 | `http://127.0.0.1:9866` | 01:9864 |
| ResourceManager | `http://127.0.0.1:8088` | nn:8088 |
| NodeManager на nn | `http://127.0.0.1:8042` | nn:8042 |
| NodeManager на 00 | `http://127.0.0.1:8043` | 00:8042 |
| NodeManager на 01 | `http://127.0.0.1:8044` | 01:8042 |
| JobHistoryServer | `http://127.0.0.1:19888` | nn:19888 |

заходим на каждый адрес и проверяем следующее:

- в NameNode -- Live Nodes: 3, Dead Nodes: 0, во вкладке DataNodes все три ноды в состоянии In service
- в ResourceManager -- Active Nodes: 3, в разделе Nodes все три NodeManager имеют статус RUNNING
- страницы всех DataNode и NodeManager открываются -- проверяем каждый адрес из таблицы
- SecondaryNameNode тоже открывается и показывает информацию о своей работе
- JobHistoryServer открывается. список заданий пока пустой -- это нормально, мы еще не запускали MapReduce-задачи

В случае несовпадений занимаемся траблшутингом смотря на соответствующие пункты инструкции.
