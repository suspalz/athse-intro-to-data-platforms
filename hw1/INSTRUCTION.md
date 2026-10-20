# HW1: развертывание HDFS (team-17)

Кластер из 4 машин: NameNode, SecondaryNameNode и 3 DataNode.

> **Где это делалось:** для удобства все шаги выполнены и проверены на **локальном стенде** (4 виртуалки в Vagrant, лежит в папке `lab/`, как поднять - в `lab/README.md`), который имитирует выданный командный кластер: те же имена хостов, адреса и пользователи (`team`, `hadoop`). На выданном кластере команды те же, отличается только то, как попасть на edge (шаг 0 и туннель в конце шага 10).

| Хост | Адрес | Роль |
|---|---|---|
| `team-17-en` | 10.17.0.10 | edge: отсюда всё раскатываем |
| `team-17-nn` | 10.17.0.11 | NameNode, SecondaryNameNode, DataNode |
| `team-17-00` | 10.17.0.12 | DataNode |
| `team-17-01` | 10.17.0.13 | DataNode |

Где выполняется команда, написано в заголовке шага: **[team@edge] aka [team@team-17-en]**, **[hadoop@edge] aka [hadoop@team-17-en]**, **[hadoop@nn] aka [hadoop@team-17-nn]**.

---

## Быстрый вариант: скрипт deploy.sh — [свой компьютер] → [team@edge]

все шаги 1–10 ниже собраны в один скрипт `deploy.sh`: он делает ровно то же самое, только сам (без nano и ручного ввода)

---

## Шаг 0. Зайти на edge — [свой компьютер]

заходим на edge под team по своему ssh ключу. снаружи доступна только edge, все остальные ноды - уже с нее
- стенд Vagrant (из папки `lab`): `ssh -i keys\user_key team@10.17.0.10`
- выданный кластер: `ssh -i <private_ssh> team@178.236.27.243`

## Шаг 1. Осмотреться — [team@edge]

```bash
ls -la ~/.ssh
```
смотрим, что лежит в папке .ssh, какие у нее права

```bash
cat /etc/hosts
```
выводим, что сейчас лежит в /etc/hosts, нужно, чтобы осмотреть, не образовалось ли **петли у ноды на саму себя** (как было на семинаре)

```bash
for h in team-17-nn team-17-00 team-17-01; do
  echo "== $h"
  ssh -i ~/.ssh/team_internal team@$h 'hostname; id hadoop; pgrep -a java'
done
```
на всех нодах проверяем: имя машины, есть ли уже пользователь hadoop, работает ли сейчас какая-то программа java - **не занимаю ли я чужой рабочий кластер**

## Шаг 2. /etc/hosts — [team@edge]

```bash
NODES='10.17.0.10 team-17-en
10.17.0.11 team-17-nn
10.17.0.12 team-17-00
10.17.0.13 team-17-01'
```
на всякий случай, в случае нарушенной конфигурации /etc/hosts стопроцентно закидываем туда все адреса корректно. для начала корректные матчи закинем в переменную

```bash
for h in team-17-nn team-17-00 team-17-01; do
  ssh -i ~/.ssh/team_internal team@$h 'for n in team-17-en team-17-nn team-17-00 team-17-01; do sudo sed -i "/\b$n\b/d" /etc/hosts; done'
done
for h in team-17-nn team-17-00 team-17-01; do
  echo "$NODES" | ssh -i ~/.ssh/team_internal team@$h 'sudo tee -a /etc/hosts'
done
```
стираем все интересующие нас адреса из /etc/hosts, чтобы уже потом добавить корректные NODES (а то вдруг нам подкинули плохой файл!). сначала на остальных нодах

```bash
for n in team-17-en team-17-nn team-17-00 team-17-01; do sudo sed -i "/\b$n\b/d" /etc/hosts; done
echo "$NODES" | sudo tee -a /etc/hosts
```
**edge правим последней**: с нее мы ходим на остальные ноды по именам, а имена она берет из своего /etc/hosts. если стереть их на edge первой, следующий `ssh team@team-17-00` может получить `Could not resolve hostname`

```bash
getent hosts $(hostname)
for h in team-17-nn team-17-00 team-17-01; do ssh -i ~/.ssh/team_internal team@$h 'getent hosts $(hostname)'; done
```
делаем проверочку, **все хосты должны смотреть на заданные в NODES адреса**. если у team-17-en вместо `10.17.0.10` вылезли адреса `fe80::...` - система еще не перечитала файл (пару секунд помнит старый), повторяем проверку чуть позже

## Шаг 3. Java — [team@edge]

```bash
sudo apt update && sudo apt install -y openjdk-11-jdk-headless
java -version
```
на edge обновляем каталог ссылок на пакеты, чтобы пакет с джавой нашелся и устанавливаем этот пакет - java 11 (конкретно нужен **jdk**). hadoop написан на java, поэтому он нам нужен. проверяем версию после установки.

```bash
for h in team-17-nn team-17-00 team-17-01; do ssh -i ~/.ssh/team_internal team@$h 'sudo apt update && sudo apt install -y openjdk-11-jdk-headless'; done
```
то же самое на всех остальных машинах: кластер hadoop (и соответственно java) должен быть установлен везде

```bash
for h in team-17-nn team-17-00 team-17-01; do ssh -i ~/.ssh/team_internal team@$h 'java -version 2>&1 | head -1'; done
```
после установки проверяем наличие джавы, конкретно ее версию. **Важно:** Java записывает свою версию по старинке в **поток ошибок**, а head -n берет строки из стандартного потока -> нам нужно влить поток ошибок в стандартный (`2>&1`), тогда head увидит эту версию (она идет первой строкой). ожидаем **3 одинаковых надписи** с нужной нам версией джавы

## Шаг 4. Пользователь hadoop — [team@edge]

```bash
sudo adduser --disabled-password --gecos "" hadoop
for h in team-17-nn team-17-00 team-17-01; do ssh -i ~/.ssh/team_internal team@$h 'sudo adduser --disabled-password --gecos "" hadoop'; done
```
добавляем пользователя hadoop для безопасности (по **принципу минимальных прав** минимизируем урон в случае взлома hadoop) и удобства, выключаем доступ через пароль (только через `sudo -i -u hadoop` из team и через ssh ключ, который мы создадим), `--gecos` - для краткости (не будет спрашивать наши контактные данные) `""` - вместо них.

```bash
id hadoop
for h in team-17-nn team-17-00 team-17-01; do ssh -i ~/.ssh/team_internal team@$h 'id hadoop'; done
```
проверяем то что юзер создался на этой ноде, далее на всех остальных

## Шаг 5а. Ключ hadoop — [team@edge] → [hadoop@edge]

```bash
sudo -i -u hadoop
```
становимся hadoop на edge-ноде

```bash
mkdir -p .ssh
chmod 700 .ssh
ls -ld .ssh
```
создаем папку .ssh в домашней директории юзера hadoop, далее меняем права этой папки (по принципу минимальных прав) на **700** - максимум для самого hadoop (чтение+запись+исполнение) и остальным прав не даем. проверяем права этой папки через `ls -ld` -- должно быть `drwx------`.

```bash
ssh-keygen -t ed25519
```
начинаем создавать ssh ключ для hadoop. `-t ed25519` (нынешний стандарт), далее спросят куда сохранить (по умолчанию `.ssh/id_ed25519` - мы ничего не меняем, жмем enter) и попросит парольную фразу, **ее не ставим**, иначе потом постоянно ее придется вводить и кластер нормально не будет работать. появится две части ключа для пользователя hadoop в `~/.ssh/`: `id_ed25519.pub` - публичная его часть и `id_ed25519` - приватная.

```bash
ssh -o StrictHostKeyChecking=accept-new localhost hostname
```
можем проверить что не можем зайти на эту же машину через hadoop - ключ создан, но не авторизован (будет `Permission denied`)

```bash
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```
добавляем публичную часть ключа в authorized_keys и меняем по принципу минимальных прав на **600** (читать + писать), authorized_keys неисполняем (можно и 700, но это не лучшая практика)

```bash
exit
```
выходим из пользователя hadoop на edge в пользователя team

## Шаг 5б. Раскладка ключа — [team@edge]

```bash
sudo cp -r /home/hadoop/.ssh /home/team/hadoop_ssh
sudo chown -R team:team /home/team/hadoop_ssh
```
создаем временную папку hadoop_ssh прямо в team (копируем в нее ключи пользователя hadoop) и передаем права на эту папку также team. У team сейчас нет прав на папку hadoop/.ssh (мы сами это настраивали) поэтому приходится делать такой костыль

```bash
for h in team-17-nn team-17-00 team-17-01; do
  scp -i ~/.ssh/team_internal -r /home/team/hadoop_ssh team@$h:/home/team/hadoop_ssh &&
  ssh -i ~/.ssh/team_internal team@$h 'sudo rm -rf /home/hadoop/.ssh && sudo mv /home/team/hadoop_ssh /home/hadoop/.ssh && sudo chown -R hadoop:hadoop /home/hadoop/.ssh && sudo chmod 700 /home/hadoop/.ssh && sudo chmod 600 /home/hadoop/.ssh/id_ed25519 /home/hadoop/.ssh/authorized_keys'
done
```
теперь с помощью ключа team_internal (у которого есть доступ ко всем нодам) из юзера team мы закидываем полный ключ юзера hadoop по всем нодам. делаем это для **двусторонней связи между нодами**, которая нужна hadoop. на всякий случай пишем удаление папки hadoop/.ssh на каждой ноде - вдруг там она уже есть, в таком случае:
1) тогда там может уже лежать не тот ключ
2) mv создаст папку `/home/hadoop/.ssh/hadoop_ssh/` - ключи не будут лежать в ожидаемом месте, работать не будет.

далее передаем права на папку и ключ пользователю hadoop и только ему. права ставим на все файлы внутри для безопасности включая authorized_keys.

> **ВАЖНО**: мы используем `&&` между командами прежде всего ради того, чтобы ничего преждевременно не сломать. видим, как часто следующая команда ждет успешного выполнения предыдущей: если scp не доехал, на ноде ничего не удаляется; если упал mv, дальше не трогаем права несуществующей папки

```bash
rm -rf /home/team/hadoop_ssh
```
удаляем на edge ноде временную папку hadoop_ssh, она больше не нужна, еще в ней лежит приватный ключ от hadoop в папке team -- **ключ в чужой папке, плохая практика**.

## Шаг 5в. Проверка — [hadoop@edge]

```bash
sudo -i -u hadoop
for h in team-17-nn team-17-00 team-17-01; do ssh $h hostname; done
```
проверяем, заходим на hadoop@team-17 и смотрим валиден ли ключ

## Шаг 6. Hadoop — [hadoop@edge]

```bash
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
```
скачиваем непосредственно hadoop (версии **3.4.0**) на edge ноду. он поставляется как архив

```bash
for h in team-17-nn team-17-00 team-17-01; do scp hadoop-3.4.0.tar.gz $h:/home/hadoop/; done
```
переносим архив с hadoop на остальные ноды. здесь выбран вариант перенести с edge-ноды на остальные: в теории можно было и отдельно скачать на каждой одновременно.

```bash
tar -xzf hadoop-3.4.0.tar.gz
for h in team-17-nn team-17-00 team-17-01; do ssh $h 'tar -xzf hadoop-3.4.0.tar.gz'; done
```
разархивируем скачанный hadoop 3.4.0 на edge ноде и на других

## Шаг 7. Переменные — [hadoop@edge]

```bash
echo 'export HADOOP_HOME=/home/hadoop/hadoop-3.4.0' >> ~/.profile
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.profile
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.profile
```
для корректной работы hadoop нужны правильно настроенные переменные окружения `HADOOP_HOME`, `JAVA_HOME` (для джавы), и добавить hadoop в `PATH`. Нужно, чтобы хадуп знал, где лежат ключевые файлы. используем **одинарные кавычки** для корректной подстановки и не забываем **не стереть старый PATH**, добавляя его в начало нового PATH

```bash
source ~/.profile
for h in team-17-nn team-17-00 team-17-01; do scp ~/.profile $h:/home/hadoop/; done
hadoop version
```
исполняем .profile **в этом шелле**, чтобы переменные сразу вынеслись в окружение. переносим .profile на все остальные ноды. проверяем версию через PATH. Теперь можем спокойно пользоваться hadoop через терминал на любой ноде

## Шаг 8. Конфиги — [hadoop@edge]

```bash
cd ~/hadoop-3.4.0/etc/hadoop
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> hadoop-env.sh
```
заходим в папку с конфигами хадупа (`etc/hadoop` внутри `hadoop-3.4.0`), далее кладем JAVA_HOME в hadoop-env.sh - так сам хадуп будет знать, где находится Java. hadoop-env.sh читает каждый скрипт хадупа при запуске, даже когда start-dfs.sh заходит на ноду через `ssh нода 'команда'` и .profile не читается. **поэтому JAVA_HOME второй раз**

```bash
nano core-site.xml      # fs.defaultFS + hadoop.tmp.dir
nano hdfs-site.xml      # dfs.replication = 3
printf '%s\n' team-17-nn team-17-00 team-17-01 > workers
```

`core-site.xml` (между `<configuration>` и `</configuration>`):
```xml
<property>
    <name>fs.defaultFS</name>
    <value>hdfs://team-17-nn:9000</value>
</property>
<property>
    <name>hadoop.tmp.dir</name>
    <value>/home/hadoop/hadoop_data</value>
</property>
```

`hdfs-site.xml`:
```xml
<property>
    <name>dfs.replication</name>
    <value>3</value> <!-- можем выбрать значение -->
</property>
```
xml это формат на тегах: `<name>` открыли, `</name>` закрыли, между ними значение. `<configuration>` это корень, внутри куча `<property>`, у каждого пара: `name` (ключ) и `value` (значение). хадуп читает эти файлы при старте, а все чего тут нет берет **по умолчанию** (дефолты зашиты внутри самого хадупа)
- `fs.defaultFS` = `hdfs://team-17-nn:9000`: адрес файловой системы по умолчанию. `hdfs://` протокол, `team-17-nn` где живет NameNode, `9000` порт. по нему DataNode регистрируются у NameNode, и туда же идет любой клиент (`hdfs dfs -ls /`). без этого дефолт `file:///`, то есть просто локальный диск, и никакого кластера
- `hadoop.tmp.dir` = `/home/hadoop/hadoop_data`: базовая папка под данные, от нее считаются `dfs/name` (метаданные NameNode) и `dfs/data` (блоки на DataNode). по дефолту это `/tmp/hadoop-hadoop`, а **`/tmp` чистится при перезагрузке**: ребутнул ноду и NameNode потеряла метаданные, все заново форматировать
- `dfs.replication` = `3`: каждый блок хранится в трех копиях на разных DataNode. у нас ровно 3 DataNode, значит по копии на каждой: упала одна, данные живы. дефолт тоже 3, но пишем явно, чтобы было видно

`workers`: список нод, где start-dfs.sh по ssh поднимет DataNode. nn тоже в списке, поэтому DataNode есть и на ней (итого 3). `printf '%s\n'` печатает каждое слово на отдельной строке, `>` перезаписывает файл целиком (там лежал `localhost`)

```bash
for h in team-17-nn team-17-00 team-17-01; do
  scp hadoop-env.sh core-site.xml hdfs-site.xml workers $h:/home/hadoop/hadoop-3.4.0/etc/hadoop/
done
```
раскладываем все 4 файла в `etc/hadoop` на остальные ноды. **конфиги должны быть одинаковые везде**: если на DataNode не будет `fs.defaultFS`, она не узнает, куда стучаться к NameNode. мы уже стоим в `etc/hadoop`, поэтому файлы по коротким именам, а куда класть пишем после двоеточия

## Шаг 9. Формат и запуск — [hadoop@nn]

```bash
ssh team-17-nn
cd ~/hadoop-3.4.0
bin/hdfs namenode -format
```
после шага 8 мы все еще на edge, поэтому сначала **переходим на nn** (под hadoop, ключ по умолчанию). форматировать надо именно там: NameNode живет на nn
форматируем -- создаем непосредственно каркас нашей распределенной файловой системы на NameNode. у нас появляется **clusterID**, по которому ориентируются все ноды, поэтому **в случае повторного форматирования кластер может сломаться**. в выводе должно быть `has been successfully formatted`, а в последней строке `Shutting down NameNode at team-17-nn/10.17.0.11`. если там `127.0.1.1` - шаг 2 не сработал, возвращаемся к нему

```bash
sbin/start-dfs.sh
```
запускаем непосредственно процесс кластера, активизируем все ноды. на nn - должен подняться NameNode, SecondaryNameNode, DataNode; на нодах из workers - DataNode. распределенная система должна оказаться полностью поднята в результате

```bash
jps
ssh team-17-00 jps
ssh team-17-01 jps
```
проверяем на NameNode и обеих DataNode jps - джава-процессы, там должны увидеть поднятый hadoop-процесс: на nn - `NameNode`, `SecondaryNameNode`, `DataNode`; а на 00 и 01 - `DataNode`

## Шаг 10. Проверка — [hadoop@nn]

```bash
hdfs dfsadmin -report | grep -E "Live datanodes|^Name:"
```
отчет NameNode о том, кто к ней подключился. grep -E оставляет только строки с "Live datanodes" или с адресами нод. должно быть **`Live datanodes (3)`** и три адреса .11/.12/.13 - значит все DataNode отметились у NameNode

```bash
ss -tln | grep 9000
```
смотрим, кто слушает порт 9000 (t - tcp, l - listening, n - цифрами, без имен). должно быть **`10.17.0.11:9000`** - NameNode ждет подключений на сетевом адресе

```bash
grep -E "ERROR|FATAL" ~/hadoop-3.4.0/logs/*.log
for h in team-17-00 team-17-01; do ssh $h 'grep -E "ERROR|FATAL" ~/hadoop-3.4.0/logs/*.log'; done
```
ищем ошибки в логах хадупа на nn и на DataNode. если все хорошо - **вывод пустой**. если что-то нашлось, в строке обычно сразу причина (например `Incompatible clusterIDs`). исключение: после остановки кластера (`sbin/stop-dfs.sh`) в логах появляются строки `ERROR ... RECEIVED SIGNAL 15: SIGTERM` - это штатное выключение сервисов, а не ошибка

```bash
hdfs dfs -mkdir /test
hdfs dfs -put ~/.profile /test/
hdfs fsck /test -files -blocks -locations
```
отчет показывает только что ноды живы, а тут проверяем что кластер **реально хранит данные**: создаем папку, кладем туда файл, fsck показывает его блоки и где они лежат. должно быть `replication=3`, три адреса DataNode и **`Status: HEALTHY`** - копия на каждой ноде

**[На своем компьютере (тут конкретно для сетапа через Vagrant)]**
```powershell
ssh -i keys\user_key -L 9870:10.17.0.11:9870 team@10.17.0.10
```
туннель: `-L` - local forward. `9870:10.17.0.11:9870` читаем как "мой порт 9870 -> через edge -> на 10.17.0.11 порт 9870". внутренняя сеть 10.17.0.x снаружи не видна, а edge в нее пускает. открываем у себя `localhost:9870` и видим веб-интерфейс NameNode

**[На своем компьютере (выданный командный кластер)]**
```bash
ssh -i <путь к ключу> -L 9870:10.17.0.11:9870 team@178.236.27.243
```

пока сессия с туннелем открыта, открываем `http://localhost:9870`:
- вкладка **Overview** - `Live Nodes: 3`, `Dead Nodes: 0`
- вкладка **Datanodes** - три ноды в состоянии `In service`
