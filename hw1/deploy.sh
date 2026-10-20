#!/bin/bash
# HW1: развертывание HDFS (team-17) по шагам из INSTRUCTION.md
# NameNode, SecondaryNameNode и 3 DataNode
#
# запуск: на edge (team-17-en) под пользователем team
#     bash deploy.sh
# скрипт можно запускать сколько угодно раз: каждый шаг сначала смотрит "уже сделано?"
# и если да - пропускает. так можно перезапустить после ошибки и не сломать то что уже работает
# но если пользователь hadoop уже где-то есть, скрипт сначала остановится (стоп в шаге 1):
# вдруг кластер уже развернул кто-то из команды. если это твой же прошлый запуск:
#     FORCE=1 bash deploy.sh

# e - падаем на первой ошибке, u - необъявленная переменная тоже ошибка,
# pipefail - ошибка в середине трубы | тоже считается
set -euo pipefail

# ключ team для прыжка на внутренние ноды + опции ssh:
# accept-new - новую ноду запоминаем сами, без вопроса yes/no (как в шаге 5а с localhost)
# BatchMode - никогда не спрашивать пароль: если ключ не подошел, сразу ошибка, а не зависание
TEAM="-i $HOME/.ssh/team_internal -o StrictHostKeyChecking=accept-new -o BatchMode=yes"
# те же опции, но для hadoop (у него ключ по умолчанию, -i не нужен)
HS="-o StrictHostKeyChecking=accept-new -o BatchMode=yes"

step() { echo; echo "========== $* =========="; }


step "Шаг 1. Осмотреться [team@edge]"
# ---------------------------------------------------------------------------
ls -la ~/.ssh
cat /etc/hosts

# на всех нодах проверяем: имя машины, есть ли уже пользователь hadoop, работает ли сейчас
# какая-то программа java - не занимаю ли я чужой рабочий кластер
# (в конце true: на чистой ноде id и pgrep ничего не находят и возвращают ошибку, а это ок.
#  при повторном запуске тут будет видно наш же hadoop - это тоже ок)
for h in team-17-nn team-17-00 team-17-01; do
  echo "== $h"
  ssh $TEAM team@$h 'hostname; id hadoop; pgrep -a java; true'
done

# стоп: кластер один на всю команду. если hadoop уже есть хоть на одной ноде, а FORCE=1 не сказали -
# дальше не идем: иначе можно переписать конфиги и отформатировать NameNode поверх чужой установки
FOUND=""
if id hadoop >/dev/null 2>&1; then FOUND="team-17-en"; fi
for h in team-17-nn team-17-00 team-17-01; do
  if ssh $TEAM team@$h 'id hadoop >/dev/null 2>&1'; then FOUND="$FOUND $h"; fi
done
if [ -n "$FOUND" ] && [ "${FORCE:-0}" != 1 ]; then
  echo
  echo "СТОП: пользователь hadoop уже есть на:$FOUND - похоже, кластер уже кто-то развернул."
  echo "если это твой прошлый запуск: FORCE=1 bash deploy.sh"
  exit 1
fi


step "Шаг 2. /etc/hosts [team@edge]"
# ---------------------------------------------------------------------------
# на всякий случай, в случае нарушенной конфигурации /etc/hosts стопроцентно закидываем туда
# все адреса корректно. для начала корректные матчи закинем в переменную
NODES='10.17.0.10 team-17-en
10.17.0.11 team-17-nn
10.17.0.12 team-17-00
10.17.0.13 team-17-01'

# стираем все интересующие нас адреса из /etc/hosts, чтобы уже потом добавить корректные NODES
# (а то вдруг нам подкинули плохой файл! как на семинаре)
# повторный запуск тут ничего не ломает: удалили наши строки и дописали их же заново, дублей нет.
# сначала остальные ноды (стерли + дописали на каждой сразу)
for h in team-17-nn team-17-00 team-17-01; do
  ssh $TEAM team@$h 'for n in team-17-en team-17-nn team-17-00 team-17-01; do sudo sed -i "/\b$n\b/d" /etc/hosts; done'
  echo "$NODES" | ssh $TEAM team@$h 'sudo tee -a /etc/hosts >/dev/null'
done

# edge правим последней: с нее мы ходим на остальные ноды по именам, а имена она берет из своего /etc/hosts.
# если стереть их на edge первой, следующий ssh может получить Could not resolve hostname
for n in team-17-en team-17-nn team-17-00 team-17-01; do sudo sed -i "/\b$n\b/d" /etc/hosts; done
echo "$NODES" | sudo tee -a /etc/hosts >/dev/null

# делаем проверочку, все хосты должны смотреть на заданные в NODES адреса.
# в скрипте глазами не посмотришь, поэтому если имя где-то все еще на 127.x - останавливаемся.
# после правки система пару секунд помнит старый /etc/hosts (на edge тогда вылезают адреса fe80::...),
# поэтому сначала даем ей перечитать файл
sleep 5
getent hosts "$(hostname)"
if getent hosts "$(hostname)" | grep -q '^127\.'; then echo "ОШИБКА: $(hostname) смотрит на 127.x"; exit 1; fi
for h in team-17-nn team-17-00 team-17-01; do
  R=$(ssh $TEAM team@$h 'getent hosts $(hostname)')
  echo "$R"
  if grep -q '^127\.' <<< "$R"; then echo "ОШИБКА: $h смотрит на 127.x"; exit 1; fi
done


step "Шаг 3. Java [team@edge]"
# ---------------------------------------------------------------------------
# обновляем каталог ссылок на пакеты и ставим java 11 (конкретно нужен jdk). hadoop написан на java.
# для повторного запуска: dpkg -s отвечает "пакет уже стоит?" - если да, apt вообще не трогаем
dpkg -s openjdk-11-jdk-headless >/dev/null 2>&1 || { sudo apt update && sudo apt install -y openjdk-11-jdk-headless; }
java -version

# то же самое на всех остальных машинах: кластер hadoop (и соответственно java) должен быть установлен везде
for h in team-17-nn team-17-00 team-17-01; do
  ssh $TEAM team@$h 'dpkg -s openjdk-11-jdk-headless >/dev/null 2>&1 || { sudo apt update && sudo apt install -y openjdk-11-jdk-headless; }'
done

# java пишет версию в поток ошибок, поэтому 2>&1, тогда head увидит ее первой строчкой.
# ожидаем 3 одинаковых надписи с нужной нам версией джавы
for h in team-17-nn team-17-00 team-17-01; do ssh $TEAM team@$h 'java -version 2>&1 | head -1'; done


step "Шаг 4. Пользователь hadoop [team@edge]"
# ---------------------------------------------------------------------------
# добавляем пользователя hadoop (принцип минимальных прав), без пароля, --gecos "" - не спрашивать контакты.
# для повторного запуска: id hadoop отвечает "юзер уже есть?" - если да, adduser не зовем (иначе он упадет)
id hadoop >/dev/null 2>&1 || sudo adduser --disabled-password --gecos "" hadoop
for h in team-17-nn team-17-00 team-17-01; do
  ssh $TEAM team@$h 'id hadoop >/dev/null 2>&1 || sudo adduser --disabled-password --gecos "" hadoop'
done

# проверяем то что юзер создался на этой ноде, далее на всех остальных
id hadoop
for h in team-17-nn team-17-00 team-17-01; do ssh $TEAM team@$h 'id hadoop'; done


step "Шаг 5а. Ключ hadoop [hadoop@edge]"
# ---------------------------------------------------------------------------
# становимся hadoop на edge-ноде. в скрипте нельзя "зайти и печатать руками",
# поэтому команды для hadoop отдаем ему пачкой: sudo -i -u hadoop bash -s, а сами команды между <<'EOF' и EOF
# (кавычки вокруг 'EOF' - чтобы $ внутри раскрывал уже hadoop, а не team)
sudo -i -u hadoop bash -s <<'EOF'
set -euo pipefail

# создаем .ssh, права 700 - все для hadoop, остальным ничего
mkdir -p .ssh
chmod 700 .ssh
ls -ld .ssh

# создаем ключ ed25519. руками мы жали enter на вопросы, а в скрипте отвечаем заранее:
# -N "" - парольная фраза пустая (ее не ставим), -f - куда сохранить (то самое место по умолчанию).
# для повторного запуска: ключ создаем только если его еще нет. иначе новый ключ перезаписал бы старый,
# а на нодах лежал бы старый - и hadoop бы никуда не пускало
[ -f .ssh/id_ed25519 ] || ssh-keygen -t ed25519 -N "" -f .ssh/id_ed25519

# проверку "Permission denied до authorized_keys" тут не делаем: в скрипте она бы просто уронила все

# добавляем публичную часть ключа в authorized_keys, права 600.
# для повторного запуска: дописываем, только если ровно этой строки там еще нет (grep -qxF), иначе плодили бы дубли
touch .ssh/authorized_keys
grep -qxF "$(cat .ssh/id_ed25519.pub)" .ssh/authorized_keys || cat .ssh/id_ed25519.pub >> .ssh/authorized_keys
chmod 600 .ssh/authorized_keys
EOF
# exit не нужен: пачка команд закончилась, и мы снова team


step "Шаг 5б. Раскладка ключа [team@edge]"
# ---------------------------------------------------------------------------
# закидываем полный ключ юзера hadoop по всем нодам через team (для двусторонней связи между нодами).
# для повторного запуска: если hadoop уже заходит на ноду - ее пропускаем, там все уже лежит
for h in team-17-nn team-17-00 team-17-01; do
  if sudo -i -u hadoop ssh $HS $h true 2>/dev/null; then
    echo "$h: hadoop уже заходит, пропуск"
    continue
  fi
  echo "$h: раскладываю ключ"

  # временная папка hadoop_ssh в team (у team нет прав на hadoop/.ssh, поэтому такой костыль).
  # сначала удаляем старую, если прошлый запуск оборвался: иначе cp -r положил бы копию ВНУТРЬ нее
  # (та же ловушка, что с mv ниже). то же самое на ноде, перед scp
  sudo rm -rf /home/team/hadoop_ssh
  sudo cp -r /home/hadoop/.ssh /home/team/hadoop_ssh
  sudo chown -R team:team /home/team/hadoop_ssh
  ssh $TEAM team@$h 'rm -rf /home/team/hadoop_ssh'

  # rm -rf перед mv: вдруг там уже есть .ssh - 1) там может лежать не тот ключ 2) mv создаст .ssh/hadoop_ssh/.
  # && - чтобы ничего преждевременно не сломать: если scp не доехал, на ноде ничего не удаляется
  scp $TEAM -r /home/team/hadoop_ssh team@$h:/home/team/hadoop_ssh &&
  ssh $TEAM team@$h 'sudo rm -rf /home/hadoop/.ssh && sudo mv /home/team/hadoop_ssh /home/hadoop/.ssh && sudo chown -R hadoop:hadoop /home/hadoop/.ssh && sudo chmod 700 /home/hadoop/.ssh && sudo chmod 600 /home/hadoop/.ssh/id_ed25519 /home/hadoop/.ssh/authorized_keys'
done

# удаляем на edge временную папку: в ней приватный ключ hadoop в папке team - плохая практика
sudo rm -rf /home/team/hadoop_ssh


step "Шаг 5в. Проверка [hadoop@edge]"
# ---------------------------------------------------------------------------
# заходим на hadoop@team-17-* и смотрим валиден ли ключ.
# если где-то не пустит - set -e остановит скрипт прямо тут, дальше идти смысла нет.
# -n у ssh внутри пачки обязательно: команды пачки приходят bash'у на вход (stdin), и ssh без -n
# забрал бы себе все оставшиеся строки пачки - они бы просто не выполнились. поэтому во всех пачках ssh -n
sudo -i -u hadoop bash -s <<'EOF'
set -euo pipefail
for h in team-17-nn team-17-00 team-17-01; do ssh -n -o StrictHostKeyChecking=accept-new -o BatchMode=yes $h hostname; done
EOF


step "Шаг 6. Hadoop [hadoop@edge]"
# ---------------------------------------------------------------------------
sudo -i -u hadoop bash -s <<'EOF'
set -euo pipefail
S="-o StrictHostKeyChecking=accept-new -o BatchMode=yes"

# скачиваем hadoop 3.4.0 (архив). для повторного запуска: только если архива еще нет.
# качаем во временный .part и переименовываем только когда докачался: оборванная загрузка
# не притворится готовым архивом. если на dlcdn этой версии уже нет (там держат только свежие) - берем из archive.
# --show-progress: качается долго, без полоски кажется, что скрипт завис.
# --progress=bar:force:noscroll - полоска одной строкой, даже если вывод скрипта пишется в файл
if [ ! -f hadoop-3.4.0.tar.gz ]; then
  wget -q --show-progress --progress=bar:force:noscroll -O hadoop-3.4.0.tar.gz.part https://dlcdn.apache.org/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz ||
  wget -q --show-progress --progress=bar:force:noscroll -O hadoop-3.4.0.tar.gz.part https://archive.apache.org/dist/hadoop/common/hadoop-3.4.0/hadoop-3.4.0.tar.gz
  mv hadoop-3.4.0.tar.gz.part hadoop-3.4.0.tar.gz
fi

# разархивируем на edge (если еще не), потом переносим и разархивируем на остальных.
# для повторного запуска: ноду, где папка hadoop-3.4.0 уже есть, пропускаем
[ -d hadoop-3.4.0 ] || tar -xzf hadoop-3.4.0.tar.gz
for h in team-17-nn team-17-00 team-17-01; do
  if ssh -n $S $h '[ -d hadoop-3.4.0 ]'; then echo "$h: уже распакован, пропуск"; continue; fi
  echo "$h: переношу и распаковываю"
  scp $S hadoop-3.4.0.tar.gz $h:/home/hadoop/
  ssh -n $S $h 'tar -xzf hadoop-3.4.0.tar.gz'
done
EOF


step "Шаг 7. Переменные [hadoop@edge]"
# ---------------------------------------------------------------------------
sudo -i -u hadoop bash -s <<'EOF'
set -euo pipefail
S="-o StrictHostKeyChecking=accept-new -o BatchMode=yes"

# HADOOP_HOME, JAVA_HOME и hadoop в PATH. одинарные кавычки - чтобы в файл попал сам текст $PATH,
# а старый PATH не стираем, добавляя его в начало нового.
# для повторного запуска: add дописывает строку, только если ровно такой в .profile еще нет
add() { grep -qxF "$1" ~/.profile || echo "$1" >> ~/.profile; }
add 'export HADOOP_HOME=/home/hadoop/hadoop-3.4.0'
add 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64'
add 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin'

# исполняем .profile в этом шелле. set +u на время source: стандартный .profile/.bashrc
# трогает необъявленные переменные, и наш set -u из-за этого упал бы
set +u; source ~/.profile || true; set -u

# переносим .profile на остальные ноды (перезапись, повторять можно сколько угодно)
for h in team-17-nn team-17-00 team-17-01; do scp $S ~/.profile $h:/home/hadoop/; done

# проверяем версию через PATH
hadoop version
EOF


step "Шаг 8. Конфиги [hadoop@edge]"
# ---------------------------------------------------------------------------
sudo -i -u hadoop bash -s <<'EOF'
set -euo pipefail
S="-o StrictHostKeyChecking=accept-new -o BatchMode=yes"

# заходим в папку с конфигами хадупа. JAVA_HOME второй раз, теперь в hadoop-env.sh: его читает каждый
# скрипт хадупа, даже когда start-dfs.sh заходит на ноду через ssh и .profile не читается.
# для повторного запуска: дописываем, только если такой строки еще нет
cd ~/hadoop-3.4.0/etc/hadoop
grep -qxF 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' hadoop-env.sh ||
  echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> hadoop-env.sh

# вместо nano пишем файлы целиком через cat > файл <<'XML' ... XML.
# > перезаписывает файл, поэтому при повторном запуске получается ровно то же самое
cat > core-site.xml <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- адрес NameNode: сюда идут DataNode и клиенты -->
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://team-17-nn:9000</value>
    </property>
    <!-- данные не в /tmp, который чистится при перезагрузке -->
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/hadoop/hadoop_data</value>
    </property>
</configuration>
XML

cat > hdfs-site.xml <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- каждый блок в трех копиях, по одной на каждой DataNode -->
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
</configuration>
XML

# workers: где start-dfs.sh поднимет DataNode. каждое имя на своей строке, > перезаписывает (там лежал localhost)
printf '%s\n' team-17-nn team-17-00 team-17-01 > workers

# раскладываем все 4 файла на остальные ноды: конфиги должны быть одинаковые везде
for h in team-17-nn team-17-00 team-17-01; do
  scp $S hadoop-env.sh core-site.xml hdfs-site.xml workers $h:/home/hadoop/hadoop-3.4.0/etc/hadoop/
done
EOF


step "Шаг 9. Формат и запуск [hadoop@nn]"
# ---------------------------------------------------------------------------
# теперь команды идут уже на nn: hadoop с edge заходит туда по ssh и выполняет пачку.
# через ssh .profile не читается (см. шаг 8), поэтому пути полные/относительные, как в инструкции: bin/hdfs
sudo -i -u hadoop ssh $HS team-17-nn bash -s <<'EOF'
set -euo pipefail
cd ~/hadoop-3.4.0

# форматируем -- создаем каркас файловой системы на NameNode, появляется clusterID.
# повторное форматирование сломало бы кластер (новый clusterID, DataNode со старым не пустят),
# поэтому форматируем ТОЛЬКО если папки, которую создает форматирование, еще нет.
# -nonInteractive: если что-то уже есть, не спрашивать "перезаписать?", а отказаться
if [ -d ~/hadoop_data/dfs/name/current ]; then
  echo "NameNode уже отформатирована, пропуск"
else
  bin/hdfs namenode -format -nonInteractive
fi

# запускаем: на nn NameNode, SecondaryNameNode, DataNode; на нодах из workers - DataNode.
# для повторного запуска: уже работающие сервисы start-dfs.sh пропускает ("running as process"),
# но может при этом вернуть ошибку, поэтому || true. живой ли кластер, проверяем в шаге 10
sbin/start-dfs.sh || true

# jps на nn и на DataNode: ожидаем NameNode, SecondaryNameNode, DataNode / DataNode
echo "== team-17-nn"; jps
for h in team-17-00 team-17-01; do echo "== $h"; ssh -n -o StrictHostKeyChecking=accept-new $h jps; done
EOF


step "Шаг 10. Проверка [hadoop@nn]"
# ---------------------------------------------------------------------------
sudo -i -u hadoop ssh $HS team-17-nn bash -s <<'EOF'
set -euo pipefail
HDFS=~/hadoop-3.4.0/bin/hdfs

# отчет NameNode о том, кто к ней подключился: нужно Live datanodes (3).
# DataNode отмечаются не мгновенно после запуска, поэтому ждем до минуты (12 раз по 5 секунд)
for i in $(seq 1 12); do
  R=$($HDFS dfsadmin -report 2>/dev/null || true)
  if grep -q "Live datanodes (3)" <<< "$R"; then break; fi
  echo "DataNode еще не все, жду 5 секунд..."
  sleep 5
done
grep -E "Live datanodes|^Name:" <<< "$R" || echo "ОШИБКА: NameNode не ответила на dfsadmin -report"
grep -q "Live datanodes (3)" <<< "$R" || { echo "ОШИБКА: живых DataNode не 3. смотри логи ниже"; }

# кто слушает порт 9000: должно быть 10.17.0.11:9000, а не 127.0.0.1
ss -tln | grep 9000

# ищем ошибки в логах. если все хорошо - вывод пустой, но grep, ничего не найдя, возвращает ошибку
# и set -e уронил бы скрипт. поэтому || echo.
# ssh -n обязательно (см. шаг 5в): без него ssh съедал остаток пачки, и safemode/fsck ниже не выполнялись
grep -E "ERROR|FATAL" ~/hadoop-3.4.0/logs/*.log || echo "team-17-nn: ERROR/FATAL нет"
for h in team-17-00 team-17-01; do
  ssh -n -o StrictHostKeyChecking=accept-new $h 'grep -E "ERROR|FATAL" ~/hadoop-3.4.0/logs/*.log' || echo "$h: ERROR/FATAL нет"
done
grep -q "Live datanodes (3)" <<< "$R" || exit 1

# проверяем что кластер реально хранит данные: папка, файл, fsck.
# safemode wait: сразу после старта NameNode может быть в safe mode (только чтение), ждем пока выйдет.
# для повторного запуска: mkdir -p не ругается, если /test уже есть, а put -f перезаписывает файл
$HDFS dfsadmin -safemode wait
$HDFS dfs -mkdir -p /test
$HDFS dfs -put -f ~/.profile /test/
F=$($HDFS fsck /test -files -blocks -locations 2>/dev/null)
echo "$F"
# должно быть replication=3, три адреса DataNode и HEALTHY
grep -q "is HEALTHY" <<< "$F" || { echo "ОШИБКА: fsck не HEALTHY"; exit 1; }
EOF


echo
echo "Готово. Кластер поднят: 3 живых DataNode, fsck HEALTHY."
echo "Веб-интерфейс NameNode - туннель со своего компьютера (шаг 10 в INSTRUCTION.md):"
echo "  стенд Vagrant:     ssh -i keys\\user_key -L 9870:10.17.0.11:9870 team@10.17.0.10"
echo "  выданный кластер:  ssh -i <путь к ключу> -L 9870:10.17.0.11:9870 team@178.236.27.243"
echo "и открыть http://localhost:9870: Overview - Live Nodes: 3, Dead Nodes: 0; Datanodes - три ноды In service"
