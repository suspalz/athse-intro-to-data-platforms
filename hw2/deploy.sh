#!/bin/bash
# HW2: развертывание YARN (team-17) по шагам из INSTRUCTION.md
#
# запуск: на edge (team-17-en) под пользователем team
#     bash deploy.sh
# каждый запуск сохраняет конфиги, применяет два XML и перезапускает YARN/HistoryServer.
# после ошибки можно исправить причину и повторить запуск; все предыдущие бэкапы остаются.
# запускать по одному экземпляру и без выполняющихся заданий. работающий HDFS не перезапускаем.

# e - останавливаемся при ошибке, u - неизвестная переменная тоже ошибка,
# pipefail - ошибка в середине трубы | тоже считается; E - сохраняем обработчик ERR.
set -Eeuo pipefail
[[ $# == 0 ]] || { echo "Запуск: bash deploy.sh (без аргументов)"; exit 2; }
[[ $(id -un) == team && $(hostname -s) == team-17-en ]] || {
  echo "Скрипт запускается на team-17-en под пользователем team."; exit 1;
}
sudo -n -u hadoop true || { echo "Нужен sudo: перед повторным запуском выполнить sudo -v."; exit 1; }

mkdir -p "$HOME/hw2-deploy-logs"
LOG="$HOME/hw2-deploy-logs/$(date +%Y%m%d-%H%M%S)-$$.log"
exec > >(tee -a "$LOG") 2>&1
echo "Лог: $LOG"
trap 'rc=$?; echo "Запуск остановлен. Лог: $LOG"; exit "$rc"' ERR

# заходим под пользователем hadoop, соблюдаем порядок проекта и правильное распределение ролей; все что связанное с hadoop -- делаем под пользователем hadoop
sudo -n -i -u hadoop bash -s <<'HADOOP'
set -Eeuo pipefail
HD=/home/hadoop/hadoop-3.4.0
CONF="$HD/etc/hadoop"
NODES=(team-17-nn team-17-00 team-17-01)
# BatchMode - не ждать пароль; ConnectTimeout - не зависать при недоступной ноде.
S=(-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new
   -o ServerAliveInterval=10 -o ServerAliveCountMax=2)
STEP="подготовка"
BACKUP="еще не создан"
step() { STEP=$1; printf '\n========== %s ==========\n' "$STEP"; }
trap 'rc=$?; echo "ОШИБКА: $STEP, строка $LINENO. Бэкап: $BACKUP"; exit "$rc"' ERR

step "Шаг 1. Проверить основу и запустить HDFS [hadoop@edge]"
# ---------------------------------------------------------------------------
# проверяем наши результаты из дз1, установленный hadoop; что-то не так или не установлено -> troubleshooting и/или инструкция или скрипт из дз1
whoami
hostname
set +u
source "$HOME/.profile"
set -u
"$HD/bin/hadoop" version
java -version
command -v curl >/dev/null || { echo "На edge нужен curl: sudo apt install curl под team."; exit 1; }
ssh -n "${S[@]}" team-17-nn 'test -s /home/hadoop/hadoop_data/dfs/name/current/VERSION'

# При повторе поднимаем только отсутствующие службы HDFS. Ничего не форматируем.
for h in "${NODES[@]}"; do
  P=$(ssh -n "${S[@]}" "$h" /usr/bin/jps)
  if [[ $h == team-17-nn ]]; then
    if ! grep -q ' NameNode$' <<< "$P"; then
      ssh -n "${S[@]}" "$h" "$HD/bin/hdfs --daemon start namenode"
    fi
    if ! grep -q ' SecondaryNameNode$' <<< "$P"; then
      ssh -n "${S[@]}" "$h" "$HD/bin/hdfs --daemon start secondarynamenode"
    fi
  fi
  if ! grep -q ' DataNode$' <<< "$P"; then
    ssh -n "${S[@]}" "$h" "$HD/bin/hdfs --daemon start datanode"
  fi
done

# проверяем запуск; все наши датаноды должны быть живы (3 штуки); Safe mode is OFF - ожидаем такой вывод 2-ой команды -- HDFS вышел из режима только для чтения. если после запуска еще ON, ждем и повторяем проверку; после OFF идем дальше, тогда сможем продолжить установку
R="" SM=""
for i in $(seq 1 18); do
  if R=$(timeout 30 "$HD/bin/hdfs" dfsadmin -report) &&
     SM=$(timeout 30 "$HD/bin/hdfs" dfsadmin -safemode get) &&
     grep -q 'Live datanodes (3)' <<< "$R" && grep -q 'Safe mode is OFF' <<< "$SM"; then
    break
  fi
  echo "HDFS еще не готов, проверка $i/18; повтор через 5 секунд..."
  sleep 5
done
echo "$R"
echo "$SM"
grep -q 'Live datanodes (3)' <<< "$R" && grep -q 'Safe mode is OFF' <<< "$SM" || {
  echo "ОШИБКА: нужны три живые DataNode и safe mode OFF."; exit 1;
}

# Дальше будет перезапуск YARN. Если он уже работает, сначала проверяем задания.
P=$(ssh -n "${S[@]}" team-17-nn /usr/bin/jps)
if grep -q ' ResourceManager$' <<< "$P"; then
  JOBS=$(timeout 30 "$HD/bin/yarn" application -list -appStates NEW,NEW_SAVING,SUBMITTED,ACCEPTED,RUNNING)
  echo "$JOBS"
  grep -Eq 'Total number of applications.*:[[:space:]]*0[[:space:]]*$' <<< "$JOBS" || {
    echo "ОШИБКА: не подтверждено отсутствие заданий. Перед повторным запуском нужно дождаться их завершения."; exit 1;
  }
fi

step "Шаг 2. Сохранить конфиги и проверить workers [hadoop@edge]"
# ---------------------------------------------------------------------------
# бэкапим конфиги hadoop, чтобы при неудачных изменениях вернуть предыдущие настройки
BACKUP="hadoop-config-before-hw2-$(date +%Y%m%d-%H%M%S)-$$"
cp -a "$CONF" "$HOME/$BACKUP"

# бэкапим на остальных нодах тоже
for h in "${NODES[@]}"; do
  ssh -n "${S[@]}" "$h" "cp -a '$CONF' '/home/hadoop/$BACKUP'"
done
echo "Бэкапы на всех четырех нодах: /home/hadoop/$BACKUP"

# проверяем, что workers настроен правильно, он нужен будет YARN
cd "$CONF"
cat workers
EXPECTED=$(printf '%s\n' "${NODES[@]}" | sort)
W=$(ssh -n "${S[@]}" team-17-nn "cat '$CONF/workers'")
[[ $(awk 'NF && $1 !~ /^#/ {print $1}' <<< "$W" | sort) == "$EXPECTED" ]] || {
  echo "ОШИБКА: в workers на nn должны быть nn, 00 и 01."; exit 1;
}

step "Шаг 3. Настроить YARN [hadoop@edge]"
# ---------------------------------------------------------------------------
# Здесь мы канонично настроили важные для работы YARN параметры:
# yarn.resourcemanager.hostname: нода, на которой будет установлен ResourceManager (в нашем небольшом кластере размещаем его на team-17-nn вместе с NameNode)
# yarn.nodemanager.aux-services: выставляем в aux-services (доп. сервисы) mapreduce_shuffle. Это необходимо выставить, чтобы подготовить кластер к MapReduce заданиям
# yarn.nodemanager.env-whitelist: используем список переменных окружения из официального примера настройки MapReduce. Тоже необходимо выставить
# Пишем во временный файл, чтобы обрыв не оставил рабочий XML наполовину записанным.
cat > yarn-site.xml.tmp <<'XML_YARN'
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
XML_YARN
mv yarn-site.xml.tmp yarn-site.xml

step "Шаг 4. Настроить MapReduce и HistoryServer [hadoop@edge]"
# ---------------------------------------------------------------------------
# Здесь мы настроили непосредственно MapReduce:
# mapreduce.framework.name: фреймворк ответственный за MapReduce -- YARN. Он выделяет ресурсы и организует запуск процессов задания
# mapreduce.application.classpath: указываем, где Java искать библиотеки MapReduce
# mapreduce.jobhistory.address: адрес HistoryServer который хранит информацию о завершенных MapReduce заданиях. Нода -- team-17-nn, порт для обращений программ -- 10020
# mapreduce.jobhistory.webapp.address: так же настраиваем адрес для веб-интерфейса HistoryServer на нашей NameNode.
cat > mapred-site.xml.tmp <<'XML_MAPRED'
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
XML_MAPRED
mv mapred-site.xml.tmp mapred-site.xml

step "Шаг 5. Раздать конфиги [hadoop@edge]"
# ---------------------------------------------------------------------------
# для стабильной работы кластера все вышеупомянтуые конфиги должны быть на всех нодах, копируем их туда. при ошибке копирования цикл прекращается; перед запуском службы конфиги должны попасть на все три ноды
# set -e остановит весь скрипт, если хотя бы один scp не удался.
for h in "${NODES[@]}"; do
  scp "${S[@]}" yarn-site.xml mapred-site.xml "$h:$CONF/" </dev/null
done

step "Шаг 6. Запустить YARN и HistoryServer [hadoop@nn]"
# ---------------------------------------------------------------------------
# заходим на неймноду, непосредственно на "управляющую" кластером ноду
# здесь SSH получает следующий блок по stdin, поэтому -n ему не нужен.
ssh "${S[@]}" team-17-nn bash -s <<'YARN'
set -euo pipefail
cd /home/hadoop/hadoop-3.4.0
export HADOOP_SSH_OPTS="-o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new -o ServerAliveInterval=10 -o ServerAliveCountMax=2"

# при повторе останавливаем YARN/HistoryServer, чтобы они прочитали новые XML.
# HistoryServer мог потерять PID-файл. Сверяем его с реальным процессом из jps.
P=$(/usr/bin/jps)
JHS_PID=$(awk '$2 == "JobHistoryServer" {print $1}' <<< "$P")
if [[ -n $JHS_PID ]]; then
  [[ $JHS_PID =~ ^[0-9]+$ ]] || { echo "ОШИБКА: найдено несколько HistoryServer."; exit 1; }
  if [[ -r /tmp/hadoop-hadoop-historyserver.pid ]] &&
     [[ $(cat /tmp/hadoop-hadoop-historyserver.pid) == "$JHS_PID" ]]; then
    bin/mapred --daemon stop historyserver
  else
    # PID-файл по умолчанию лежит в /tmp. Без него stop не находит процесс.
    echo "HistoryServer: PID-файл отсутствует или неверен; завершаем процесс $JHS_PID через SIGTERM."
    kill -TERM "$JHS_PID"
  fi
fi
sbin/stop-yarn.sh

# ждем завершения служб; общая команда stop могла не достучаться до одной из нод.
for h in team-17-nn team-17-00 team-17-01; do
  for i in $(seq 1 15); do
    P=$(timeout 20 ssh -n $HADOOP_SSH_OPTS "$h" /usr/bin/jps)
    if ! grep -Eq ' (ResourceManager|NodeManager|JobHistoryServer)$' <<< "$P"; then
      break
    fi
    echo "$h: ждем остановки YARN/HistoryServer, проверка $i/15..."
    sleep 2
  done
  if grep -Eq ' (ResourceManager|NodeManager|JobHistoryServer)$' <<< "$P"; then
    echo "ОШИБКА: YARN/HistoryServer на $h не остановился. Оставшиеся процессы:"
    echo "$P"
    exit 1
  fi
done

# запускаем на ней YARN
sbin/start-yarn.sh

# запускаем так же на ней historyserver. он будет принимать обращения по адресам и портам из mapred-site.xml
bin/mapred --daemon start historyserver
YARN

step "Шаг 7. Проверить службы [hadoop@nn]"
# ---------------------------------------------------------------------------
# спрашиваем у ResourceManager список известных ему NodeManager. ожидаем наши три ноды -- nn, 00, 01 -- в состоянии RUNNING
R="" RUNNING=""
for i in $(seq 1 18); do
  if R=$(timeout 30 "$HD/bin/yarn" node -list -all); then
    RUNNING=$(awk '$2 == "RUNNING" {sub(/:.*/, "", $1); print $1}' <<< "$R" | sort)
    [[ $RUNNING == "$EXPECTED" ]] && break
  fi
  echo "NodeManager еще не все RUNNING, проверка $i/18; повтор через 5 секунд..."
  sleep 5
done
echo "$R"
[[ $RUNNING == "$EXPECTED" ]] || { echo "ОШИБКА: нет трех ожидаемых RUNNING NodeManager."; exit 1; }

# проверяем, что все поднялось на всех рабочих нодах. На nn должны увидеть NameNode, SecondaryNameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer. На 00 и 01 -- DataNode и NodeManager. Также в каждом выводе будет Jps -- сама программа проверки. Порядок строк может отличаться от ноде к ноде
for h in "${NODES[@]}"; do
  P=$(ssh -n "${S[@]}" "$h" /usr/bin/jps)
  echo "== $h"
  echo "$P"
  SERVICES="DataNode NodeManager"
  if [[ $h == team-17-nn ]]; then
    SERVICES="NameNode SecondaryNameNode DataNode ResourceManager NodeManager JobHistoryServer"
  fi
  for service in $SERVICES; do
    grep -q " $service$" <<< "$P" || { echo "ОШИБКА: нет $service на $h."; exit 1; }
  done
done

step "Шаги 8-9. Веб-интерфейсы [hadoop@edge]"
# ---------------------------------------------------------------------------
# страницы всех DataNode и NodeManager открываются -- проверяем каждый адрес из таблицы
# JobHistoryServer открывается. список заданий пока пустой -- это нормально, мы еще не запускали MapReduce-задачи
# Для HDFS берем сами HTML-страницы, а не корневую страницу с переходом для браузера.
# curl -f сообщает об ошибках HTTP, -L проходит HTTP-переходы, retry дает службе время подняться.
for url in \
  http://team-17-nn:9870/dfshealth.html \
  http://team-17-nn:9868/status.html \
  http://team-17-nn:9864/datanode.html \
  http://team-17-00:9864/datanode.html \
  http://team-17-01:9864/datanode.html \
  http://team-17-nn:8088/ \
  http://team-17-nn:8042/ \
  http://team-17-00:8042/ \
  http://team-17-01:8042/ \
  http://team-17-nn:19888/; do
  CODE=$(curl --noproxy '*' -fsSL --connect-timeout 3 --max-time 10 \
    --retry 5 --retry-connrefused --retry-delay 2 -o /dev/null -w '%{http_code}' "$url")
  [[ $CODE == 200 ]] || { echo "ОШИБКА: $url вернул HTTP $CODE."; exit 1; }
  echo "OK $url"
done

echo
echo "Готово: HDFS, три NodeManager, HistoryServer и десять внутренних веб-интерфейсов проверены."
echo "Бэкапы на каждой ноде: /home/hadoop/$BACKUP"
# здесь мы открываем SSH-туннели под выведенные нашим кластером адреса на различные веб-интерфейсы
echo "SSH-туннель запускается на своем компьютере по шагу 8 INSTRUCTION.md; адреса для браузера - в шаге 9."
HADOOP

echo "Лог сохранен: $LOG"

