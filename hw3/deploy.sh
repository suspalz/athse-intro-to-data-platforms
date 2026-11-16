#!/bin/bash
# HW3: развертывание Apache Hive (team-17) по шагам из INSTRUCTION.md
# Внешний PostgreSQL 16 на team-17-01, отдельный Hive Metastore и HiveServer2 на team-17-en,
# загрузка данных в партиционированную таблицу hw3.sales и проверка двух одновременных клиентов.
#
# запуск: на edge (team-17-en) под пользователем team
#     bash deploy.sh
# или без интерактивного ввода пароля роли hive в PostgreSQL:
#     PG_PASS='...' bash deploy.sh
# скрипт можно запускать повторно: уже скачанные архивы и существующая схема metastore
# сохраняются, конфиги обновляются с бэкапом, службы Hive перезапускаются, а партиции
# перезаписываются через OVERWRITE без накопления дублей.

# e - останавливаемся при ошибке, u - неизвестная переменная тоже ошибка,
# pipefail - ошибка в середине трубы | тоже считается; E - сохраняем обработчик ERR.
set -Eeuo pipefail
[[ $# == 0 ]] || { echo "Запуск: bash deploy.sh (без аргументов, опционально PG_PASS=...)"; exit 2; }
[[ $(id -un) == team && $(hostname -s) == team-17-en ]] || {
  echo "Скрипт запускается на team-17-en под пользователем team."; exit 1;
}
sudo -n true || { echo "Нужен sudo без запроса пароля: перед запуском выполнить sudo -v."; exit 1; }
sudo -n -u hadoop true || { echo "Нет прав sudo для перехода в пользователя hadoop."; exit 1; }

# Пароль роли hive в PostgreSQL не храним в коде скрипта: берем из PG_PASS или спрашиваем скрыто.
if [[ -z "${PG_PASS:-}" ]]; then
  if [[ -t 0 ]]; then
    read -r -s -p "Пароль роли hive в PostgreSQL: " PG_PASS
    echo
  fi
fi
[[ -n "${PG_PASS:-}" ]] || {
  echo "ОШИБКА: не задан пароль роли hive (нужно передать PG_PASS='...' или запускать в интерактивном терминале)."
  exit 1
}
if [[ "$PG_PASS" == *$'\n'* || "$PG_PASS" == *$'\r'* ]]; then
  echo "ОШИБКА: пароль PG_PASS не должен содержать переводы строк."
  exit 1
fi

mkdir -p "$HOME/hw3-deploy-logs"
LOG="$HOME/hw3-deploy-logs/$(date +%Y%m%d-%H%M%S)-$$.log"
exec > >(tee -a "$LOG") 2>&1
echo "Лог: $LOG"

STEP="подготовка"
step() { STEP=$1; printf '\n========== %s ==========\n' "$STEP"; }
trap 'rc=$?; echo "ОШИБКА на этапе: $STEP (строка $LINENO). Лог: $LOG"; exit "$rc"' ERR

# Опции SSH из-под team к внутренним нодам: на стенде Vagrant берем ~/.ssh/team_internal,
# на выданном кластере подойдет он же или ключ по умолчанию.
TEAM_SSH=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new
          -o ServerAliveInterval=10 -o ServerAliveCountMax=3)
if [[ -f "$HOME/.ssh/team_internal" ]]; then
  TEAM_SSH=(-i "$HOME/.ssh/team_internal" "${TEAM_SSH[@]}")
fi


step "Шаг 1. Проверить кластер и поднять HDFS + YARN [hadoop@edge]"
# ---------------------------------------------------------------------------
# переключаемся в нашего сервисного юзера hadoop -- всю работа по hadoop всегда делаем от его имени, хороший тон
sudo -n -i -u hadoop bash -s <<'STEP1_HADOOP'
set -Eeuo pipefail
HD=/home/hadoop/hadoop-3.4.0
NODES=(team-17-nn team-17-00 team-17-01)
S=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new
   -o ServerAliveInterval=10 -o ServerAliveCountMax=3)

# подгружаем переменные окружения и проверяем базу после ДЗ1 и ДЗ2 -- Java 11, Hadoop 3.4.0, живы ли все 3 DataNode, выключен ли Safe mode и видны ли 3 NodeManager в статусе RUNNING
set +u
source "$HOME/.profile"
set -u
hostname
java -version
"$HD/bin/hadoop" version
ssh -n "${S[@]}" team-17-nn 'test -s /home/hadoop/hadoop_data/dfs/name/current/VERSION'

# у демонов hadoop нет автозапуска через systemd, поэтому после выключения виртуалок стартуем HDFS, YARN и HistoryServer через ssh на управляющей ноде nn; ждем пока Safe mode станет OFF
# При повторе поднимаем только отсутствующие процессы, чтобы не дергать уже работающий HDFS и YARN.
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
  if R=$(timeout 30 "$HD/bin/hdfs" dfsadmin -report 2>/dev/null) &&
     SM=$(timeout 30 "$HD/bin/hdfs" dfsadmin -safemode get 2>/dev/null) &&
     YN=$(timeout 30 "$HD/bin/yarn" node -list -all 2>/dev/null); then
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
STEP1_HADOOP


step "Шаг 2. Установить PostgreSQL 16 под внешний Metastore [team@01]"
# ---------------------------------------------------------------------------
# обновляем списки пакетов и ставим PostgreSQL 16 на ноду 01 -- так разгружаем память слабой edge-ноды; через pg_lsclusters проверяем, что кластер 16/main поднялся на порту 5432 в статусе online
ssh "${TEAM_SSH[@]}" team@team-17-01 bash -s <<'STEP2_PG_INSTALL'
set -euo pipefail
if ! dpkg -s postgresql-16 >/dev/null 2>&1; then
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-16
fi
pg_lsclusters
pg_lsclusters | awk '$1 == "16" && $2 == "main" && $4 == "online" {found=1} END {exit !found}' || {
  sudo systemctl start postgresql@16-main
  pg_lsclusters
}
pg_lsclusters | awk '$1 == "16" && $2 == "main" && $4 == "online" {found=1} END {exit !found}'
STEP2_PG_INSTALL


step "Шаг 3. Открыть сетевой доступ к PostgreSQL с edge [team@01]"
# ---------------------------------------------------------------------------
EDGE_IP=$(getent ahostsv4 team-17-en | awk 'NR==1 {print $1}')
NODE01_IP=$(getent ahostsv4 team-17-01 | awk 'NR==1 {print $1}')
[[ -n "$EDGE_IP" && -n "$NODE01_IP" ]] || { echo "ОШИБКА: не удалось определить IP team-17-en / team-17-01."; exit 1; }
echo "Внутренний IP edge: $EDGE_IP, ноды 01: $NODE01_IP"

ssh "${TEAM_SSH[@]}" team@team-17-01 EDGE_IP="$EDGE_IP" NODE01_IP="$NODE01_IP" bash -s <<'STEP3_PG_NET'
set -euo pipefail
PG_DIR=/etc/postgresql/16/main
PG_CONF="$PG_DIR/postgresql.conf"
PG_HBA="$PG_DIR/pg_hba.conf"

# бэкапим дефолтные конфиги постгреса с таймстампом, чтобы легко откатиться если ошибемся в синтаксисе и служба не рестартанет
BACKUP="$HOME/postgresql-before-hw3-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$BACKUP"
sudo cp -a "$PG_DIR" "$BACKUP/"
echo "Бэкап конфигов PostgreSQL на team-17-01: $BACKUP"

# по дефолту постгрес слушает только локальную петлю 127.0.0.1 и снаружи к нему не достучаться; добавляем имя/адрес ноды team-17-01, чтобы принимать подключения внутри сети кластера на порту 5432, оставляя и localhost для локального psql
if sudo grep -Eq '^[[:space:]]*listen_addresses[[:space:]]*=' "$PG_CONF"; then
  sudo sed -i "s|^[[:space:]]*listen_addresses[[:space:]]*=.*|listen_addresses = 'localhost,team-17-01'|" "$PG_CONF"
else
  sudo sed -i "s|^[[:space:]]*#*[[:space:]]*listen_addresses[[:space:]]*=.*|listen_addresses = 'localhost,team-17-01'|" "$PG_CONF"
fi
if ! sudo grep -Eq "^[[:space:]]*listen_addresses[[:space:]]*=[[:space:]]*'localhost,team-17-01'" "$PG_CONF"; then
  echo "listen_addresses = 'localhost,team-17-01'" | sudo tee -a "$PG_CONF" >/dev/null
fi
if sudo grep -Eq '^[[:space:]]*#?[[:space:]]*port[[:space:]]*=' "$PG_CONF"; then
  sudo sed -i '0,/^[[:space:]]*#\?[[:space:]]*port[[:space:]]*=.*/s//port = 5432/' "$PG_CONF"
fi

# listen_addresses только открыл сетевой порт, а pg_hba.conf решает кого пускать; по принципу минимальных прав разрешаем TCP вход только к базе metastore, только роли hive, только с конкретного IP нашей edge-ноды /32 и с современным хешированием пароля scram-sha-256
RULE="host    metastore       hive            ${EDGE_IP}/32           scram-sha-256"
sudo sed -i '/^[[:space:]]*host[[:space:]]\+metastore[[:space:]]\+hive[[:space:]]\+/d' "$PG_HBA"
echo "$RULE" | sudo tee -a "$PG_HBA" >/dev/null

# смена listen_addresses требует полного рестарта службы, проверяем что сервис active (running), а в ss -ltn порт 5432 слушается сразу на двух адресах -- 127.0.0.1:5432 и 10.17.0.13:5432
sudo systemctl restart postgresql@16-main
sudo systemctl status postgresql@16-main --no-pager

SOCKETS=""
for i in $(seq 1 10); do
  SOCKETS=$(ss -ltn 'sport = :5432')
  if grep -Fq "127.0.0.1:5432" <<< "$SOCKETS" && grep -Fq "${NODE01_IP}:5432" <<< "$SOCKETS"; then
    break
  fi
  sleep 1
done
echo "$SOCKETS"
grep -Fq "127.0.0.1:5432" <<< "$SOCKETS" || { echo "ОШИБКА: PostgreSQL не слушает 127.0.0.1:5432."; exit 1; }
grep -Fq "${NODE01_IP}:5432" <<< "$SOCKETS" || { echo "ОШИБКА: PostgreSQL не слушает ${NODE01_IP}:5432."; exit 1; }
STEP3_PG_NET


step "Шаг 4. Создать роль hive и базу metastore [team@01]"
# ---------------------------------------------------------------------------
# создаем отдельную роль hive, а пароль задаем через \password hive -- так он вводится скрыто, сразу хешируется клиентом и не светится открытым текстом в истории psql и логах. следом создаем пустую БД metastore с владельцем hive, чтобы schematool сам смог накатить внутри таблицы без прав суперюзера. команды \du и \l проверяют результат, \q выходит из psql
# В неинтерактивном скрипте передаем пароль по закрытому потоку stdin без аргументов командной строки:
# сначала создаем или обновляем роль hive, затем при отсутствии базы создаем metastore с владельцем hive.
SQL_PASS=${PG_PASS//\'/\'\'}
{
  printf 'SQL_PASS_REMOTE=%q\n' "$SQL_PASS"
  cat <<'STEP4_PG_ROLE'
{
set -euo pipefail
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hive') THEN
    CREATE USER hive;
  END IF;
END
\$\$;
ALTER USER hive WITH LOGIN PASSWORD '${SQL_PASS_REMOTE}';
SQL
unset SQL_PASS_REMOTE

if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname = 'metastore'" | grep -qx '1'; then
  sudo -u postgres psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE metastore OWNER hive;"
else
  sudo -u postgres psql -v ON_ERROR_STOP=1 -c "ALTER DATABASE metastore OWNER TO hive;"
fi

sudo -u postgres psql -v ON_ERROR_STOP=1 -c '\du hive' -c '\l metastore'
} </dev/null
STEP4_PG_ROLE
} | ssh "${TEAM_SSH[@]}" team@team-17-01 bash -s


step "Шаг 5. Проверить доступность PostgreSQL с edge [team@edge]"
# ---------------------------------------------------------------------------
# ставим psql и tmux на edge и до установки самого Hive убеждаемся, что сеть, порт 5432, правило в pg_hba.conf и наш пароль роли hive реально работают -- в ответ должны увидеть metastore и hive
if ! command -v psql >/dev/null 2>&1 || ! command -v tmux >/dev/null 2>&1 || ! command -v curl >/dev/null 2>&1; then
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-client tmux curl
fi
PG_CHECK=$(PGPASSWORD="$PG_PASS" psql "host=team-17-01 port=5432 dbname=metastore user=hive sslmode=prefer" \
  -v ON_ERROR_STOP=1 -tAF '|' -c "SELECT current_database(), current_user;")
echo "Проверка входа в PostgreSQL с edge: $PG_CHECK"
[[ "$PG_CHECK" == "metastore|hive" ]] || {
  echo "ОШИБКА: ожидался ответ metastore|hive, получено: $PG_CHECK"
  exit 1
}


# Для шагов 6-14 переходим в пользователя hadoop на edge.
# Пароль для hive-site.xml экранируем для XML и передаем через закрытый stdin без аргументов командной строки.
XML_PASS=${PG_PASS//'&'/'&amp;'}
XML_PASS=${XML_PASS//'<'/'&lt;'}
XML_PASS=${XML_PASS//'>'/'&gt;'}

{
  printf 'XML_PASS_IN_HADOOP=%q\n' "$XML_PASS"
  cat <<'HADOOP_HIVE_STEPS'
{
set -Eeuo pipefail

STEP="Шаг 6"
step() { STEP=$1; printf '\n========== %s ==========\n' "$STEP"; }
trap 'rc=$?; echo "ОШИБКА внутри блока hadoop на этапе: $STEP (строка $LINENO)"; exit "$rc"' ERR

step "Шаг 6. Скачать дистрибутив Hive 4.0.0-alpha-2 и JDBC-драйвер [hadoop@edge]"
# ---------------------------------------------------------------------------
set +u
source "$HOME/.profile"
set -u
cd "$HOME"

# сразу бэкапим ~/.profile перед будущими правками, качаем рекомендованный в задании бинарный архив Hive 4.0.0-alpha-2 с ключом -c для докачки при сбое и сверяем контрольную сумму sha256 -- ждем статус OK, чтобы не распаковать битый архив
PROFILE_BACKUP="$HOME/.profile.before-hw3-$(date +%Y%m%d-%H%M%S)-$$"
cp -a "$HOME/.profile" "$PROFILE_BACKUP"
echo "Бэкап профиля: $PROFILE_BACKUP"

HIVE_TAR="apache-hive-4.0.0-alpha-2-bin.tar.gz"
HIVE_SHA="${HIVE_TAR}.sha256"
HIVE_URL_BASE="https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2"

if [[ ! -s "$HIVE_SHA" ]]; then
  wget -O "$HIVE_SHA" "${HIVE_URL_BASE}/${HIVE_SHA}"
fi
if [[ ! -f "$HIVE_TAR" ]] || ! sha256sum -c "$HIVE_SHA" >/dev/null 2>&1; then
  wget -c --progress=bar:force:noscroll "${HIVE_URL_BASE}/${HIVE_TAR}"
fi
sha256sum -c "$HIVE_SHA"

# распаковываем Hive; сам он написан на Java и общается с Postgres через JDBC-драйвер -- старый встроенный postgresql-42.4.1.jar (с ним PostgreSQL нашей верси работать не будет) убираем из lib/ в отдельную папку, чтобы в classpath не конфликтовали две версии сразу, и докачиваем свежий postgresql-42.7.14.jar под наш PostgreSQL 16; exit закрывает временное окно tmux
[[ -d apache-hive-4.0.0-alpha-2-bin ]] || tar -xzf "$HIVE_TAR"
mkdir -p "$HOME/hive-disabled-jars"
if [[ -f apache-hive-4.0.0-alpha-2-bin/lib/postgresql-42.4.1.jar ]]; then
  mv apache-hive-4.0.0-alpha-2-bin/lib/postgresql-42.4.1.jar "$HOME/hive-disabled-jars/"
fi
JDBC_JAR="apache-hive-4.0.0-alpha-2-bin/lib/postgresql-42.7.14.jar"
if [[ ! -s "$JDBC_JAR" ]]; then
  wget -O "${JDBC_JAR}.part" https://jdbc.postgresql.org/download/postgresql-42.7.14.jar
  mv "${JDBC_JAR}.part" "$JDBC_JAR"
fi
ls -lh "$HOME"/apache-hive-4.0.0-alpha-2-bin/lib/postgresql* "$HOME/hive-disabled-jars/"
JarCount=$(compgen -G "$HOME/apache-hive-4.0.0-alpha-2-bin/lib/postgresql-*.jar" | wc -l)
[[ "$JarCount" -eq 1 && -s "$HOME/$JDBC_JAR" ]] || {
  echo "ОШИБКА: в lib/ должен лежать ровно один JDBC-драйвер postgresql-42.7.14.jar."
  exit 1
}


step "Шаг 7. Настроить переменные окружения Hive [hadoop@edge]"
# ---------------------------------------------------------------------------
# прописываем пути до самого Hive, его папки конфигов и конфигов Hadoop, чтобы Hive знал адреса нашего HDFS и YARN, плюс добавляем bin в конец PATH не затирая старый. после source проверяем, что бинарники находятся и выводится версия Hive 4.0.0-alpha-2
add_profile() { grep -qxF "$1" "$HOME/.profile" || echo "$1" >> "$HOME/.profile"; }
add_profile 'export HIVE_HOME=/home/hadoop/apache-hive-4.0.0-alpha-2-bin'
add_profile 'export HIVE_CONF_DIR=$HIVE_HOME/conf'
add_profile 'export HADOOP_CONF_DIR=/home/hadoop/hadoop-3.4.0/etc/hadoop'
add_profile 'export PATH=$PATH:$HIVE_HOME/bin'

set +u
source "$HOME/.profile"
set -u
which hive beeline schematool
HIVE_VER=$(hive --version 2>&1)
echo "$HIVE_VER"
grep -q 'Hive 4.0.0-alpha-2' <<< "$HIVE_VER" || { echo "ОШИБКА: не подтверждена версия Hive 4.0.0-alpha-2."; exit 1; }


step "Шаг 8. Сконфигурировать hive-site.xml [hadoop@edge]"
# ---------------------------------------------------------------------------
# Если рабочий hive-site.xml уже был, сохраняем резервную копию с правами 600.
if [[ -f "$HIVE_CONF_DIR/hive-site.xml" ]]; then
  HIVE_CFG_BACKUP="$HOME/hive-config-before-hw3-$(date +%Y%m%d-%H%M%S)-$$"
  mkdir -p "$HIVE_CFG_BACKUP"
  chmod 700 "$HIVE_CFG_BACKUP"
  cp -a "$HIVE_CONF_DIR/hive-site.xml" "$HIVE_CFG_BACKUP/"
  echo "Бэкап прежнего hive-site.xml: $HIVE_CFG_BACKUP"
fi

# права 600 ставим, чтобы чужие юзеры не прочитали пароль БД
# - hive.server2.thrift.bind.host / port (team-17-en:5433): сетевой адрес и порт HiveServer2, куда по JDBC подключаются клиенты Beeline
# - authentication=NONE, authorization.enabled=false, enable.doAs=false: учебный режим без Kerberos, а операции в HDFS идут от имени запустившего сервис юзера hadoop
# - webui.host / port (127.0.0.1:10002): веб-интерфейс HiveServer2 слушает только 127.0.0.1 на самой edge, смотреть будем через SSH-туннель
# - hive.metastore.uris (thrift://team-17-en:9083): ключевая настройка non-embedded режима -- явно заставляет HiveServer2 ходить в отдельный сервис Metastore по Thrift, а не поднимать локальный встроенный metastore
# - hive.metastore.event.db.notification.api.auth=false: разрешает HiveServer2 при старте опрашивать события удаленного Metastore от имени hadoop без настройки proxyuser в core-site.xml (иначе HS2 застревает в цикле инициализации и не открывает порт 5433)
# - hive.metastore.warehouse.dir (/user/hive/warehouse): корневой каталог таблиц Hive в HDFS
# - javax.jdo.option.*: JDBC-реквизиты подключения самого Metastore к нашей БД PostgreSQL на team-17-01:5432/metastore
(
  umask 077
  cat > "$HIVE_CONF_DIR/hive-site.xml.tmp" <<XML_HIVE
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
    <value>${XML_PASS_IN_HADOOP}</value>
  </property>
</configuration>
XML_HIVE
  chmod 600 "$HIVE_CONF_DIR/hive-site.xml.tmp"
  mv "$HIVE_CONF_DIR/hive-site.xml.tmp" "$HIVE_CONF_DIR/hive-site.xml"
)
unset XML_PASS_IN_HADOOP
ls -l "$HIVE_CONF_DIR/hive-site.xml"


step "Шаг 9. Подготовить каталоги в HDFS и инициализировать схему Metastore [hadoop@edge]"
# ---------------------------------------------------------------------------
# создаем в HDFS временную папку /tmp с правами 1777 (1 - sticky bit) под промежуточные файлы запросов и каталог хранилища таблиц /user/hive/warehouse с правами 775; проверяем права через ls -d
hdfs dfs -mkdir -p /tmp /user/hive/warehouse
hdfs dfs -chmod 1777 /tmp
hdfs dfs -chmod 775 /user/hive/warehouse
hdfs dfs -ls -d /tmp /user/hive/warehouse

# пустой базы в Postgres мало -- schematool -initSchema накатывает в нее десятки системных таблиц Hive для хранения описаний баз, колонок и партиций; вторая команда с -info сверяет, что и версия дистрибутива, и версия схемы в БД совпадают -- 4.0.0-alpha-2; повторно -initSchema на живой базе делать не нужно (и не желательно)
if schematool -dbType postgres -info >/dev/null 2>&1; then
  echo "Схема Metastore уже инициализирована, повторный -initSchema пропускаем."
else
  schematool -dbType postgres -initSchema
fi
SCHEMA_INFO=$(schematool -dbType postgres -info 2>&1)
echo "$SCHEMA_INFO"
grep -Eq 'Hive distribution version:[[:space:]]*4\.0\.0-alpha-2' <<< "$SCHEMA_INFO" || {
  echo "ОШИБКА: не совпала версия дистрибутива в schematool -info."; exit 1;
}
grep -Eq 'Metastore schema version:[[:space:]]*4\.0\.0-alpha-2' <<< "$SCHEMA_INFO" || {
  echo "ОШИБКА: не совпала версия схемы БД в schematool -info."; exit 1;
}


step "Шаг 10. Запустить сервисы Metastore и HiveServer2 [hadoop@edge]"
# ---------------------------------------------------------------------------
# создаем папку для логов и перед первым стартом проверяем, что старые экземпляры служб еще не висят в памяти и порты 9083, 5433, 10002 свободны
# При повторном запуске аккуратно останавливаем прежние HiveServer2 и Metastore, чтобы применился актуальный hive-site.xml.
mkdir -p "$HOME/hive-logs"

if tmux has-session -t hive-server2 2>/dev/null; then
  tmux kill-session -t hive-server2
fi
if tmux has-session -t hive-metastore 2>/dev/null; then
  tmux kill-session -t hive-metastore
fi
pkill -u hadoop -TERM -f '[o]rg\.apache\.hive\.service\.server\.HiveServer2' 2>/dev/null || true
pkill -u hadoop -TERM -f '[o]rg\.apache\.hadoop\.hive\.metastore\.HiveMetaStore' 2>/dev/null || true

for i in $(seq 1 20); do
  JPS_OUT=$(jps -lm)
  PORTS_OUT=$(ss -ltn | grep -E ':(5433|9083|10002)\b' || true)
  if ! grep -Eq 'HiveServer2|HiveMetaStore' <<< "$JPS_OUT" && [[ -z "$PORTS_OUT" ]]; then
    break
  fi
  echo "Ожидание завершения прежних процессов Hive, попытка $i/20..."
  sleep 2
done
JPS_OUT=$(jps -lm)
PORTS_OUT=$(ss -ltn | grep -E ':(5433|9083|10002)\b' || true)
if grep -Eq 'HiveServer2|HiveMetaStore' <<< "$JPS_OUT" || [[ -n "$PORTS_OUT" ]]; then
  echo "ОШИБКА: старые процессы Hive или порты 9083/5433/10002 все еще заняты:"
  echo "$JPS_OUT"
  echo "$PORTS_OUT"
  exit 1
fi

# запускаем самостоятельный процесс Metastore на порту 9083 внутри tmux, чтобы он не упал при закрытии SSH, а вывод дублируем через tee в лог-файл; отцепляемся от окна и ждем пока java начнет слушать порт 9083
tmux new-session -d -s hive-metastore \
  "bash -lc 'source ~/.profile && hive --service metastore -p 9083 2>&1 | tee -a ~/hive-logs/metastore.log'"

HMS_PORT=""
for i in $(seq 1 30); do
  tmux has-session -t hive-metastore 2>/dev/null || {
    echo "ОШИБКА: сессия tmux hive-metastore завершилась. Последние строки лога:"
    tail -n 60 "$HOME/hive-logs/metastore.log" || true
    exit 1
  }
  HMS_PORT=$(ss -ltnp 'sport = :9083' | grep LISTEN || true)
  [[ -n "$HMS_PORT" ]] && break
  echo "Ожидание запуска Metastore (:9083), попытка $i/30..."
  sleep 3
done
[[ -n "$HMS_PORT" ]] || {
  echo "ОШИБКА: Metastore не открыл порт 9083. Последние строки лога:"
  tail -n 60 "$HOME/hive-logs/metastore.log" || true
  exit 1
}
echo "$HMS_PORT"

# аналогично во втором окне tmux поднимаем HiveServer2, который принимает SQL-клиентов и общается с нашим Metastore; проверяем что живы обе сессии tmux, два процесса RunJar в jps -lm и открыты все три порта -- 9083 у Metastore, 5433 под JDBC и 10002 под Web UI
tmux new-session -d -s hive-server2 \
  "bash -lc 'source ~/.profile && hive --service hiveserver2 2>&1 | tee -a ~/hive-logs/hiveserver2.log'"

ALL_PORTS=""
for i in $(seq 1 45); do
  tmux has-session -t hive-server2 2>/dev/null || {
    echo "ОШИБКА: сессия tmux hive-server2 завершилась. Последние строки лога:"
    tail -n 60 "$HOME/hive-logs/hiveserver2.log" || true
    exit 1
  }
  ALL_PORTS=$(ss -ltnp | grep -E ':(5433|9083|10002)\b' || true)
  if grep -Eq ':9083\b' <<< "$ALL_PORTS" &&
     grep -Eq ':5433\b' <<< "$ALL_PORTS" &&
     grep -Eq ':10002\b' <<< "$ALL_PORTS"; then
    break
  fi
  echo "Ожидание готовности HiveServer2 (:5433 и :10002), попытка $i/45..."
  sleep 3
done

tmux ls
jps -lm
echo "$ALL_PORTS"
for p in 9083 5433 10002; do
  grep -Eq ":${p}\b" <<< "$ALL_PORTS" || {
    echo "ОШИБКА: порт $p не открыт. Лог HiveServer2:"
    tail -n 60 "$HOME/hive-logs/hiveserver2.log" || true
    exit 1
  }
done


step "Шаг 11. Проверить подключение через Beeline [hadoop@edge]"
# ---------------------------------------------------------------------------
# подключаемся к HiveServer2 по JDBC на порт 5433 в базу default (пароль пустой из-за учебного режима NONE). запрос SHOW DATABASES проходит всю цепочку Beeline -> HS2 -> Metastore -> Postgres и возвращает базу default. два SET подтверждают работу через удаленный thrift://team-17-en:9083 и штатный движок mr, дальше выходим через !quit
BL_SMOKE=$(beeline -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' \
  -e "SHOW DATABASES; SET hive.metastore.uris; SET hive.execution.engine;")
echo "$BL_SMOKE"
grep -q 'default' <<< "$BL_SMOKE" || { echo "ОШИБКА: база default не получена через Beeline."; exit 1; }
grep -q 'hive.metastore.uris=thrift://team-17-en:9083' <<< "$BL_SMOKE" || {
  echo "ОШИБКА: hive.metastore.uris не указывает на thrift://team-17-en:9083."; exit 1;
}
grep -q 'hive.execution.engine=mr' <<< "$BL_SMOKE" || {
  echo "ОШИБКА: неожиданный hive.execution.engine."; exit 1;
}


step "Шаг 12. Подготовить CSV-данные и загрузить их в партиционированную таблицу [hadoop@edge]"
# ---------------------------------------------------------------------------
# готовим тестовый датасет за два дня прямо на диске edge-ноды, потому что для команды LOAD DATA LOCAL через HiveServer2 локальной считается файловая система самого сервера HS2, wc -l подтверждает по 3 строки в каждом файле
mkdir -p "$HOME/hw3-data"
cat > "$HOME/hw3-data/sales-2026-10-01.csv" <<'CSV1'
1,book,2,500.00
2,pen,5,30.00
3,notebook,1,120.00
CSV1

cat > "$HOME/hw3-data/sales-2026-10-02.csv" <<'CSV2'
4,book,1,500.00
5,pen,3,30.00
6,notebook,2,120.00
CSV2

wc -l "$HOME"/hw3-data/sales-*.csv

# создаем ДБ hw3 и внешнюю таблицу EXTERNAL TABLE с разделителем `,` в каталоге HDFS /user/hive/warehouse/hw3_sales и партиционированием PARTITIONED BY (sale_date) -- так Hive раскладывает данные разных дней по отдельным подпапкам и при фильтрации по дате не читает лишние файлы; двумя командами LOAD DATA LOCAL ... OVERWRITE загружаем каждый CSV в свою партицию без накопления дублей при перезапуске
beeline -u 'jdbc:hive2://team-17-en:5433/default' -n hadoop -p '' <<'SQL_LOAD'
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
!quit
SQL_LOAD

# убеждаемся, что видны обе партиции sale_date=2026-10-01 и sale_date=2026-10-02, запросы по каждой дате возвращают свои 3 записи с корректными типами без NULL, а в DESCRIBE FORMATTED указаны EXTERNAL_TABLE, путь в HDFS и numPartitions = 2
PARTS=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p '' \
  -e "SHOW PARTITIONS hw3.sales;")
DAY1=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p '' \
  -e "SELECT sale_id, product, quantity, unit_price, sale_date FROM hw3.sales WHERE sale_date='2026-10-01' ORDER BY sale_id LIMIT 20;")
DAY2=$(beeline --outputformat=tsv2 --silent=true -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p '' \
  -e "SELECT sale_id, product, quantity, unit_price, sale_date FROM hw3.sales WHERE sale_date='2026-10-02' ORDER BY sale_id LIMIT 20;")
DESC_TBL=$(beeline -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p '' \
  -e "DESCRIBE FORMATTED hw3.sales;")

echo "--- SHOW PARTITIONS ---"
echo "$PARTS"
echo "--- SELECT 2026-10-01 ---"
echo "$DAY1"
echo "--- SELECT 2026-10-02 ---"
echo "$DAY2"
echo "--- DESCRIBE FORMATTED ---"
echo "$DESC_TBL"

grep -qx 'sale_date=2026-10-01' <<< "$PARTS" || { echo "ОШИБКА: нет партиции 2026-10-01."; exit 1; }
grep -qx 'sale_date=2026-10-02' <<< "$PARTS" || { echo "ОШИБКА: нет партиции 2026-10-02."; exit 1; }
[[ $(grep -c '^sale_date=' <<< "$PARTS") -eq 2 ]] || { echo "ОШИБКА: неожидаемое число партиций."; exit 1; }
grep -Fq $'1\tbook\t2\t500.00\t2026-10-01' <<< "$DAY1" || { echo "ОШИБКА: неверные строки в партиции 2026-10-01."; exit 1; }
[[ $(grep -c $'\t2026-10-01$' <<< "$DAY1") -eq 3 ]] || { echo "ОШИБКА: в партиции 2026-10-01 должно быть ровно 3 строки."; exit 1; }
grep -Fq $'4\tbook\t1\t500.00\t2026-10-02' <<< "$DAY2" || { echo "ОШИБКА: неверные строки в партиции 2026-10-02."; exit 1; }
[[ $(grep -c $'\t2026-10-02$' <<< "$DAY2") -eq 3 ]] || { echo "ОШИБКА: в партиции 2026-10-02 должно быть ровно 3 строки."; exit 1; }
grep -Fq 'EXTERNAL_TABLE' <<< "$DESC_TBL" || { echo "ОШИБКА: таблица не EXTERNAL_TABLE."; exit 1; }
grep -Fq 'hdfs://team-17-nn:9000/user/hive/warehouse/hw3_sales' <<< "$DESC_TBL" || {
  echo "ОШИБКА: неверный LOCATION в DESCRIBE FORMATTED."; exit 1;
}

# смотрим со стороны самого HDFS -- внутри /user/hive/warehouse/hw3_sales реально появились две папки sale_date=2026-10-01 и sale_date=2026-10-02 с нашими CSV-файлами внутри
hdfs dfs -ls -R /user/hive/warehouse/hw3_sales
HDFS_C1=$(hdfs dfs -cat '/user/hive/warehouse/hw3_sales/sale_date=2026-10-01/*')
HDFS_C2=$(hdfs dfs -cat '/user/hive/warehouse/hw3_sales/sale_date=2026-10-02/*')
echo "$HDFS_C1"
echo "$HDFS_C2"
[[ $(wc -l <<< "$HDFS_C1") -eq 3 && $(wc -l <<< "$HDFS_C2") -eq 3 ]] || {
  echo "ОШИБКА: число строк в файлах партиций HDFS отличается от 3."; exit 1;
}


step "Шаг 13. Проверить одновременную работу двух клиентов [hadoop@edge]"
# ---------------------------------------------------------------------------
# проверяем главное требование задания -- многопользовательский non-embedded режим. обе сессии Beeline висят подключенными одновременно, параллельно читают данные и метаданные через общие HiveServer2, Metastore и PostgreSQL и не блокируют друг друга (в отличие от встроенного Derby). оставляем оба окна открытыми, чтобы увидеть обе сессии в Web UI на следующем шаге
# В скрипте открываем два клиентских процесса Beeline параллельно и удерживаем оба соединения открытыми,
# пока проверяем число активных TCP-сессий к :5433 и страницу Web UI :10002.
TMP_DIR=$(mktemp -d /tmp/hw3-clients-XXXXXX)
C1_OUT="$TMP_DIR/client1.out"
C2_OUT="$TMP_DIR/client2.out"
FIFO1="$TMP_DIR/c1.fifo"
FIFO2="$TMP_DIR/c2.fifo"
mkfifo "$FIFO1" "$FIFO2"
cleanup_clients() {
  exec 3>&- 4>&- 2>/dev/null || true
  rm -rf "$TMP_DIR"
}
trap cleanup_clients EXIT

beeline -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p '' <"$FIFO1" >"$C1_OUT" 2>&1 &
PID1=$!
beeline -u 'jdbc:hive2://team-17-en:5433/hw3' -n hadoop -p '' <"$FIFO2" >"$C2_OUT" 2>&1 &
PID2=$!
exec 3>"$FIFO1"
exec 4>"$FIFO2"

printf '%s\n' "SELECT * FROM sales WHERE sale_date='2026-10-01' LIMIT 20;" >&3
printf '%s\n' "SELECT * FROM sales WHERE sale_date='2026-10-02' LIMIT 20;" >&4
printf '%s\n' "SHOW PARTITIONS sales;" >&4

# Ждем, пока оба клиента подключатся и получат свои выборки, оставаясь одновременно подключенными.
for i in $(seq 1 30); do
  if grep -Fq 'notebook' "$C1_OUT" 2>/dev/null && grep -Fq 'sale_date=2026-10-02' "$C2_OUT" 2>/dev/null; then
    break
  fi
  kill -0 "$PID1" 2>/dev/null && kill -0 "$PID2" 2>/dev/null || break
  sleep 2
done

ESTAB_5433=$(ss -tn '( dport = :5433 )' | grep -c ESTAB || true)
echo "Одновременных установленных соединений клиентов к HiveServer2 (:5433): $ESTAB_5433"
(( ESTAB_5433 >= 2 )) || {
  echo "ОШИБКА: ожидалось не менее 2 одновременных клиентских сессий к :5433."
  cat "$C1_OUT" "$C2_OUT" || true
  exit 1
}


step "Шаг 14. Проверить Web UI HiveServer2 и напомнить про SSH-туннель [hadoop@edge]"
# ---------------------------------------------------------------------------
# так как Web UI HiveServer2 привязан к 127.0.0.1:10002 на самой edge-ноде, прокидываем к нему локальный порт 10002 через SSH-туннель, флаг -N держит только проброс порта без открытия удаленного шелла
WEB_HTML=$(curl --noproxy '*' -fsSL --connect-timeout 3 --max-time 10 http://127.0.0.1:10002/)
grep -Fq 'HiveServer2' <<< "$WEB_HTML" || { echo "ОШИБКА: Web UI :10002 не вернул страницу HiveServer2."; exit 1; }
grep -Fq '4.0.0-alpha-2' <<< "$WEB_HTML" || { echo "ОШИБКА: на странице Web UI :10002 не найдена версия 4.0.0-alpha-2."; exit 1; }
echo "OK http://127.0.0.1:10002/ (HiveServer2 4.0.0-alpha-2 ответил HTTP 200 при двух открытых сессиях Beeline)"

# Штатно завершаем оба параллельных клиента после проверки активных сессий.
printf '%s\n' "SHOW PARTITIONS sales;" "!quit" >&3
printf '%s\n' "!quit" >&4
exec 3>&- 4>&-
wait "$PID1"
wait "$PID2"

grep -Fq 'notebook' "$C1_OUT" && grep -Fq 'sale_date=2026-10-01' "$C1_OUT" || {
  echo "ОШИБКА: первый клиент не вернул ожидаемые данные:"; cat "$C1_OUT"; exit 1;
}
grep -Fq 'notebook' "$C2_OUT" && grep -Fq 'sale_date=2026-10-02' "$C2_OUT" || {
  echo "ОШИБКА: второй клиент не вернул ожидаемые данные:"; cat "$C2_OUT"; exit 1;
}
echo "Оба параллельных клиента Beeline успешно отработали и штатно закрыли сессии."
} </dev/null
HADOOP_HIVE_STEPS
} | sudo -n -i -u hadoop bash -s

echo
echo "Готово: PostgreSQL 16 (team-17-01:5432), Hive Metastore (:9083), HiveServer2 (:5433), Web UI (:10002),"
echo "партиционированная таблица hw3.sales и одновременная работа двух клиентов Beeline проверены."
echo "SSH-туннель для Web UI запускается на своем компьютере по шагу 14 INSTRUCTION.md:"
echo "  стенд Vagrant:     ssh -i keys\\user_key -N -o ExitOnForwardFailure=yes -L 127.0.0.1:10002:127.0.0.1:10002 team@10.17.0.10"
echo "  выданный кластер:  ssh -i \"<private_ssh>\" -N -o ExitOnForwardFailure=yes -L 127.0.0.1:10002:127.0.0.1:10002 team@178.236.27.243"
echo "Адрес в браузере: http://127.0.0.1:10002"
echo "Лог сохранен: $LOG"
