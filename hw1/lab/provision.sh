#!/bin/bash
# Makes a stock Ubuntu box look like a node of the course environment:
# user `team` with passwordless sudo, key-only ssh, own hostname on 127.0.1.1.
set -euo pipefail

ROLE="$1"            # edge | inner
NAME="$2"

hostnamectl set-hostname "$NAME"

if ! id team >/dev/null 2>&1; then
  adduser --disabled-password --gecos "" team
fi
echo 'team ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/team
chmod 440 /etc/sudoers.d/team

install -d -m 700 -o team -g team /home/team/.ssh
if [ "$ROLE" = "edge" ]; then
  # edge accepts the key "sent in the form" and holds the key for hopping inside
  cp /tmp/lab/user_key.pub /home/team/.ssh/authorized_keys
  install -m 600 -o team -g team /tmp/lab/team_internal /home/team/.ssh/team_internal
else
  cp /tmp/lab/team_internal.pub /home/team/.ssh/authorized_keys
fi
chown team:team /home/team/.ssh/authorized_keys
chmod 600 /home/team/.ssh/authorized_keys
rm -rf /tmp/lab

# Own name sits on 127.0.1.1 as on the course VMs; the other nodes are listed
# by address, which stands in for the internal DNS of the real environment.
{
  echo "127.0.0.1 localhost"
  echo "127.0.1.1 $NAME.hse.c.mws $NAME"
  echo
  for entry in "10.17.0.10 team-17-en" "10.17.0.11 team-17-nn" "10.17.0.12 team-17-00" "10.17.0.13 team-17-01"; do
    [ "${entry#* }" = "$NAME" ] || echo "$entry"
  done
  echo
  echo "::1 ip6-localhost ip6-loopback"
  echo "ff02::1 ip6-allnodes"
  echo "ff02::2 ip6-allrouters"
} > /etc/hosts

echo 'PasswordAuthentication no' > /etc/ssh/sshd_config.d/00-lab.conf
systemctl restart ssh
