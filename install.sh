#!/bin/bash
# proctor installer: one command from a checkout to running services on a dedicated Linux host.
#
#   sudo ./install.sh              install or upgrade, then start and run `proctor doctor`
#   sudo ./install.sh --dry-run    print every action without changing anything
#   sudo ./install.sh --no-start   install but leave the services stopped
#   sudo ./install.sh --hermes     use Hermes Agent for subscription credentials/quota/notifications
#   sudo ./install.sh --uninstall  stop and remove services, app, CLI and firewall rules (keeps config/state)
#   sudo ./install.sh --uninstall --purge   ...and delete /etc/proctor, /var/lib/proctor and the proctor user
#
# Re-running is safe: existing settings.toml, credentials.env and project registrations are never
# overwritten. Read SECURITY.md first: the broker runs as root and Docker access is root-equivalent.
set -euo pipefail

SRC=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
APP=/opt/proctor
ETC=/etc/proctor
STATE=/var/lib/proctor
UNITDIR=/etc/systemd/system
UNITS=(proctor-firewall proctor-broker proctor-supervisor)
HERMES_PY=/usr/local/lib/hermes-agent/venv/bin/python

DRY=0 START=1 HERMES=0 UNINSTALL=0 PURGE=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    --no-start) START=0 ;;
    --hermes) HERMES=1 ;;
    --uninstall) UNINSTALL=1 ;;
    --purge) PURGE=1 ;;
    -h|--help) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $arg (see --help)" >&2; exit 2 ;;
  esac
done
[ "$PURGE" = 1 ] && [ "$UNINSTALL" = 0 ] && { echo "--purge only makes sense with --uninstall" >&2; exit 2; }

say() { printf '\033[1m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31merror:\033[0m %s\n' "$*" >&2; exit 1; }
run() { if [ "$DRY" = 1 ]; then printf '    [dry-run] %s\n' "$*"; else "$@"; fi; }
# write <path> <mode> <owner:group> <<content: create or replace a file atomically
write() {
  local path=$1 mode=$2 own=$3 tmp
  if [ "$DRY" = 1 ]; then printf '    [dry-run] write %s (%s %s)\n' "$path" "$mode" "$own"; cat >/dev/null; return; fi
  tmp=$(mktemp "$path.XXXXXX"); cat >"$tmp"; chmod "$mode" "$tmp"; chown "$own" "$tmp"; mv -f "$tmp" "$path"
}

[ "$(id -u)" = 0 ] || [ "$DRY" = 1 ] || die "run as root: sudo $0 $*"

# --------------------------------------------------------------------------- uninstall
if [ "$UNINSTALL" = 1 ]; then
  say "stopping and removing services"
  for u in "${UNITS[@]}"; do
    run systemctl disable --now "$u.service" 2>/dev/null || true
    run rm -f "$UNITDIR/$u.service"
  done
  run rm -rf "$UNITDIR/proctor-broker.service.d"
  run systemctl daemon-reload
  say "removing firewall rules and the runner network"
  for T in iptables ip6tables; do
    while $T -C INPUT -i proctor-gw0 -j PROCTOR-GW-IN 2>/dev/null; do run $T -D INPUT -i proctor-gw0 -j PROCTOR-GW-IN; [ "$DRY" = 1 ] && break; done
    run $T -F PROCTOR-GW-IN 2>/dev/null || true
    run $T -X PROCTOR-GW-IN 2>/dev/null || true
  done
  while iptables -C DOCKER-USER -i proctor-gw0 -j DROP 2>/dev/null; do run iptables -D DOCKER-USER -i proctor-gw0 -j DROP; [ "$DRY" = 1 ] && break; done
  run docker network rm proctor-gw 2>/dev/null || true
  say "removing $APP and /usr/local/bin/proctor"
  run rm -rf "$APP" /usr/local/bin/proctor
  if [ "$PURGE" = 1 ]; then
    say "purging $ETC, $STATE and the proctor user (job history and registrations are deleted)"
    run rm -rf "$ETC" "$STATE"
    id proctor >/dev/null 2>&1 && run userdel proctor
    getent group proctor >/dev/null && run groupdel proctor 2>/dev/null || true
  else
    say "kept $ETC and $STATE (use --uninstall --purge to delete them)"
  fi
  exit 0
fi

# --------------------------------------------------------------------------- preflight
say "checking host requirements"
missing=()
for c in python3 git docker systemctl iptables ip6tables ip runuser; do command -v "$c" >/dev/null || missing+=("$c"); done
[ ${#missing[@]} -eq 0 ] || die "missing commands: ${missing[*]} (install them and re-run)"
python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))' || die "python3 >= 3.11 required (found $(python3 -V 2>&1))"
[ -d /run/systemd/system ] || die "systemd is not the running init system"
docker info >/dev/null 2>&1 || die "docker daemon not reachable (is it installed and running?)"
if [ "$HERMES" = 1 ] && [ ! -x "$HERMES_PY" ]; then
  die "--hermes: $HERMES_PY not found; install Hermes Agent first (https://hermes-agent.nousresearch.com/docs/)"
fi
# The gateway subnet must not overlap a network this host already routes (except our own bridge).
# routed_overlap <subnet>: prints overlapping routes; succeeds only when there is an overlap.
routed_overlap() {
  ip -4 route show | awk '!/dev proctor-gw0/ && $1 ~ /\// {print $1}' | python3 -c '
import ipaddress, sys
mine = ipaddress.ip_network(sys.argv[1])
hits = [l.strip() for l in sys.stdin if ipaddress.ip_network(l.strip(), strict=False).overlaps(mine)]
print(*hits); sys.exit(0 if hits else 1)' "$1"
}
GW_SUBNET=""   # set when a fresh install has to move off the default subnet
if [ -f "$ETC/settings.toml" ]; then
  subnet=$(PROCTOR_ETC=$ETC python3 -I "$SRC/proctor/common.py" gateway-env | sed -n "s/^SUBNET=//p" | tr -d "'")
  if hits=$(routed_overlap "$subnet"); then
    die "gateway subnet $subnet in $ETC/settings.toml overlaps routed network(s) $hits; change [gateway].subnet and .bind"
  fi
else
  for cand in 172.31.250.0/24 172.31.251.0/24 172.31.252.0/24 172.31.253.0/24 10.231.250.0/24 10.231.251.0/24; do
    if ! routed_overlap "$cand" >/dev/null; then GW_SUBNET=$cand; break; fi
  done
  [ -n "$GW_SUBNET" ] || die "no free gateway subnet among the candidates; write $ETC/settings.toml with [gateway].subnet/.bind first"
  [ "$GW_SUBNET" = 172.31.250.0/24 ] && GW_SUBNET="" || echo "    default gateway subnet is in use; using $GW_SUBNET"
fi

# --------------------------------------------------------------------------- user and directories
say "creating the proctor service account"
getent group proctor >/dev/null || run groupadd --system proctor
id proctor >/dev/null 2>&1 || run useradd --system -g proctor -d "$STATE" -M -s /usr/sbin/nologin proctor
id -nG proctor 2>/dev/null | grep -qw docker || run usermod -aG docker proctor

say "creating $ETC and $STATE"
run install -d -m 0750 -o root -g proctor "$ETC"
# setgid: registrations written by `sudo proctor project add` inherit group proctor, so the supervisor can read them
run install -d -m 2750 -o root -g proctor "$ETC/projects"
run install -d -m 2770 -o proctor -g proctor "$STATE"
for d in build db jobs orch repos run xfer; do run install -d -m 2770 -o proctor -g proctor "$STATE/$d"; done

# --------------------------------------------------------------------------- application
say "installing application to $APP"
run install -d -m 0755 -o root -g root "$APP" "$APP/harness"
for d in proctor runner images bin; do
  run rm -rf "$APP/$d.new"
  run cp -R "$SRC/$d" "$APP/$d.new"
  run find "$APP/$d.new" -name __pycache__ -prune -exec rm -rf {} +
  run chown -R root:root "$APP/$d.new"
  run chmod -R u=rwX,go=rX "$APP/$d.new"
  run rm -rf "$APP/$d"
  run mv "$APP/$d.new" "$APP/$d"
done
run chmod 0755 "$APP/bin/proctor-firewall" "$APP/bin/proctor-devops-check" "$APP/runner/run.sh" "$APP/runner/mock/mock.sh"
write /usr/local/bin/proctor 0755 root:root <<EOF
#!/bin/sh
exec /usr/bin/python3 $APP/proctor/cli.py "\$@"
EOF

# --------------------------------------------------------------------------- configuration
if [ -f "$ETC/settings.toml" ]; then
  say "keeping existing $ETC/settings.toml"
else
  say "writing $ETC/settings.toml"
  settings=$(cat "$SRC/config/settings.toml")
  # Stage an installed Claude Code binary where the supervisor (ProtectHome=yes) can read it.
  claude=$(command -v claude || true)
  if [ -n "$claude" ]; then
    real=$(readlink -f "$claude")
    ver=$("$real" --version 2>/dev/null | awk '{print $1; exit}' | tr -cd '0-9A-Za-z._-')
    dst="$APP/harness/claude-${ver:-local}"
    echo "    found Claude Code at $real; staging it as $dst"
    run install -m 0755 -o root -g root "$real" "$dst"
    settings=${settings/claude_binary = \"\"/claude_binary = \"$dst\"}
  fi
  if [ -n "$GW_SUBNET" ]; then
    gw_ip=${GW_SUBNET%.0/24}.1
    settings=${settings/subnet = \"172.31.250.0\/24\"/subnet = \"$GW_SUBNET\"}
    settings=${settings/bind = \"172.31.250.1\"/bind = \"$gw_ip\"}
  fi
  if [ "$HERMES" = 1 ]; then
    settings=${settings/anthropic = \"env\"/anthropic = \"hermes\"}
    settings=${settings/openai = \"env\"/openai = \"hermes\"}
  fi
  write "$ETC/settings.toml" 0640 root:proctor <<<"$settings"
fi
if [ ! -f "$ETC/credentials.env" ]; then
  say "writing $ETC/credentials.env (root-only; add API keys here)"
  write "$ETC/credentials.env" 0600 root:root <"$SRC/config/api-keys.env.example"
fi

# --------------------------------------------------------------------------- services
say "installing systemd units"
for u in "${UNITS[@]}"; do write "$UNITDIR/$u.service" 0644 root:root <"$SRC/systemd/$u.service"; done
if [ "$HERMES" = 1 ]; then
  run install -d -m 0755 "$UNITDIR/proctor-broker.service.d"
  write "$UNITDIR/proctor-broker.service.d/hermes.conf" 0644 root:root <<EOF
# Written by install.sh --hermes: the broker imports Hermes' credential resolvers.
[Service]
ExecStart=
ExecStart=$HERMES_PY $APP/proctor/broker.py
EOF
fi
run systemctl daemon-reload
for u in "${UNITS[@]}"; do run systemctl enable --quiet "$u.service"; done

if [ "$START" = 0 ]; then
  say "installed; start with: sudo systemctl start ${UNITS[*]}"
  exit 0
fi
say "starting services"
for u in "${UNITS[@]}"; do run systemctl restart "$u.service"; done
[ "$DRY" = 1 ] && exit 0
sleep 2
say "proctor doctor"
/usr/local/bin/proctor doctor || true
cat <<EOF

Next steps
  1. Register a repository:   sudo proctor project add myproj --path /path/to/repo --check 'test=python3 -m unittest -q'
  2. Try the mock harness:    sudo proctor submit myproj 'Describe a small change' -a 'what done looks like' --harness mock
                              sudo proctor list; sudo proctor status <job-id>
  3. Real models: put API keys in $ETC/credentials.env, then in $ETC/settings.toml set [prices] and
     [budget] ceilings; re-register the project with --harness claude-code --model <id>.
EOF
