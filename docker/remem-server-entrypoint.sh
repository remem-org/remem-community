#!/bin/sh
# Runs as root so it can fix ownership of bind-mounted volumes (which Docker
# creates as root:root on the host when the source directory doesn't exist
# yet), then drops privileges to the unprivileged `remem` user before exec'ing
# the server.
set -e

chown -R remem:remem /var/lib/remem

# CMD is the bare `remem-server` (no args) so the image genuinely supports
# running config-less on built-in defaults, as the docs promise. If a config
# file has been bind-mounted at the conventional path, wire it up here rather
# than baking --config into CMD, which would make a missing file a hard crash
# instead of a supported "no config" mode. Only do this for the untouched
# default command -- an explicit `command:` override (e.g. a custom --config
# path) is left alone so misconfigurations there still fail loudly.
if [ "$#" -eq 1 ] && [ "$1" = "remem-server" ] && [ -f /etc/remem/config.toml ]; then
    set -- remem-server --config /etc/remem/config.toml
fi

exec setpriv --reuid=1000 --regid=1000 --init-groups "$@"
