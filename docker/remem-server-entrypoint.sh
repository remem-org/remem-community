#!/bin/sh
# Runs as root so it can fix ownership of bind-mounted volumes (which Docker
# creates as root:root on the host when the source directory doesn't exist
# yet), then drops privileges to the unprivileged `remem` user before exec'ing
# the server.
set -e

chown -R remem:remem /var/lib/remem

exec setpriv --reuid=1000 --regid=1000 --init-groups "$@"
