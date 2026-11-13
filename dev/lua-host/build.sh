#!/bin/sh
# Needs gcc and the Lua 5.4 runtime library (Debian/Ubuntu: apt install liblua5.4-0). Used only by the dev stand-in.
set -e
cd "$(dirname "$0")"
LIB=$(ls /usr/lib/*/liblua5.4.so.0 /usr/lib/liblua5.4.so.0 /usr/local/lib/liblua5.4.so* 2>/dev/null | head -1)
[ -n "$LIB" ] || { echo "liblua5.4 not found"; exit 1; }
gcc -O2 -o lua-host host.c "$LIB" -Wl,-rpath,"$(dirname "$LIB")"
echo "built $(pwd)/lua-host"
