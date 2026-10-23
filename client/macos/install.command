#!/bin/bash
# Double-click in Finder. No sudo, package downloads, or automatic prerequisites.
set -u
cd -- "$(dirname -- "$0")" || exit 1

python="$(command -v python3 2>/dev/null || true)"
if [[ "$python" == /usr/bin/python3 ]] && ! /usr/bin/xcode-select -p >/dev/null 2>&1; then
  python=""
fi
if [[ -z "$python" ]]; then
  printf '%s\n' 'Нужен установленный Python 3.9 или новее.' \
    'Установщик ничего не скачивает. Установите Python, затем откройте install.command снова.'
  read -r -p 'Нажмите Enter, чтобы закрыть окно. ' _
  exit 1
fi

"$python" ./install.py
status=$?
printf '\n'
read -r -p 'Нажмите Enter, чтобы закрыть окно. ' _
exit "$status"
