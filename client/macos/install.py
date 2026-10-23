#!/usr/bin/env python3
"""Install this prepared Media Test 10 package; never downloads prerequisites.

Execution is explicit. Importing this module does not install or launch anything.
Only the listed files and local.media.document-pool LaunchAgent are replaced.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import urlsplit

BASE = Path(__file__).resolve().parent
HOME = Path.home()
APP = HOME / "Library/Application Support/Media Test 10"
LABEL = "local.media.document-pool"
PLIST = HOME / "Library/LaunchAgents" / (LABEL + ".plist")
PORT = 19180
MAX_BOOTSTRAP_BYTES = 16 << 20
INPUTS = ("media-adapter-msr", "documents.urls", "stream-relay.key", "Media Test 10.json",
          "bootstrap.json")
LOGS = ("msr.stdout.log", "msr.stderr.log")
TARGETS = {name: APP / name for name in INPUTS + LOGS}
TARGETS["launch-agent.plist"] = PLIST


class InstallError(Exception):
    """Messages are safe to display: no secrets, log contents, or command output."""


def regular(path):
    if path.is_symlink() or not path.is_file():
        raise InstallError("Отсутствует обычный файл пакета/установки: " + path.name)


def safe_target(path):
    # Refuse redirects outside the intended user-owned application directories.
    for item in [path] + list(path.parents):
        if item == HOME:
            break
        if item.is_symlink():
            raise InstallError("В пути установки обнаружена символическая ссылка; установка остановлена.")
    if path.exists() and not path.is_file():
        raise InstallError("На месте файла установки находится другой объект: " + path.name)
    if path.exists() and path.stat().st_uid != os.getuid():
        raise InstallError("Файл установки принадлежит другому пользователю: " + path.name)


def bounded_read(path, limit):
    regular(path)
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    if not data or len(data) > limit:
        raise InstallError("Некорректный размер файла: " + path.name)
    return data


def document_set(data):
    try:
        lines = [line.strip() for line in data.decode("utf-8").splitlines() if line.strip()]
        valid = all(len(line.encode("utf-8")) <= 8192 and
                    urlsplit(line).scheme == "https" and urlsplit(line).hostname and
                    not urlsplit(line).username and not urlsplit(line).password for line in lines)
    except (UnicodeError, ValueError):
        valid, lines = False, []
    if not valid or len(lines) != 10 or len(set(lines)) != 10:
        raise InstallError("documents.urls должен содержать ровно 10 разных HTTPS-ссылок.")
    return set(lines)


def validate_bootstrap(data, documents):
    """Validate private metadata and exact URL keys without exposing its values."""
    try:
        bundle = json.loads(data)
        if (not isinstance(bundle, dict)
                or type(bundle.get("schemaVersion")) is not int
                or bundle["schemaVersion"] != 1):
            raise ValueError()
        entries = bundle.get("documents")
        if not isinstance(entries, list) or not 1 <= len(entries) <= 10:
            raise ValueError()
        urls = []
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("url"), str):
                raise ValueError()
            urls.append(entry["url"])
            editor_url = entry.get("editorUrl")
            config = entry.get("clientConfig")
            if not isinstance(editor_url, str) or not isinstance(config, dict):
                raise ValueError()
            parsed = urlsplit(editor_url)
            if (parsed.scheme != "https" or not parsed.hostname
                    or parsed.username or parsed.password):
                raise ValueError()
            office = config.get("officeActionData")
            if (not isinstance(office, dict)
                    or not isinstance(office.get("editor_config"), dict)
                    or not isinstance(office.get("balancer_url"), str)
                    or not office["balancer_url"]):
                raise ValueError()
        if len(urls) != len(set(urls)) or set(urls) != documents:
            raise ValueError()
    except (TypeError, ValueError, UnicodeError):
        raise InstallError("bootstrap.json повреждён или не соответствует набору документов; ничего не заменено.") from None


def atomic_write(path, data, mode):
    safe_target(path)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=str(path.parent))
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def launchctl(*args, check=True):
    result = subprocess.run(["/bin/launchctl", *args], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=30)
    if check and result.returncode:
        raise InstallError("launchctl " + args[0] + ": ошибка " + str(result.returncode) + ".")
    return result


def domain():
    return "gui/" + str(os.getuid())


def loaded():
    return launchctl("print", domain() + "/" + LABEL, check=False).returncode == 0


def bootout():
    if loaded():
        launchctl("bootout", domain() + "/" + LABEL)


def port_free():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # A stopped helper can leave TIME_WAIT connections; they are not a
        # competing listener. This does not enable SO_REUSEPORT sharing.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", PORT))
        except OSError:
            return False
    return True


def wait_port_free(seconds=10, *, restoring=False):
    deadline = time.monotonic() + seconds
    while not port_free():
        if time.monotonic() >= deadline:
            if restoring:
                raise InstallError("Порт 19180 не освободился для отката; резервная копия сохранена, восстановление требует проверки.")
            raise InstallError("Локальный порт 19180 занят; существующая установка будет восстановлена.")
        time.sleep(0.2)


def wait_ready():
    print("Ожидаю подключения адаптера к 10 документам (до 135 секунд)…", flush=True)
    deadline = time.monotonic() + 135
    while time.monotonic() < deadline:
        status = launchctl("print", domain() + "/" + LABEL, check=False)
        if status.returncode:
            raise InstallError("Новый LaunchAgent исчез после запуска.")
        pid = re.search(rb"\n\s*pid = (\d+)\s*\n", status.stdout)
        if pid:
            # Confirm that this job, rather than another process, owns the port.
            owner = subprocess.run(["/usr/sbin/lsof", "-nP", "-a", "-p", pid.group(1).decode(),
                                    "-iTCP:" + str(PORT), "-sTCP:LISTEN", "-t"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
            if pid.group(1) in owner.stdout.split():
                try:
                    with socket.create_connection(("127.0.0.1", PORT), timeout=1) as sock:
                        sock.sendall(b"\x05\x01\x00")
                        reply = b""
                        while len(reply) < 2:
                            part = sock.recv(2 - len(reply))
                            if not part:
                                break
                            reply += part
                        if reply == b"\x05\x00":
                            return
                except OSError:
                    pass
        time.sleep(0.5)
    raise InstallError("Адаптер не подтвердил локальную готовность за 135 секунд.")


def validate_plist(path):
    result = subprocess.run(["/usr/bin/plutil", "-lint", str(path)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
    if result.returncode:
        raise InstallError("Проверка LaunchAgent plist не пройдена.")


def candidate_plist():
    return {
        "Label": LABEL,
        "ProgramArguments": [str(APP / "media-adapter-msr"), "--client",
                             "--url-list-file", str(APP / "documents.urls"),
                             "--stream-relay", "--stream-key-file", str(APP / "stream-relay.key"),
                             "--doc-bootstrap-file", str(APP / "bootstrap.json"),
                             "--stream-chunk-bytes", "1024", "--stream-window-bytes", "4194304",
                             "--stream-rto", "5s", "--max-connections", "128", "--queue-packets", "128",
                             "--min-ready", "10", "--ready-timeout", "120s",
                             "--socks5", "127.0.0.1:19180", "--stats-interval", "15s"],
        "RunAtLoad": True, "KeepAlive": {"SuccessfulExit": False},
        "ThrottleInterval": 60, "ProcessType": "Background", "Umask": 0o077,
        "WorkingDirectory": str(APP),
        "StandardOutPath": str(APP / LOGS[0]), "StandardErrorPath": str(APP / LOGS[1]),
    }


def preflight():
    if sys.version_info < (3, 9):
        raise InstallError("Нужен Python 3.9 или новее.")
    if sys.platform != "darwin" or os.getuid() == 0:
        raise InstallError("Запустите на Mac от обычного вошедшего пользователя, без sudo.")
    silicon = subprocess.run(["/usr/sbin/sysctl", "-n", "hw.optional.arm64"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
    if silicon.returncode or silicon.stdout.strip() != b"1":
        raise InstallError("Этот пакет предназначен только для Mac с Apple Silicon (M1/M2/M3/M4 и новее).")
    for tool in ("/usr/sbin/lsof", "/bin/launchctl", "/usr/bin/plutil"):
        if not os.access(tool, os.X_OK):
            raise InstallError("Недоступна необходимая системная утилита: " + Path(tool).name)
    data = {name: bounded_read(BASE / name, 100 << 20 if name == "media-adapter-msr"
                              else MAX_BOOTSTRAP_BYTES if name == "bootstrap.json" else 1 << 20)
            for name in INPUTS}
    bootstrap_stat = (BASE / "bootstrap.json").stat()
    if bootstrap_stat.st_uid != os.getuid() or stat.S_IMODE(bootstrap_stat.st_mode) & 0o077:
        raise InstallError("bootstrap.json должен принадлежать текущему пользователю и быть доступен только ему (0600).")
    if len(data["media-adapter-msr"]) < 32 or struct.unpack("<II", data["media-adapter-msr"][:8]) != (0xFEEDFACF, 0x0100000C):
        raise InstallError("Бинарник пакета должен быть Mach-O arm64.")
    docs = document_set(data["documents.urls"])
    validate_bootstrap(data["bootstrap.json"], docs)
    if len(data["stream-relay.key"]) != 32:
        raise InstallError("stream-relay.key должен содержать ровно 32 двоичных байта.")
    try:
        if not isinstance(json.loads(data["Media Test 10.json"]), dict):
            raise ValueError()
    except (ValueError, UnicodeError):
        raise InstallError("Файл Media Test 10.json повреждён; запросите готовый пакет заново.") from None
    for path in TARGETS.values():
        safe_target(path)
    for path in (APP, APP / "installer-backups", PLIST.parent):
        if path.exists() and (not path.is_dir() or path.stat().st_uid != os.getuid()):
            raise InstallError("Каталог установки недоступен текущему пользователю.")
        # Also checks an existing directory's ancestry without treating it as a file.
        safe_target(path / ".installer-safety-check")
    if (APP / "documents.urls").exists():
        if document_set(bounded_read(APP / "documents.urls", 1 << 20)) != docs:
            raise InstallError("В этой установке другие документы. Ничего не заменено; нужна отдельная настройка.")
        data["documents.urls"] = (APP / "documents.urls").read_bytes()
    if (APP / "stream-relay.key").exists():
        if bounded_read(APP / "stream-relay.key", 33) != data["stream-relay.key"]:
            raise InstallError("Ключ отличается от существующей установки. Ничего не заменено.")
    if PLIST.exists():
        try:
            old = plistlib.loads(PLIST.read_bytes())
            args = old.get("ProgramArguments", [])
            executable = old.get("Program") or (args[0] if args else None)
            if old.get("Label") != LABEL or executable not in (str(APP / "media-adapter"), str(APP / "media-adapter-msr")):
                raise ValueError()
        except (ValueError, TypeError, AttributeError, plistlib.InvalidFileException):
            raise InstallError("Существующий LaunchAgent не распознан; ничего не заменено.") from None
        validate_plist(PLIST)
    if launchctl("print", domain(), check=False).returncode:
        raise InstallError("Нет пользовательской графической сессии macOS. Запустите файл из Finder.")
    disabled = launchctl("print-disabled", domain())
    if re.search(rb'"local\.media\.document-pool"\s*=>\s*true', disabled.stdout):
        raise InstallError("Этот LaunchAgent отключён в macOS. Включите его отдельно перед установкой.")
    was_loaded = loaded()
    if was_loaded and not PLIST.exists():
        raise InstallError("Работающий LaunchAgent не имеет файла для резервной копии. Установка остановлена.")
    if not was_loaded and not port_free():
        raise InstallError("Порт 19180 уже занят другим процессом. Ничего не изменено.")
    return data, was_loaded


def manifest_write(backup, manifest):
    atomic_write(backup / "manifest.json", (json.dumps(manifest, indent=2) + "\n").encode(), 0o600)


def restore(backup, manifest):
    # Validate every snapshot before changing the running candidate.
    snapshots = {}
    for name, entry in manifest["files"].items():
        safe_target(TARGETS[name])
        if entry["existed"]:
            regular(backup / name)
            payload = (backup / name).read_bytes()
            if hashlib.sha256(payload).hexdigest() != entry["sha256"]:
                raise InstallError("Контрольная сумма резервной копии не совпадает.")
            snapshots[name] = payload
    bootout()
    # launchctl bootout can return while the old process still owns its
    # listener. Never bootstrap the restored job into that competing socket.
    wait_port_free(restoring=True)
    for name, path in TARGETS.items():
        entry = manifest["files"][name]
        if entry["existed"]:
            atomic_write(path, snapshots[name], entry["mode"])
        elif path.exists():
            path.unlink()
    if manifest["app_existed"]:
        APP.chmod(manifest["app_mode"])
    if manifest["was_loaded"]:
        launchctl("bootstrap", domain(), str(PLIST))
        if not loaded():
            raise InstallError("Предыдущий LaunchAgent не восстановлен в launchd.")
    manifest["status"] = "rolled_back"
    manifest_write(backup, manifest)


def install():
    os.umask(0o077)
    data, was_loaded = preflight()
    app_existed = APP.exists()
    app_mode = stat.S_IMODE(APP.stat().st_mode) if app_existed else None
    APP.mkdir(mode=0o700, parents=True, exist_ok=True)
    PLIST.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    backups = APP / "installer-backups"
    backups.mkdir(mode=0o700, exist_ok=True)
    backups.chmod(0o700)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = backups / (stamp + "-" + uuid.uuid4().hex[:8])
    backup.mkdir(mode=0o700)
    manifest = {"schema": 1, "label": LABEL, "created_utc": stamp, "status": "prepared",
                "was_loaded": was_loaded, "app_existed": app_existed, "app_mode": app_mode, "files": {}}
    for name, path in TARGETS.items():
        entry = {"existed": path.exists()}
        if entry["existed"]:
            payload = path.read_bytes()
            entry.update(mode=stat.S_IMODE(path.stat().st_mode), sha256=hashlib.sha256(payload).hexdigest())
            atomic_write(backup / name, payload, 0o600)
        manifest["files"][name] = entry
    manifest_write(backup, manifest)
    staged = backup / "candidate.plist"
    atomic_write(staged, plistlib.dumps(candidate_plist(), sort_keys=False), 0o600)
    validate_plist(staged)
    try:
        bootout()
        wait_port_free()
        manifest["status"] = "replacing"
        manifest_write(backup, manifest)
        APP.chmod(0o700)
        for name in INPUTS:
            atomic_write(APP / name, data[name], 0o700 if name == "media-adapter-msr" else 0o600)
        for name in LOGS:
            atomic_write(APP / name, b"", 0o600)
        atomic_write(PLIST, staged.read_bytes(), 0o600)
        validate_plist(PLIST)
        launchctl("bootstrap", domain(), str(PLIST))
        wait_ready()
        manifest["status"] = "installed_local_ready"
        manifest_write(backup, manifest)
    except BaseException:
        try:
            restore(backup, manifest)
        except BaseException:
            raise InstallError("Установка не завершена, автоматический откат требует проверки. Резервная копия: " + str(backup)) from None
        raise InstallError("Установка не завершена; прежние файлы и состояние LaunchAgent восстановлены. Резервная копия: " + str(backup)) from None
    print("Media Test 10 установлен. Локальный адаптер готов: 127.0.0.1:19180.")
    print("Теперь вручную импортируйте Media Test 10.json из этой папки в Happ на ЭТОМ Mac.")
    print("Подключение интернета через Happ установщик не проверяет.")
    print("Резервная копия: " + str(backup))


if __name__ == "__main__":
    try:
        install()
    except InstallError as error:
        print("Ошибка: " + str(error), file=sys.stderr)
        sys.exit(1)
    except BaseException:
        # Do not expose raw exceptions containing data, process output, or secrets.
        print("Установка прервана или возникла системная ошибка. Не запускайте с sudo; передайте это сообщение сопровождающему.", file=sys.stderr)
        sys.exit(1)
