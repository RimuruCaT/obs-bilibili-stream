#!/usr/bin/env python3
"""Daily Bilibili + OBS auto-stream scheduler.

This script is designed to run unattended on Windows Server and perform:
1. prepare_time: call Bilibili startLive and cache RTMP address/key
2. start_time: set OBS stream service via WebSocket and start stream
3. stop_time(next day if earlier): stop OBS stream and stop Bilibili live room
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests
from obsws_python import ReqClient


VERSION_API_URL = (
    "https://api.live.bilibili.com/xlive/app-blink/v1/liveVersionInfo/getHomePageLiveVersion"
)
START_LIVE_URL = "https://api.live.bilibili.com/room/v1/Room/startLive"
STOP_LIVE_URL = "https://api.live.bilibili.com/room/v1/Room/stopLive"
CHECK_LOGIN_URL = "https://api.bilibili.com/x/web-interface/nav"

APP_KEY = "aae92bc66f3edfab"
APP_SEC = "af125a0d5279fd576c1b4418a3e8276d"

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://link.bilibili.com",
    "Referer": "https://link.bilibili.com/p/center/index",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    ),
}


@dataclass
class RuntimeConfig:
    retry_count: int
    retry_base_delay_seconds: float
    retry_max_delay_seconds: float
    loop_interval_seconds: int
    state_file: str
    verify_login_before_prepare: bool
    stop_bilibili_after_obs_stop: bool


class RetryError(RuntimeError):
    pass


class GracefulStop:
    def __init__(self) -> None:
        self.stop_requested = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, _frame: Any) -> None:
        logging.warning("Received signal %s, graceful stop requested", signum)
        self.stop_requested = True


def setup_logging(log_file: str, max_bytes: int, backup_count: int) -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root.addHandler(stdout_handler)


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)


def parse_hhmm(value: str) -> Tuple[int, int]:
    hh, mm = value.split(":")
    hour = int(hh)
    minute = int(mm)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"Invalid HH:MM time: {value}")
    return hour, minute


def next_occurrence(now: datetime, hhmm: str) -> datetime:
    hour, minute = parse_hhmm(hhmm)
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def retry_call(
    action: str,
    fn: Callable[[], Any],
    retry_count: int,
    base_delay: float,
    max_delay: float,
) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retry_count + 1):
        try:
            return fn()
        except Exception as exc:  # broad by design for long-running service robustness
            last_exc = exc
            if attempt == retry_count:
                break
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0, 0.3 * delay)
            logging.warning(
                "%s failed (attempt %d/%d): %s; retry in %.1fs",
                action,
                attempt,
                retry_count,
                exc,
                delay,
            )
            time.sleep(delay)
    raise RetryError(f"{action} failed after {retry_count} attempts: {last_exc}")


class BilibiliClient:
    def __init__(self, room_id: str, csrf_token: str, cookies: str, timeout: int = 15) -> None:
        self.room_id = room_id
        self.csrf_token = csrf_token
        self.cookies = cookies
        self.timeout = timeout

    def _headers(self) -> Dict[str, str]:
        headers = dict(DEFAULT_HEADERS)
        headers["Cookie"] = self.cookies
        return headers

    @staticmethod
    def _appsign(params: Dict[str, str]) -> str:
        sorted_items = sorted({**params, "appkey": APP_KEY}.items())
        query = urlencode(sorted_items)
        sign = hashlib.md5((query + APP_SEC).encode("utf-8")).hexdigest()
        signed_items = dict(sorted_items)
        signed_items["sign"] = sign
        return urlencode(sorted(signed_items.items()))

    def check_login(self) -> bool:
        resp = requests.get(CHECK_LOGIN_URL, headers=self._headers(), timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        return bool(data.get("data", {}).get("isLogin"))

    def _get_live_version(self) -> Tuple[str, str]:
        params = {
            "system_version": "2",
            "ts": str(int(time.time())),
        }
        query = self._appsign(params)
        resp = requests.get(
            f"{VERSION_API_URL}?{query}", headers=self._headers(), timeout=self.timeout
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"get live version failed: {payload.get('message')}")
        build = str(payload["data"].get("build", ""))
        version = payload["data"].get("curr_version", "")
        if not build or not version:
            raise RuntimeError("invalid build/version from Bilibili")
        return build, version

    def start_live(self, area_id: int) -> Tuple[str, str]:
        build, version = self._get_live_version()
        params = {
            "room_id": self.room_id,
            "platform": "pc_link",
            "area_v2": str(area_id),
            "backup_stream": "0",
            "csrf_token": self.csrf_token,
            "csrf": self.csrf_token,
            "build": build,
            "version": version,
            "ts": str(int(time.time())),
        }
        data = self._appsign(params)
        resp = requests.post(
            START_LIVE_URL,
            data=data,
            headers=self._headers(),
            timeout=self.timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        code = payload.get("code")
        if code != 0:
            if code == 60024:
                raise RuntimeError(
                    f"face verification required: {payload.get('data', {}).get('qr', '')}"
                )
            raise RuntimeError(f"start live failed: {payload.get('message')}")

        rtmp_addr = payload.get("data", {}).get("rtmp", {}).get("addr", "")
        rtmp_code = payload.get("data", {}).get("rtmp", {}).get("code", "")
        if not rtmp_addr or not rtmp_code:
            raise RuntimeError("missing rtmp address or stream key")
        return rtmp_addr, rtmp_code

    def stop_live(self) -> None:
        data = {
            "room_id": self.room_id,
            "platform": "pc_link",
            "csrf_token": self.csrf_token,
            "csrf": self.csrf_token,
        }
        resp = requests.post(
            STOP_LIVE_URL,
            data=data,
            headers=self._headers(),
            timeout=self.timeout,
        )
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"stop live failed: {payload.get('message')}")


class ObsController:
    def __init__(self, host: str, port: int, password: str, timeout: int) -> None:
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout

    def _client(self) -> ReqClient:
        return ReqClient(
            host=self.host,
            port=self.port,
            password=self.password,
            timeout=self.timeout,
        )

    def set_stream_service_and_start(self, server: str, key: str) -> None:
        client = self._client()
        client.set_stream_service_settings(
            stream_service_type="rtmp_custom",
            stream_service_settings={"server": server, "key": key, "use_auth": False},
        )
        status = client.get_stream_status()
        if not status.output_active:
            client.start_stream()

    def stop_stream(self) -> None:
        client = self._client()
        status = client.get_stream_status()
        if status.output_active:
            client.stop_stream()


class DailyScheduler:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

        runtime_cfg = config["runtime"]
        self.runtime = RuntimeConfig(
            retry_count=int(runtime_cfg["retry_count"]),
            retry_base_delay_seconds=float(runtime_cfg["retry_base_delay_seconds"]),
            retry_max_delay_seconds=float(runtime_cfg["retry_max_delay_seconds"]),
            loop_interval_seconds=int(runtime_cfg["loop_interval_seconds"]),
            state_file=runtime_cfg["state_file"],
            verify_login_before_prepare=bool(runtime_cfg["verify_login_before_prepare"]),
            stop_bilibili_after_obs_stop=bool(runtime_cfg["stop_bilibili_after_obs_stop"]),
        )

        self.tz = ZoneInfo(config["schedule"]["timezone"])

        bilibili_cfg = config["bilibili"]
        self.area_id = int(bilibili_cfg.get("area_id", 86))
        self.bili = BilibiliClient(
            room_id=str(bilibili_cfg["room_id"]),
            csrf_token=str(bilibili_cfg["csrf_token"]),
            cookies=str(bilibili_cfg["cookies"]),
        )

        obs_cfg = config["obs"]
        self.obs = ObsController(
            host=str(obs_cfg["host"]),
            port=int(obs_cfg["port"]),
            password=str(obs_cfg["password"]),
            timeout=int(obs_cfg["request_timeout_seconds"]),
        )

        self.prepare_time = str(config["schedule"]["prepare_time"])
        self.start_time = str(config["schedule"]["start_time"])
        self.stop_time = str(config["schedule"]["stop_time"])

        self.stopper = GracefulStop()

    def _default_state(self) -> Dict[str, Any]:
        return {
            "date": "",
            "prepare_done": False,
            "start_done": False,
            "stop_done": False,
            "last_rtmp_addr": "",
            "last_rtmp_code": "",
            "last_error": "",
            "updated_at": "",
        }

    def _load_state(self) -> Dict[str, Any]:
        if not os.path.exists(self.runtime.state_file):
            return self._default_state()
        try:
            data = load_json(self.runtime.state_file)
            if not isinstance(data, dict):
                return self._default_state()
            return {**self._default_state(), **data}
        except Exception as exc:
            logging.warning("State file load failed, reset state: %s", exc)
            return self._default_state()

    def _save_state(self, state: Dict[str, Any]) -> None:
        state["updated_at"] = datetime.now(self.tz).isoformat()
        save_json(self.runtime.state_file, state)

    def _roll_day_if_needed(self, state: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        today = now.date().isoformat()
        if state["date"] != today:
            logging.info("New day detected (%s), reset daily flags", today)
            state.update(
                {
                    "date": today,
                    "prepare_done": False,
                    "start_done": False,
                    "stop_done": False,
                    "last_error": "",
                }
            )
            self._save_state(state)
        return state

    def _should_run(self, now: datetime, hhmm: str, done: bool) -> bool:
        hour, minute = parse_hhmm(hhmm)
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        return now >= target and not done

    def _prepare(self, state: Dict[str, Any]) -> None:
        if self.runtime.verify_login_before_prepare:
            ok = retry_call(
                "check_login",
                self.bili.check_login,
                self.runtime.retry_count,
                self.runtime.retry_base_delay_seconds,
                self.runtime.retry_max_delay_seconds,
            )
            if not ok:
                raise RuntimeError("Bilibili login invalid, manual re-login required")

        rtmp_addr, rtmp_code = retry_call(
            "start_live",
            lambda: self.bili.start_live(self.area_id),
            self.runtime.retry_count,
            self.runtime.retry_base_delay_seconds,
            self.runtime.retry_max_delay_seconds,
        )
        state["last_rtmp_addr"] = rtmp_addr
        state["last_rtmp_code"] = rtmp_code
        state["prepare_done"] = True
        state["last_error"] = ""
        self._save_state(state)
        logging.info("Prepare done: obtained RTMP address and stream key")

    def _start(self, state: Dict[str, Any]) -> None:
        if not state["last_rtmp_addr"] or not state["last_rtmp_code"]:
            raise RuntimeError("RTMP params missing, run prepare stage first")

        retry_call(
            "obs_set_stream_and_start",
            lambda: self.obs.set_stream_service_and_start(
                state["last_rtmp_addr"], state["last_rtmp_code"]
            ),
            self.runtime.retry_count,
            self.runtime.retry_base_delay_seconds,
            self.runtime.retry_max_delay_seconds,
        )
        state["start_done"] = True
        state["last_error"] = ""
        self._save_state(state)
        logging.info("Start done: OBS streaming started")

    def _stop(self, state: Dict[str, Any]) -> None:
        retry_call(
            "obs_stop_stream",
            self.obs.stop_stream,
            self.runtime.retry_count,
            self.runtime.retry_base_delay_seconds,
            self.runtime.retry_max_delay_seconds,
        )

        if self.runtime.stop_bilibili_after_obs_stop:
            retry_call(
                "bilibili_stop_live",
                self.bili.stop_live,
                self.runtime.retry_count,
                self.runtime.retry_base_delay_seconds,
                self.runtime.retry_max_delay_seconds,
            )

        state["stop_done"] = True
        state["last_error"] = ""
        self._save_state(state)
        logging.info("Stop done: OBS stopped and Bilibili room closed")

    def run_forever(self) -> None:
        logging.info("Scheduler started")
        logging.info(
            "Schedule: prepare=%s start=%s stop=%s tz=%s",
            self.prepare_time,
            self.start_time,
            self.stop_time,
            self.tz,
        )

        while not self.stopper.stop_requested:
            now = datetime.now(self.tz)
            state = self._roll_day_if_needed(self._load_state(), now)

            try:
                if self._should_run(now, self.prepare_time, bool(state["prepare_done"])):
                    self._prepare(state)
                elif self._should_run(now, self.start_time, bool(state["start_done"])):
                    self._start(state)
                elif self._should_run(now, self.stop_time, bool(state["stop_done"])):
                    self._stop(state)
            except Exception as exc:
                logging.exception("Stage execution failed: %s", exc)
                state["last_error"] = str(exc)
                self._save_state(state)

            time.sleep(self.runtime.loop_interval_seconds)

        logging.info("Scheduler exited")


def run_once(config: Dict[str, Any], stage: str) -> None:
    scheduler = DailyScheduler(config)
    state = scheduler._load_state()
    now = datetime.now(scheduler.tz)
    state = scheduler._roll_day_if_needed(state, now)

    if stage == "prepare":
        scheduler._prepare(state)
    elif stage == "start":
        scheduler._start(state)
    elif stage == "stop":
        scheduler._stop(state)
    else:
        raise ValueError(f"unknown stage: {stage}")


def validate_config(config: Dict[str, Any]) -> None:
    for key in ["bilibili", "obs", "schedule", "runtime"]:
        if key not in config:
            raise ValueError(f"missing section: {key}")
    for key in ["prepare_time", "start_time", "stop_time", "timezone"]:
        if key not in config["schedule"]:
            raise ValueError(f"missing schedule.{key}")
    parse_hhmm(config["schedule"]["prepare_time"])
    parse_hhmm(config["schedule"]["start_time"])
    parse_hhmm(config["schedule"]["stop_time"])
    ZoneInfo(config["schedule"]["timezone"])


def main() -> int:
    parser = argparse.ArgumentParser(description="OBS + Bilibili daily auto stream scheduler")
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument(
        "--mode",
        choices=["run", "prepare", "start", "stop"],
        default="run",
        help="run=daemon loop; others execute stage once",
    )
    args = parser.parse_args()

    config = load_json(args.config)
    validate_config(config)

    runtime_cfg = config["runtime"]
    setup_logging(
        runtime_cfg["log_file"],
        int(runtime_cfg["log_max_bytes"]),
        int(runtime_cfg["log_backup_count"]),
    )

    logging.info("obs_bilibili_scheduler started with mode=%s", args.mode)

    if args.mode == "run":
        DailyScheduler(config).run_forever()
    else:
        run_once(config, args.mode)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
