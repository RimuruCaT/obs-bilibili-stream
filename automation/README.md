# 自动化联动（Bilibili + OBS WebSocket）

这个目录提供一个独立脚本 `obs_bilibili_scheduler.py`，用于在 Windows Server 上无人值守执行，并且默认直接复用原插件扫码登录后的配置（真正联动原仓库流程）：

1. `prepare_time`：调用 B 站开播接口，获取当日 RTMP 地址/推流码。
2. `start_time`：通过 OBS WebSocket 设置推流服务并自动开始推流。
3. `stop_time`：停止 OBS 推流，并调用 B 站停播接口。

> 脚本复用了仓库中同样的接口流程（`startLive` / `stopLive` 逻辑与签名方式）。

## 1. 安装依赖

```bash
python -m pip install -r automation/requirements.txt
```

## 2. 准备配置

复制并编辑示例配置：

```bash
copy automation\config.example.json automation\config.json
```

关键字段（推荐默认 `integration.use_obs_plugin_config=true`）：

- `integration.obs_plugin_config_path`：OBS 插件配置路径，脚本从这里读取 `cookies/csrf_token/room_id/area_id`。
  - 默认：`%APPDATA%/obs-studio/plugin_config/bilibili-stream-for-obs/config.json`
- `integration.sync_back_rtmp_to_plugin_config`：prepare 阶段后将 RTMP 地址/推流码回写到插件配置。
- `obs.host/port/password`：对应 OBS WebSocket (通常 4455)。
- `schedule.prepare_time/start_time/stop_time`：每天执行时间（`HH:MM`）。
- `schedule.timezone`：建议 `Asia/Shanghai`。
- `runtime.enable_obs_auto_recover`：OBS 推流异常中断且停止重连后，自动用当前 RTMP 参数重新拉起推流（默认开启）。
- `runtime.obs_inactive_grace_seconds`：OBS 非活跃判定宽限期（秒），避免“短暂重连抖动”被误判（默认 120）。
- `runtime.obs_recover_cooldown_seconds`：两次自动恢复的最小间隔（秒，默认 180）。
- `runtime.max_obs_recover_attempts_per_cycle`：单个直播周期内最大自动恢复次数（默认 8）。
- `runtime.obs_reconnect_flap_window_seconds`：统计 OBS 重连抖动的时间窗口（秒，默认 300）。
- `runtime.obs_reconnect_flap_threshold`：在窗口内达到该重连次数就触发主动恢复（默认 6）。
- `runtime.auto_stop_bilibili_on_manual_obs_stop`：关闭自动恢复时，若检测到你在 OBS 手动点了“停止推流”，脚本会自动同步调用 B 站停播。
- `runtime.enable_cookie_keepalive`：定时登录态保活检查并尝试刷新 cookies（默认开启）。
- `runtime.cookie_keepalive_interval_minutes`：保活检查间隔分钟数（默认 20）。
- `runtime.wait_heartbeat_interval_minutes`：空闲等待日志心跳间隔（分钟，默认 10）。

如果你明确不想读取插件配置，可把 `integration.use_obs_plugin_config` 改为 `false`，再手动填写 `bilibili.room_id/csrf_token/cookies`。

- Windows 路径写在 JSON 里时要注意：
  - 推荐用正斜杠：`C:/ProgramData/obs-studio/...`
  - 或把反斜杠转义：`C:\\ProgramData\\obs-studio\\...`

## 3. 运行模式

### 常驻模式（推荐）

```bash
python automation/obs_bilibili_scheduler.py --config automation/config.json --mode run
```

### 单步模式（用于测试）

```bash
python automation/obs_bilibili_scheduler.py --config automation/config.json --mode prepare
python automation/obs_bilibili_scheduler.py --config automation/config.json --mode start
python automation/obs_bilibili_scheduler.py --config automation/config.json --mode stop
```
### 预检模式（强烈建议先执行）

```bash
python automation/obs_bilibili_scheduler.py --config automation/config.json --mode doctor
```

会检查：
- 运行时依赖是否已安装（`requests`、`obsws-python`）
- 配置是否合法
- 凭据来源是否可读取（插件配置或手动配置）


## 4. 稳定性设计

- **跨天调度顺序**：`prepare -> start -> stop` 以一个 cycle 执行；当 `stop_time` 早于/等于 `start_time` 时会自动按“次日停播”处理。
- **Cookie 保活与回写**：运行中按间隔调用登录态检查；若服务端返回新 cookies，会自动回写到插件配置，尽量降低过期概率。
- **空闲心跳日志**：`run` 模式未到触发时间时会周期性输出下一阶段等待信息（英文，包含 “in HH:MM:SS” 倒计时），避免看起来像“卡住不动”。
- **OBS 断流自动恢复**：若 OBS 出现“反复重连后掉线”，脚本不仅会在非活跃宽限期后恢复，还会统计 reconnect 抖动次数；在窗口内达到阈值会主动恢复。恢复时若 OBS 仍显示“正在直播”，会先执行一次 stop 再更新推流配置并 start，避免 OBS 在直播中拒绝修改推流配置（带冷却与次数限制）。
- **OBS 手动停播联动**：当你关闭自动恢复后，脚本可把 OBS 手动停播同步为 B 站停播，避免还要在插件里再点一次“停止直播”。
- **重试机制**：接口/OBS 调用失败自动重试（指数退避 + 抖动）。
- **状态文件**：`runtime.state_file` 记录当天阶段状态，避免重复执行。
- **状态自修复**：若检测到阶段标记与当前时间矛盾（例如测试时残留了已完成标记），`run` 模式会自动修正并继续推进。
- **日志轮转**：按大小滚动日志，防止日志无限增长。
- **信号优雅退出**：收到中断信号时安全退出。

## 5. Windows Server 部署建议

可用「任务计划程序」启动常驻脚本（开机自启），或改成 3 个独立任务（`prepare/start/stop`）。

### 方案 A：一个常驻任务（更简单）

- 触发器：开机时
- 操作：
  - 程序：`python`
  - 参数：`automation/obs_bilibili_scheduler.py --config automation/config.json --mode run`
  - 起始于：仓库目录

### 方案 B：三个时间任务（更接近传统 cron）

分别建立三个任务，`--mode` 分别为 `prepare/start/stop`，触发时间配置成你的三个时间点。

## 6. 风险提示

- 首次仍需要在 OBS 插件内扫码登录一次（与原仓库流程一致）；后续脚本会复用同一份配置。
- `prepare` 阶段会调用 B 站 `startLive`，这在平台侧会使直播间进入开播状态。
- 若 B 站返回人脸认证（例如 code `60024`），脚本会把 `prepare` 阶段标记为当日阻塞，不会在 `run` 模式里反复重试轰炸接口；人工完成验证后可手动执行一次 `--mode prepare` 继续。
- 若 B 站返回“开播太频繁，请稍后再试”，脚本也会把 `prepare` 阶段标记为当日阻塞，避免持续重试触发更严格限流；建议隔一段时间后再手动执行 `--mode prepare`。
- Cookie 过期时，可重新在插件里扫码登录，脚本会自动读到新值。
- 注意：保活机制是“尽量降低过期概率”，不能保证永不过期，也不能绕过平台的人脸/风控校验。


## 7. 常见问题

- 报错 `No time zone found with key Asia/Shanghai`：
  - 运行：`python -m pip install tzdata`
  - 然后重试 `--mode doctor`。

- 报错 `Invalid \escape`（通常是 `config.json` 写了 `C:\xxx` 这种未转义路径）：
  - 把路径改成 `C:/xxx`，或写成 `C:\\xxx`（双反斜杠）。

- 终端长期运行会累积滚动历史显示，但脚本本身不缓存全部日志；如担心终端窗口负担，建议重定向到文件或用任务计划程序后台运行。
- 文件日志已采用轮转（`log_max_bytes` + `log_backup_count`）；一般不需要“清空终端”来控制脚本内存。
