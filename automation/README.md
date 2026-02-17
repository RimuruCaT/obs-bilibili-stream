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
- `runtime.auto_stop_bilibili_on_manual_obs_stop`：若检测到你在 OBS 手动点了“停止推流”，脚本会自动同步调用 B 站停播。

如果你明确不想读取插件配置，可把 `integration.use_obs_plugin_config` 改为 `false`，再手动填写 `bilibili.room_id/csrf_token/cookies`。

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

- **OBS 手动停播联动**：你在 OBS 手动点击停止推流后，脚本检测到流状态从活跃变为停止，会自动调用 B 站停播接口，避免还要在插件里再点一次“停止直播”。
- **重试机制**：接口/OBS 调用失败自动重试（指数退避 + 抖动）。
- **状态文件**：`runtime.state_file` 记录当天阶段状态，避免重复执行。
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
- 若 B 站返回人脸认证（例如 code `60024`），需要人工完成后才能继续自动流程。
- Cookie 过期时，可重新在插件里扫码登录，脚本会自动读到新值。
