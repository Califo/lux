# lux（飞书长连接 + Redis + Cursor SDK）

在飞书里 @ 机器人并附上 Gerrit change 链接，本地服务拉 Diff，用 Cursor Agent 评审，再把结果发回 Gerrit，并在飞书回执。

## 架构

```
飞书 IM @机器人
    │  WebSocket 长连接（无需公网 webhook）
    ▼
feishu_bot.py  ──LPUSH──►  Redis 队列
                               │
                               ▼ BRPOP
                         local_agent.py
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
         Gerrit Diff     Cursor SDK 评审    飞书通知 / Gerrit Review
```

## 准备

1. **Python ≥ 3.10**
2. **Redis** 本机可访问（默认 `127.0.0.1:6379`）
3. **飞书企业自建应用**
   - 权限：收发消息相关（如 `im:message`、`im:message.group_at_msg` 等，按后台提示开通）
   - 事件：订阅 `im.message.receive_v1`
   - 事件订阅方式选 **使用长连接接收事件**（需先在本机把 `feishu_bot.py` 跑起来再保存）
4. **Gerrit HTTP 密码**：Settings → HTTP Credentials（本服务用 **Digest** 认证访问 `/a/` API）
5. **Cursor API Key**： [Dashboard → Integrations](https://cursor.com/dashboard/integrations)

## 安装

```bash
cd /path/to/lux
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# 编辑 .env，填入 FEISHU_* / GERRIT_* / CURSOR_API_KEY
```

## 配置说明

| 变量 | 含义 |
| --- | --- |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | 飞书应用凭证 |
| `REDIS_*` | 队列连接与 key |
| `GERRIT_HOST` / `GERRIT_USER` / `GERRIT_HTTP_PWD` | Gerrit REST |
| `CURSOR_API_KEY` | Cursor SDK Key |
| `CURSOR_MODEL` | 默认 `grok-4.6`（深度评审）；可用 `composer-2.5` 等更便宜模型 |
| `CURSOR_MODEL_EFFORT` | Claude: `high`/`max`；GPT 则映射为 `reasoning` |
| `CURSOR_MODEL_THINKING` | `true`/`false`，开启模型 thinking |
| `CURSOR_WORKSPACE` | 本地仓库路径；配合只读 tools（read/grep/glob/ls）做上下文评审；空则纯 Diff |
| `DIFF_MAX_CHARS` | 送入模型的 Diff 上限 |
| `FEISHU_PROJECT_PLUGIN_ID` / `FEISHU_PROJECT_PLUGIN_SECRET` | 飞书项目插件凭证（不是 IM 应用的 App ID） |
| `FEISHU_PROJECT_USER_KEY` | 飞书项目里双击头像得到的 user_key |
| `MEDATA_DOWNLOAD_SH` | 本机下载脚本，默认 MeData 的 `data_download.sh` |

## 开机自启（推荐）

飞书长连接只有在本机 `feishu_bot` **已经在跑**时才能收到 `@`。  
因此无法靠「@ 机器人」去唤醒一个完全没启动的进程；正确做法是让服务常驻并自动拉起。

```bash
# 已提供 systemd --user 单元（安装一次即可）
systemctl --user enable --now lux.service
systemctl --user status lux.service

# 常用命令
systemctl --user restart lux.service
systemctl --user stop lux.service
journalctl --user -u lux.service -f
# 业务日志
tail -f logs/feishu_bot.log logs/local_agent.log logs/lux_scan.log logs/lux_watch.log
```

`lux.sh` 会同时跑 `feishu_bot.py`、`local_agent.py`、`lux_scan.py` 和 `lux_watch.py`，崩溃后自动重启。  
启用后请**不要再手动开两份进程**，否则长连接可能抢消息。

## 使用

在群里或单聊对机器人说：

```
@lux review https://gerrit.uisee.ai/#/c/176498/
```

也支持：

- `@我 检查代码 176498`
- `@我 帮忙看看 gerrit 176498`
- 直接贴 Gerrit 链接（链接里已含 gerrit，视为要评审）

触发词：`review` / `检查代码` / `gerrit` / `评审` / `帮忙看` 等。  
再发一条新消息就会再评一次；同一条飞书消息不会因重推重复入队。

其他话题会礼貌说明，并列出当前能力（`common.py` 里的 `BOT_CAPABILITIES`，后续加功能改这里）。  
文字回复会尽量以**话题**形式挂在你的原消息下。  
Review 完成后先发**预览**（不写 Gerrit），并 @ 你确认：

- ✅ `CheckMark`：发布到 Gerrit  
- ❌ `CrossMark`：不发布  
- `OnIt`：重新 Review  

请在飞书后台额外订阅事件：`im.message.reaction.created_v1`。

严重级别（是否必修）：

| 标签 | 要求 | 含义 |
| --- | --- | --- |
| **P0** | **必修** | 崩溃 / 致命正确性问题，合入前必须修 |
| **P1** | **必修** | 高风险 / 很可能有功能 bug，合入前必须修 |
| **P2** | 建议修 | 推荐改，可不阻塞合入 |
| **P3** | 非必修 | 风格 / 注释等小建议，可选 |

## 下载飞书项目里的数据

在群里或单聊对机器人说（带「下载」，或只贴项目链接）：

```
@lux 下载 https://project.feishu.cn/空间简称/issue/detail/123456789
```

机器人会读取该工作项的字段和评论，抽出 `*.carN_log_part_lidar_pcap_cut_时间_时间`，再调用 `data_download.sh`。下载可能要几分钟，结果会回到原消息的话题里。本地已经有这份数据（`part_info` 或 pcap 目录里已有文件）会跳过。

另外，`lux_scan.py` 会在每天 **22:00** 和 **06:00**（`Asia/Shanghai`，可用 `LUX_SCAN_TIMES` 改）自动扫一遍插件能看到的空间：

- 负责人、经办人、当前操作人或角色负责人是你的缺陷（新的、老的都看）
- 上次扫描之后，评论里新 @ 了你的缺陷（第一次默认回溯 48 小时）

扫到的闭环数据名，本地还没有的才入队下载，已经下过的跳过。也可以随时在飞书里对机器人说 **「扫描」**，马上扫一遍，结果回到这条消息里。日志在 `logs/lux_scan.log`。

## 被加为 reviewer

`lux_watch.py` 大约每 3 分钟看一次 Gerrit。你被加为 reviewer、当前补丁还没打 Code-Review，会先发一条飞书。你回 **「评」**（或「评 123456」）才开始评审。直接发 change 链接仍然会马上评。

## 自己的 change

同一轮询里，你名下还开着的 change 如果 CI 变红，或有别人留了新评论，会推一条短消息：change 号、失败阶段、日志末尾几行报错。不会贴整段 console。通知发到 `LUX_NOTIFY_CHAT_ID`；没配的话，发到你最近一次跟机器人说话的会话。所以先在那个群或单聊里 @ 它一次。

## 飞书项目插件

插件只用来读数据，不用做界面控件，凭证和飞书 IM 应用不是同一套。

1. 打开 [飞书项目开放平台](https://project.feishu.cn/openapp/home)（项目页左下角头像 → 开发者后台），**我的插件 → 添加插件**。
2. 在插件 **基本信息** 复制 Plugin ID、Plugin Secret。
3. **权限管理** 勾选读取类权限：获取空间详情、获取工作项、查询评论。保存后到 **插件发布** 发一个新版本，权限才会生效。
4. 进入要读的那个空间：**空间配置 → 插件管理 → 安装插件**，选刚发布的这个插件。你不是空间管理员的话，把插件名发给管理员代装。权限有改动时要重新发版本，空间里的插件才会跟上。
5. 在飞书项目页面点左下角头像，再双击弹层里的小头像，复制 **user_key**。
6. 把下面三项写进本目录 `.env`，然后重启：

```bash
# .env
FEISHU_PROJECT_PLUGIN_ID=...
FEISHU_PROJECT_PLUGIN_SECRET=...
FEISHU_PROJECT_USER_KEY=...

systemctl --user restart lux.service
```

## 文件

| 文件 | 作用 |
| --- | --- |
| `feishu_bot.py` | 长连接收消息、解析 change / 项目链接、入队、即时回执 |
| `local_agent.py` | 消费队列、Cursor 评审、写 Gerrit、调用本地下载 |
| `feishu_project.py` | 飞书项目 Open API、抽出数据名、判断本地是否已下载 |
| `lux_scan.py` | 定时或手动扫描指派和评论 @，未下载的闭环数据入队 |
| `lux_watch.py` | 轮询 Gerrit：reviewer 提醒、CI 失败、新评论 |
| `common.py` | Redis / 飞书 token 缓存 / 链接解析 |
| `.env.example` | 配置模板（勿把真实 `.env` 提交进 git） |

## 注意

- 长连接要求进程能访问公网；同一应用多实例时消息只会被其中一个连接收到。
- 事件处理需尽快返回，入队与回执已放到后台线程。
- Cursor 调用按账号用量计费，与 IDE Agent 同类。
- `.env` 含密钥，已加入 `.gitignore`。
