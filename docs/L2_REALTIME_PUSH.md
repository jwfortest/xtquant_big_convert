# L2 原生实时推送（Redis，fork 分支）

此分支将 `xtdata.subscribe_quote(..., period='l2transaction'/'l2order'/'l2quote')`
从 K 线轮询中分离，改为 **大 QMT 原生回调 → 有界内存队列 → Redis Stream → 外部 callback**。
不通过 `get_market_data_ex(count=1)` 拼装逐笔，不合并为最新一条，不按市场时间戳去重。
普通 tick/full_tick 仍走原来的全推通道；K 线仍走原来的轮询。

这不是已发布 PyPI 版本的能力。需要安装此 fork 分支，并同步升级 QMT 服务端。
支持 `l2quote`、`l2quoteaux`、`l2transaction`、`l2order`、`l2transactioncount`、`l2orderqueue`。
权限、数据品种和逐笔覆盖取决于实际券商终端；注册成功、没有报错不代表已有有效 L2。

## 使用

客户端从本 fork 安装：

```powershell
python -m pip install -e ".[redis]"
```

使用既有 Redis 客户端配置，调用方式保持 MiniQMT 风格：

```python
from xtquant import xtdata

def on_transactions(batch):
    # {股票代码: [记录1, 记录2, ...]}，同一批次所有记录均保留。
    # 此处仅做轻量入队；慢回调会使 Redis 保留窗口耗尽。
    for code, records in batch.items():
        for record in records:
            consume(code, record)

sid = xtdata.subscribe_quote('000001.SZ', period='l2transaction', callback=on_transactions)
print(xtdata.l2_subscription_status())
# 使用完毕：
xtdata.unsubscribe_quote(sid)
# 回收本对象的所有 L2 流及 K 线轮询：xtdata.stop_all_subscriptions()
```

L2 订阅仅接收实时回调，`start_time/end_time` 必须为空、`count` 必须为 0（默认）。
非默认历史参数明确报错，请单独查询历史。此次未改造 `get_l2_*` 查询方法的原生 SDK 路由。
非 Redis 传输明确报错，不偷偷退回轮询。旧服务端缺少对应 RPC 时也明确失败。

服务端使用现有 `quote_push` 配置，Redis 模式下默认创建惰性的 L2 管理器：
**没有客户端请求就不新增行情订阅、不启动发送线程**。`quote_push.enabled=False`
或 `quote_push.l2.enabled=False` 可禁用。

```python
BIGQMT_REDIS_CONFIG = {
    # 原有 host/port/db/transport 配置……
    'quote_push': {
        'enabled': True,
        'l2': {
            'enabled': True,
            'max_subscriptions': 128,       # 原生股票+周期组合上限
            'queue_batches': 1024,          # 所有 L2 流共用有界队列
            'queue_bytes': 16 * 1024 * 1024,
            'batch_bytes': 256 * 1024,      # 单回调复制的保守内存预算
            'copy_budget_seconds': 0.005,   # 超预算报故障，不截断批次
            'stream_maxlen': 256,           # 每条流最多保留256个批次，不是256条逐笔
            'stream_ttl_seconds': 90,
            'heartbeat_timeout_seconds': 30,
        },
    },
}
```

若使用显式 RPC 白名单，应加入 `subscribe_l2_quote`、`unsubscribe_l2_quote`、
`l2_keepalive`、`l2_subscription_status`。下单开关与 L2 无关。
原生订阅和退订强制在 QMT 策略线程执行，即使监听线程配置为 `*` 也不改变这一点。
既有 `adjust` 定时回调必须正常运行，以处理订阅/退订/租约回收，并让内置解释器调度后台线程。
行情发送由原生回调唤醒，不等待“三秒轮询”。实际终端的 GIL 调度仍需盘中测量。

## 数据完整性与故障

- 服务端请求 `ContextInfo.subscribe_quote(..., result_type='list')`。支持字段数组、
  行列表及单行字典；字段数组按 time/stime/timetag 的数组维度转为行，十档盘口数组不视为多笔。
  不支持的形状、字段长度不一致、NaN/inf 或超限批次明确报告故障。
- 保留源字段、源时间、源序号、数量单位及重复记录。桥接额外的 epoch/sequence 标识
  **传输批次**，不是交易所序号；不能用它证明交易所到 QMT 之间没有丢数。
- 每个股票+周期共享一个原生订阅和独立 Stream，多客户端引用计数；注册时内联回调可保留。
  发送线程承担 JSON 序列化及 Redis I/O；原生回调只做有限复制和非等待入队。
- 发送队列同时限制批次数和保守预约字节数，网络在途批次仍占预约容量。
  每条 Redis Stream 严格限制长度并带 TTL；总 Redis 内存还需结合流数、每批大小和
  Redis `maxmemory` 规划，不能把单队列16MB上限当作总进程/Redis内存上限。
- Redis 客户端短暂断连后按原游标继续读取保留的记录。每批检查序号；发现保留窗口
  被裁剪造成缺口、服务端写入/复制/入队失败、源订阅丢失或回调异常，会锁定故障，
  停止向该订阅的应用 callback 交付，并通过错误日志和状态接口报告。
  **不宣称无损恢复、不补造缺失数据、不自动用快照替代逐笔**。
- 故障后由调用者查看原因并显式退订/重新订阅；新的 epoch 表示新的连续性范围。
  共享流故障时，其所有旧引用需释放后才能重建。仅故障日志不是策略风控接入：
  上层实盘系统还必须消费状态/新鲜度并执行自己的降级保护。
- `l2_subscription_status()` 的 healthy 只表示桥接连续性与心跳，**不证明行情权限、
  市场时间新鲜度或数据内容正确**。没有回调的非交易时段不能判定无权限。
- 客户端心跳每轮批量续约。进程退出或失联后由服务端租约回收订阅；显式退订忽略迟到回调。
  QMT 退订报错时保留句柄，后续 reaper/stop 重试，不把未释放的源订阅当作已释放。

## 独立只读验证

不接入交易项目、不运行交易策略即可验证。准备程序只向独立目录写文件，不修改券商安装：

```powershell
python scripts/prepare_l2_verification.py
```

QMT 缺少 `_socket` 等内置依赖时，可重复传入 `--extra-path`，指定与 QMT **相同版本/架构**
的扩展模块目录、标准库 ZIP 和兼容的 redis-py wheel。不得用 Python3.11 扩展替换 QMT3.6 DLL。
准备程序打印三行编辑器引导代码，将它保存为独立 QMT 策略并在模拟模式运行。
生成包使用内容哈希命名，避免覆盖其他正在运行策略的 Python 模块。

默认监听 `127.0.0.1:16379`、DB12、命名空间 `L2_VERIFY_ONLY`；Redis 服务需事先运行。
该入口只开放 ping 和 L2 订阅管理，仅允许 000001.SZ、600000.SH 的三类核心 L2 周期。
入口启动本身不订阅；随后执行：

```powershell
python scripts/verify_l2_realtime.py --seconds 60
```

客户端最多订阅六路，保存每路计数和首尾原始样本到 `.local/state/l2-realtime-result.json`，
结束时退订（失败会保留错误并依靠租约兜底）。停止该 QMT 验证策略可释放所有验证资源。
本脚本是有界观察工具，不进行下单、账户查询、历史下载或全市场压力测试。

盘中验收须分别检查：

1. 原生回调有非空、有效的数据；十档价量结构、逐笔字段及原始市场时间正确。
2. QMT 接收批次/记录数与 Redis/客户端批次序号相符；对比时冻结或明确采集边界，
   运行中先后读取的计数存在自然时间差，不应直接当成丢数。
3. 原始成交/委托序号及记录内容一致，故障注入后的缺口能被准确报告。
4. 分别测源到 QMT、QMT 到客户端及应用处理耗时，再逐步扩展股票数量；
   合成测试通过不代表真实逐笔性能或交易权限验收通过。

参考：[迅投 L2 原生订阅示例](https://dict.thinktrader.net/innerApi/code_examples.html)。

## 离线回归

```powershell
python -m pytest tests/bigqmt_signal_trader/test_l2_realtime_push.py -q
# 可选：仅指定独立测试 Redis。测试仅清理自身 UUID Stream，不清库。
$env:BIGQMT_TEST_REDIS_URL = 'redis://127.0.0.1:16379/12'
python -m pytest tests/bigqmt_signal_trader/test_l2_realtime_push.py -q
```
