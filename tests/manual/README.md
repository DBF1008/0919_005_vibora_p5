# 手动测试说明（test.sh）

覆盖本次三处联动改动：

| 场景 | 脚本 | 验证点 |
|------|------|--------|
| 优雅关闭（单元） | `test_graceful_shutdown.py` | 回调注册/执行（同步+异步）、pending 任务取消、fork 前注册保留、异常回调不阻断关闭 |
| 优雅关闭（进程级） | `test_graceful_shutdown_live.py` | SIGTERM 后进程 0 退出、DB 回调/同步回调落盘、端口释放、关闭日志 |
| /healthz | `test_healthz.py` | 200、`status/pid/workers/active_connections/memory.rss_bytes/uptime/request_id` 字段、活跃连接计数 |
| request_id | `test_request_id.py` | 缺省生成 32 位 UUID hex、每请求唯一、透传客户端 `X-Request-ID`、注入响应头与日志 |

## 运行

```bash
# 需要先编译 Cython 扩展（仓库自带 build.py）：
python build.py

# 全部测试（单元 + 真实 fork worker 的 HTTP 集成测试）：
./test.sh

# 只跑不依赖网络的单元测试：
./test.sh --unit

# 一步编译 + 全量测试：
PYTHON=python3 ./test.sh --build
```

环境变量：`PYTHON`（解释器，默认 `python3`）、`VIBORA_TEST_PORT`（默认 8097，三个集成用例分别使用 8097/8098/8099）。

> `_compat.py` 仅用于让这份 2018 年代码库在 Python 3.10+ 上运行手工脚本
> （`collections.Callable` 与 macOS fork 启动方式），不被任何生产代码引用。
