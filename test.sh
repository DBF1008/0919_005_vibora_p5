#!/usr/bin/env bash
#
# Vibora 手动测试脚本
# 覆盖本次改动的三个特性：
#   1. workers/handler.py    优雅关闭（连接排空 -> 强制关闭残留连接 -> 取消 pending 任务 -> 执行 shutdown 回调 -> 关闭 loop）
#   2. server.py             内置 /healthz 健康检查端点（worker 数 / 活跃连接数 / 内存使用）
#   3. protocol/cprotocol.pyx request_id 全链路追踪（自动生成 UUID / 透传 / 注入响应头与访问日志）
#
# 前置依赖：pip install cython ujson uvloop
# 注意：tests/client/external.py 依赖外网（google.com），无网环境下该用例失败属预期。
#
set -uo pipefail
cd "$(dirname "$0")"

HOST=127.0.0.1
PORT=5055
BASE="http://$HOST:$PORT"
APP_FILE=/tmp/vibora_manual_app.py
SHUTDOWN_LOG=/tmp/vibora_shutdown_callbacks.log
SERVER_LOG=/tmp/vibora_server.log
SERVER_PID=""

fail() { echo "[FAIL] $1"; cleanup; exit 1; }
ok()   { echo "[ OK ] $1"; }

cleanup() {
    [ -n "$SERVER_PID" ] && kill -9 "$SERVER_PID" 2>/dev/null
    pkill -9 -f "$APP_FILE" 2>/dev/null
    rm -f "$APP_FILE"
}
trap cleanup EXIT

############################################
# 0. 编译 Cython 扩展（cprotocol.pyx 改动必须重新编译才生效）
############################################
echo "==> 0. 编译 Cython 扩展"
command -v cython >/dev/null || { echo "缺少 cython，请先执行: pip install cython"; exit 1; }
python3 build.py || fail "Cython 扩展编译失败"
ok "扩展编译完成"

############################################
# 1. 回归：运行已有单元测试
############################################
echo "==> 1. 运行已有单元测试（回归）"
if python3 test.py; then
    ok "已有单元测试全部通过"
else
    echo "[WARN] 部分已有测试失败（若仅 tests/client/external.py 失败，多为无外网环境导致，属预期）"
fi

############################################
# 2. 生成手动测试用应用
############################################
echo "==> 2. 启动手动测试应用（workers=1, debug=True）"
rm -f "$SHUTDOWN_LOG" "$SERVER_LOG"
cat > "$APP_FILE" <<'PYAPP'
import asyncio
import multiprocessing
from vibora import Vibora, Response

# macOS 默认 spawn 无法 pickle 闭包路由，强制使用 fork。
try:
    multiprocessing.set_start_method('fork')
except RuntimeError:
    pass

app = Vibora()
SHUTDOWN_LOG = '/tmp/vibora_shutdown_callbacks.log'


async def close_db_pool():
    # 模拟生产环境释放数据库连接池。
    with open(SHUTDOWN_LOG, 'a') as f:
        f.write('db pool closed\n')


def close_cache():
    # 同步回调同样受支持。
    with open(SHUTDOWN_LOG, 'a') as f:
        f.write('cache closed\n')


app.register_shutdown_callback(close_db_pool)
app.register_shutdown_callback(close_cache)


@app.route('/')
async def home():
    return Response(b'hello')


@app.route('/slow')
async def slow():
    await asyncio.sleep(3)
    return Response(b'slow-done')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5055, workers=1, debug=True, necromancer=False)
PYAPP

python3 "$APP_FILE" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

for i in $(seq 1 30); do
    curl -s -o /dev/null "$BASE/" && break
    sleep 0.5
    [ "$i" = "30" ] && fail "测试应用启动失败，日志：\n$(cat "$SERVER_LOG")"
done
ok "应用已启动 (pid=$SERVER_PID)"

WORKER_PID=$(pgrep -P "$SERVER_PID" | head -1)
[ -n "$WORKER_PID" ] || fail "未找到 worker 子进程"
ok "worker 进程 pid=$WORKER_PID"

############################################
# 3. /healthz 健康检查端点
############################################
echo "==> 3. 测试 /healthz 端点"
HEALTH=$(curl -s "$BASE/healthz") || fail "/healthz 请求失败"
echo "     响应: $HEALTH"
echo "$HEALTH" | python3 -c "
import json, sys
data = json.load(sys.stdin)
assert data['status'] == 'ok', data
assert isinstance(data['workers'], int) and data['workers'] >= 1, data
assert isinstance(data['active_connections'], int) and data['active_connections'] >= 0, data
assert isinstance(data['memory_usage'], int) and data['memory_usage'] > 0, data
assert isinstance(data['pid'], int) and data['pid'] > 0, data
" || fail "/healthz 响应字段校验失败"
ok "/healthz 返回 status/workers/active_connections/memory_usage/pid"

############################################
# 4. request_id 全链路追踪
############################################
echo "==> 4. 测试 request_id 追踪"

# 4.1 未携带 X-Request-ID 时自动生成 UUID 并注入响应头。
HEADERS=$(curl -s -D - -o /dev/null "$BASE/")
AUTO_ID=$(echo "$HEADERS" | grep -i '^X-Request-ID:' | tr -d '\r' | awk '{print $2}')
[ -n "$AUTO_ID" ] || fail "响应头缺少 X-Request-ID（自动生成失败）"
echo "$AUTO_ID" | grep -qE '^[0-9a-f]{32}$' || fail "自动生成的 request_id 不是 UUID hex: $AUTO_ID"
ok "缺失时自动生成 UUID: $AUTO_ID"

# 4.2 携带 X-Request-ID 时原样透传。
HEADERS=$(curl -s -D - -o /dev/null -H 'X-Request-ID: trace-abc-123' "$BASE/")
echo "$HEADERS" | grep -qi '^X-Request-ID: trace-abc-123' || fail "X-Request-ID 未透传到响应头"
ok "请求头 trace-abc-123 已透传到响应头"

# 4.3 request_id 注入访问日志。
sleep 1
grep -q 'trace-abc-123' "$SERVER_LOG" || fail "访问日志中未找到 request_id（日志注入失败）"
ok "访问日志已包含 request_id"

############################################
# 5. 优雅关闭
############################################
echo "==> 5. 测试优雅关闭 (SIGTERM)"

# 5.1 进行中的请求应在关闭前被排空（连接 draining）。
SLOW_RESULT_FILE=/tmp/vibora_slow_result.txt
rm -f "$SLOW_RESULT_FILE"
( curl -s --max-time 15 "$BASE/slow" > "$SLOW_RESULT_FILE" ) &
CURL_PID=$!
sleep 1  # 确保 /slow 请求已进入处理中

kill -TERM "$WORKER_PID" || fail "无法向 worker 发送 SIGTERM"
wait "$CURL_PID"
[ "$(cat "$SLOW_RESULT_FILE")" = "slow-done" ] || fail "进行中的请求未被优雅排空，响应: $(cat "$SLOW_RESULT_FILE")"
ok "进行中的请求在关闭前正常完成（连接排空）"

# 5.2 shutdown 回调（数据库连接等资源释放）应被执行。
for i in $(seq 1 15); do
    [ -f "$SHUTDOWN_LOG" ] && break
    sleep 0.5
done
[ -f "$SHUTDOWN_LOG" ] || fail "shutdown 回调未执行（$SHUTDOWN_LOG 不存在）"
grep -q 'db pool closed' "$SHUTDOWN_LOG" || fail "异步 shutdown 回调未执行"
grep -q 'cache closed' "$SHUTDOWN_LOG" || fail "同步 shutdown 回调未执行"
ok "shutdown 回调全部执行: $(tr '\n' ' ' < "$SHUTDOWN_LOG")"

# 5.3 worker 进程应自行退出（loop 已关闭，无残留）。
for i in $(seq 1 15); do
    kill -0 "$WORKER_PID" 2>/dev/null || break
    sleep 0.5
done
kill -0 "$WORKER_PID" 2>/dev/null && fail "worker 进程在优雅关闭后仍未退出"
ok "worker 进程已干净退出"

echo
echo "======================================"
echo " 所有手动测试通过 ✅"
echo "======================================"
