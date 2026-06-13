"""
CoreS3 MCP Gateway
让 Claude App 通过 MCP 控制 CoreS3 设备
"""

import asyncio
import json
import uuid
import time
import logging
from contextlib import asynccontextmanager

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse
from starlette.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cores3-gateway")

# ============================================================
# 命令队列系统
# CoreS3 定期轮询这里获取 Claude 发来的指令
# ============================================================

pending_commands = []       # 等待 CoreS3 执行的命令
command_results = {}        # CoreS3 执行完的结果
device_status = {
    "online": False,
    "last_seen": 0,
    "ip": None
}

# ============================================================
# MCP 协议实现 (JSON-RPC over SSE)
# ============================================================

MCP_TOOLS = [
    {
        "name": "take_photo",
        "description": "让 CoreS3 用摄像头拍一张照片，返回照片描述。用这个来看看周围的环境。",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "set_expression",
        "description": "在 CoreS3 的屏幕上显示一个表情。可选: happy, sad, angry, surprised, love, sleepy, neutral",
        "inputSchema": {
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "表情类型",
                    "enum": ["happy", "sad", "angry", "surprised", "love", "sleepy", "neutral"]
                }
            },
            "required": ["expression"]
        }
    },
    {
        "name": "set_screen_text",
        "description": "在 CoreS3 的屏幕上显示一段文字",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "要显示的文字"
                }
            },
            "required": ["text"]
        }
    },
    {
        "name": "get_touch",
        "description": "检查 CoreS3 的触摸屏是否正在被触摸，以及触摸的位置",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_device_status",
        "description": "查看 CoreS3 设备是否在线",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]


def send_command_and_wait(action: str, params: dict = None):
    """发送命令给 CoreS3 并等待结果（同步版本，供 MCP 调用）"""
    cmd_id = str(uuid.uuid4())[:8]
    cmd = {
        "id": cmd_id,
        "action": action,
        "params": params or {},
        "timestamp": time.time()
    }
    pending_commands.append(cmd)
    return cmd_id


async def wait_for_result(cmd_id: str, timeout: int = 15):
    """等待 CoreS3 返回结果"""
    for _ in range(timeout * 2):
        if cmd_id in command_results:
            result = command_results.pop(cmd_id)
            return result
        await asyncio.sleep(0.5)
    return {"error": "CoreS3 没有响应，设备可能不在线。"}


async def execute_tool(tool_name: str, arguments: dict) -> str:
    """执行 MCP 工具调用"""
    
    if tool_name == "get_device_status":
        is_online = (time.time() - device_status["last_seen"]) < 10
        if is_online:
            return f"CoreS3 在线，IP: {device_status['ip']}，最后活跃: 刚刚"
        else:
            return "CoreS3 当前不在线。请确认设备已开机并连接 WiFi。"
    
    # 检查设备是否在线
    if (time.time() - device_status["last_seen"]) > 10:
        return "CoreS3 当前不在线，无法执行命令。请确认设备已开机并连接 WiFi。"
    
    if tool_name == "take_photo":
        cmd_id = send_command_and_wait("take_photo")
        result = await wait_for_result(cmd_id, timeout=10)
        if "error" in result:
            return result["error"]
        return f"拍到了一张照片。画面描述: {result.get('description', '无法描述')}"
    
    elif tool_name == "set_expression":
        expr = arguments.get("expression", "neutral")
        cmd_id = send_command_and_wait("set_expression", {"expression": expr})
        result = await wait_for_result(cmd_id, timeout=5)
        if "error" in result:
            return result["error"]
        return f"已在屏幕上显示 {expr} 表情"
    
    elif tool_name == "set_screen_text":
        text = arguments.get("text", "")
        cmd_id = send_command_and_wait("set_screen_text", {"text": text})
        result = await wait_for_result(cmd_id, timeout=5)
        if "error" in result:
            return result["error"]
        return f"已在屏幕上显示文字: {text}"
    
    elif tool_name == "get_touch":
        cmd_id = send_command_and_wait("get_touch")
        result = await wait_for_result(cmd_id, timeout=5)
        if "error" in result:
            return result["error"]
        if result.get("touched"):
            return f"屏幕正在被触摸，位置: x={result['x']}, y={result['y']}"
        else:
            return "屏幕当前没有被触摸"
    
    return f"未知工具: {tool_name}"


# ============================================================
# MCP SSE 端点 (Claude App 连接这里)
# ============================================================

async def mcp_sse_endpoint(request: Request):
    """MCP SSE 连接端点"""
    session_id = str(uuid.uuid4())
    logger.info(f"MCP client connected: {session_id}")
    
    async def event_generator():
        # 发送 endpoint 信息
        yield {
            "event": "endpoint",
            "data": f"/mcp/messages?session_id={session_id}"
        }
        
        # 保持连接
        while True:
            await asyncio.sleep(15)
            yield {"event": "ping", "data": ""}
    
    return EventSourceResponse(event_generator())


# 存储 SSE 响应回调
mcp_sessions = {}


async def mcp_messages_endpoint(request: Request):
    """处理 MCP JSON-RPC 消息"""
    session_id = request.query_params.get("session_id", "")
    
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}})
    
    method = body.get("method", "")
    msg_id = body.get("id")
    params = body.get("params", {})
    
    logger.info(f"MCP method: {method}, id: {msg_id}")
    
    # 处理各种 MCP 方法
    if method == "initialize":
        response = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": "cores3-gateway",
                    "version": "1.0.0"
                },
                "capabilities": {
                    "tools": {}
                }
            }
        }
    
    elif method == "notifications/initialized":
        return JSONResponse({"jsonrpc": "2.0", "id": msg_id, "result": None})
    
    elif method == "tools/list":
        response = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "tools": MCP_TOOLS
            }
        }
    
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        
        try:
            result_text = await execute_tool(tool_name, arguments)
            response = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [
                        {"type": "text", "text": result_text}
                    ]
                }
            }
        except Exception as e:
            response = {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [
                        {"type": "text", "text": f"执行出错: {str(e)}"}
                    ],
                    "isError": True
                }
            }
    
    elif method == "ping":
        response = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
    
    else:
        response = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {"code": -32601, "message": f"Method not found: {method}"}
        }
    
    return JSONResponse(response)


# ============================================================
# CoreS3 轮询端点 (CoreS3 设备连接这里)
# ============================================================

async def device_poll(request: Request):
    """CoreS3 轮询获取待执行的命令"""
    device_status["online"] = True
    device_status["last_seen"] = time.time()
    device_status["ip"] = request.client.host if request.client else "unknown"
    
    if pending_commands:
        cmd = pending_commands.pop(0)
        return JSONResponse(cmd)
    
    return JSONResponse({"action": "none"})


async def device_result(request: Request):
    """CoreS3 提交命令执行结果"""
    try:
        data = await request.json()
        cmd_id = data.get("id")
        result = data.get("result", {})
        
        if cmd_id:
            command_results[cmd_id] = result
            logger.info(f"Received result for command {cmd_id}")
        
        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Error processing result: {e}")
        return JSONResponse({"status": "error", "message": str(e)})


async def device_heartbeat(request: Request):
    """CoreS3 心跳"""
    device_status["online"] = True
    device_status["last_seen"] = time.time()
    device_status["ip"] = request.client.host if request.client else "unknown"
    return JSONResponse({"status": "ok", "timestamp": time.time()})


# ============================================================
# 状态页面
# ============================================================

async def index(request: Request):
    """状态页面"""
    is_online = (time.time() - device_status["last_seen"]) < 10
    status = "🟢 在线" if is_online else "🔴 离线"
    
    html = f"""
    <html>
    <head><title>CoreS3 MCP Gateway</title></head>
    <body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: 0 auto;">
        <h1>CoreS3 MCP Gateway</h1>
        <p>设备状态: {status}</p>
        <p>设备 IP: {device_status.get('ip', '未知')}</p>
        <p>待执行命令: {len(pending_commands)}</p>
        <hr>
        <h3>MCP 连接地址</h3>
        <p>在 Claude App 中添加 MCP 连接器，URL 填写:</p>
        <code style="background: #f0f0f0; padding: 8px; display: block;">
        https://你的域名/mcp/sse
        </code>
    </body>
    </html>
    """
    return HTMLResponse(html)


# ============================================================
# 应用路由
# ============================================================

routes = [
    Route("/", index),
    Route("/mcp/sse", mcp_sse_endpoint),
    Route("/mcp/messages", mcp_messages_endpoint, methods=["POST"]),
    Route("/api/poll", device_poll),
    Route("/api/result", device_result, methods=["POST"]),
    Route("/api/heartbeat", device_heartbeat, methods=["POST"]),
]

app = Starlette(routes=routes)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
