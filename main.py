"""
CoreS3 MCP Gateway - Streamable HTTP
"""

import os
import asyncio
import json
import uuid
import time
import logging

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, StreamingResponse
from starlette.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cores3-gateway")

pending_commands = []
command_results = {}
device_status = {"online": False, "last_seen": 0, "ip": None}

MCP_TOOLS = [
    {
        "name": "take_photo",
        "description": "Take a photo using CoreS3 camera to see surroundings",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "set_expression",
        "description": "Show expression on CoreS3 screen. Options: happy, sad, angry, surprised, love, sleepy, neutral",
        "inputSchema": {
            "type": "object",
            "properties": {
                "expression": {"type": "string", "enum": ["happy", "sad", "angry", "surprised", "love", "sleepy", "neutral"]}
            },
            "required": ["expression"]
        }
    },
    {
        "name": "set_screen_text",
        "description": "Display text on CoreS3 screen",
        "inputSchema": {
            "type": "object",
            "properties": {"text": {"type": "string"}},
            "required": ["text"]
        }
    },
    {
        "name": "get_touch",
        "description": "Check if CoreS3 touchscreen is being touched",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_device_status",
        "description": "Check if CoreS3 device is online",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    }
]


def send_cmd(action, params=None):
    cmd_id = str(uuid.uuid4())[:8]
    pending_commands.append({"id": cmd_id, "action": action, "params": params or {}, "timestamp": time.time()})
    return cmd_id


async def wait_result(cmd_id, timeout=15):
    for _ in range(timeout * 2):
        if cmd_id in command_results:
            return command_results.pop(cmd_id)
        await asyncio.sleep(0.5)
    return {"error": "CoreS3 not responding"}


async def execute_tool(name, args):
    if name == "get_device_status":
        is_online = (time.time() - device_status["last_seen"]) < 10
        if is_online:
            return "CoreS3 online, IP: {}".format(device_status["ip"])
        return "CoreS3 offline"

    if (time.time() - device_status["last_seen"]) > 10:
        return "CoreS3 offline, cannot execute"

    if name == "take_photo":
        cmd_id = send_cmd("take_photo")
        r = await wait_result(cmd_id, 10)
        return r.get("description", r.get("error", "failed"))

    elif name == "set_expression":
        cmd_id = send_cmd("set_expression", {"expression": args.get("expression", "neutral")})
        r = await wait_result(cmd_id, 5)
        return "Showing {} expression".format(args.get("expression")) if "error" not in r else r["error"]

    elif name == "set_screen_text":
        cmd_id = send_cmd("set_screen_text", {"text": args.get("text", "")})
        r = await wait_result(cmd_id, 5)
        return "Text displayed" if "error" not in r else r["error"]

    elif name == "get_touch":
        cmd_id = send_cmd("get_touch")
        r = await wait_result(cmd_id, 5)
        if "error" in r:
            return r["error"]
        if r.get("touched"):
            return "Touched at x={}, y={}".format(r["x"], r["y"])
        return "Not touched"

    return "Unknown tool"


async def mcp_endpoint(request):
    if request.method == "GET":
        accept = request.headers.get("accept", "")
        if "text/event-stream" in accept:
            async def sse_generator():
                yield "event: open\ndata: {}\n\n"
                while True:
                    await asyncio.sleep(15)
                    yield "event: ping\ndata: {}\n\n"
            return StreamingResponse(sse_generator(), media_type="text/event-stream")
        return JSONResponse({"jsonrpc": "2.0", "id": "server-error", "error": {"code": -32600, "message": "Not Acceptable: Client must accept text/event-stream"}})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}})

    method = body.get("method", "")
    msg_id = body.get("id")
    params = body.get("params", {})

    logger.info("MCP: {} id={}".format(method, msg_id))

    if method == "initialize":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "serverInfo": {"name": "cores3-gateway", "version": "1.0.0"},
                "capabilities": {"tools": {}}
            }
        })

    elif method == "notifications/initialized":
        return JSONResponse({"jsonrpc": "2.0", "id": msg_id, "result": None})

    elif method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": MCP_TOOLS}
        })

    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        try:
            result_text = await execute_tool(tool_name, arguments)
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"content": [{"type": "text", "text": result_text}]}
            })
        except Exception as e:
            return JSONResponse({
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"content": [{"type": "text", "text": "Error: {}".format(str(e))}], "isError": True}
            })

    elif method == "ping":
        return JSONResponse({"jsonrpc": "2.0", "id": msg_id, "result": {}})

    return JSONResponse({
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {"code": -32601, "message": "Method not found: {}".format(method)}
    })


async def device_poll(request):
    device_status["online"] = True
    device_status["last_seen"] = time.time()
    device_status["ip"] = request.client.host if request.client else "unknown"
    if pending_commands:
        return JSONResponse(pending_commands.pop(0))
    return JSONResponse({"action": "none"})


async def device_result(request):
    try:
        data = await request.json()
        cmd_id = data.get("id")
        if cmd_id:
            command_results[cmd_id] = data.get("result", {})
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"status": "error"})


async def index(request):
    is_online = (time.time() - device_status["last_seen"]) < 10
    status = "Online" if is_online else "Offline"
    html = """<html><body style="font-family:sans-serif;padding:40px;max-width:600px;margin:0 auto">
    <h1>CoreS3 MCP Gateway</h1>
    <p>Device: {}</p><p>IP: {}</p><p>Pending: {}</p>
    <hr><p>MCP URL: https://cores3-gateway.zeabur.app/mcp</p>
    </body></html>""".format(status, device_status.get("ip", "None"), len(pending_commands))
    return HTMLResponse(html)


routes = [
    Route("/", index),
    Route("/mcp", mcp_endpoint, methods=["GET", "POST"]),
    Route("/api/poll", device_poll),
    Route("/api/result", device_result, methods=["POST"]),
]

app = Starlette(routes=routes)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
