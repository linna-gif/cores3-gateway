"""
CoreS3 MCP Gateway
Let Claude App control CoreS3 via MCP
"""

import os
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

API_KEY = os.environ.get("CORES3_API_KEY", "")

pending_commands = []
command_results = {}
device_status = {
    "online": False,
    "last_seen": 0,
    "ip": None
}

MCP_TOOLS = [
    {
        "name": "take_photo",
        "description": "Take a photo using CoreS3 camera. Use this to see the surroundings.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "set_expression",
        "description": "Show an expression on CoreS3 screen. Options: happy, sad, angry, surprised, love, sleepy, neutral",
        "inputSchema": {
            "type": "object",
            "properties": {
                "expression": {
                    "type": "string",
                    "description": "Expression type",
                    "enum": ["happy", "sad", "angry", "surprised", "love", "sleepy", "neutral"]
                }
            },
            "required": ["expression"]
        }
    },
    {
        "name": "set_screen_text",
        "description": "Display text on CoreS3 screen",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Text to display"
                }
            },
            "required": ["text"]
        }
    },
    {
        "name": "get_touch",
        "description": "Check if CoreS3 touchscreen is being touched and where",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "get_device_status",
        "description": "Check if CoreS3 device is online",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
]


def send_command_and_wait(action, params=None):
    cmd_id = str(uuid.uuid4())[:8]
    cmd = {
        "id": cmd_id,
        "action": action,
        "params": params or {},
        "timestamp": time.time()
    }
    pending_commands.append(cmd)
    return cmd_id


async def wait_for_result(cmd_id, timeout=15):
    for _ in range(timeout * 2):
        if cmd_id in command_results:
            result = command_results.pop(cmd_id)
            return result
        await asyncio.sleep(0.5)
    return {"error": "CoreS3 not responding. Device may be offline."}


async def execute_tool(tool_name, arguments):
    if tool_name == "get_device_status":
        is_online = (time.time() - device_status["last_seen"]) < 10
        if is_online:
            return "CoreS3 is online, IP: {}, last active: just now".format(device_status["ip"])
        else:
            return "CoreS3 is currently offline. Please make sure it is powered on and connected to WiFi."

    if (time.time() - device_status["last_seen"]) > 10:
        return "CoreS3 is currently offline. Cannot execute command."

    if tool_name == "take_photo":
        cmd_id = send_command_and_wait("take_photo")
        result = await wait_for_result(cmd_id, timeout=10)
        if "error" in result:
            return result["error"]
        return "Photo taken. Description: {}".format(result.get("description", "unable to describe"))

    elif tool_name == "set_expression":
        expr = arguments.get("expression", "neutral")
        cmd_id = send_command_and_wait("set_expression", {"expression": expr})
        result = await wait_for_result(cmd_id, timeout=5)
        if "error" in result:
            return result["error"]
        return "Now showing {} expression on screen".format(expr)

    elif tool_name == "set_screen_text":
        text = arguments.get("text", "")
        cmd_id = send_command_and_wait("set_screen_text", {"text": text})
        result = await wait_for_result(cmd_id, timeout=5)
        if "error" in result:
            return result["error"]
        return "Now showing text on screen: {}".format(text)

    elif tool_name == "get_touch":
        cmd_id = send_command_and_wait("get_touch")
        result = await wait_for_result(cmd_id, timeout=5)
        if "error" in result:
            return result["error"]
        if result.get("touched"):
            return "Screen is being touched at x={}, y={}".format(result["x"], result["y"])
        else:
            return "Screen is not being touched right now"

    return "Unknown tool: {}".format(tool_name)


def check_auth(request):
    if not API_KEY:
        return True
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        token = auth[7:]
        if token == API_KEY:
            return True
    token = request.query_params.get("api_key", "")
    if token == API_KEY:
        return True
    return False


async def mcp_sse_endpoint(request):
    if not check_auth(request):
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    session_id = str(uuid.uuid4())
    logger.info("MCP client connected: {}".format(session_id))

    async def event_generator():
        yield {
            "event": "endpoint",
            "data": "/mcp/messages?session_id={}".format(session_id)
        }
        while True:
            await asyncio.sleep(15)
            yield {"event": "ping", "data": ""}

    return EventSourceResponse(event_generator())


async def mcp_messages_endpoint(request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}})

    method = body.get("method", "")
    msg_id = body.get("id")
    params = body.get("params", {})

    logger.info("MCP method: {}, id: {}".format(method, msg_id))

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
                        {"type": "text", "text": "Error: {}".format(str(e))}
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
            "error": {"code": -32601, "message": "Method not found: {}".format(method)}
        }

    return JSONResponse(response)


async def device_poll(request):
    device_status["online"] = True
    device_status["last_seen"] = time.time()
    device_status["ip"] = request.client.host if request.client else "unknown"

    if pending_commands:
        cmd = pending_commands.pop(0)
        return JSONResponse(cmd)

    return JSONResponse({"action": "none"})


async def device_result(request):
    try:
        data = await request.json()
        cmd_id = data.get("id")
        result = data.get("result", {})

        if cmd_id:
            command_results[cmd_id] = result
            logger.info("Received result for command {}".format(cmd_id))

        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error("Error processing result: {}".format(e))
        return JSONResponse({"status": "error", "message": str(e)})


async def device_heartbeat(request):
    device_status["online"] = True
    device_status["last_seen"] = time.time()
    device_status["ip"] = request.client.host if request.client else "unknown"
    return JSONResponse({"status": "ok", "timestamp": time.time()})


async def index(request):
    is_online = (time.time() - device_status["last_seen"]) < 10
    status_text = "Online" if is_online else "Offline"

    html = """
    <html>
    <head><title>CoreS3 MCP Gateway</title></head>
    <body style="font-family: sans-serif; padding: 40px; max-width: 600px; margin: 0 auto;">
        <h1>CoreS3 MCP Gateway</h1>
        <p>Device status: {}</p>
        <p>Device IP: {}</p>
        <p>Pending commands: {}</p>
        <hr>
        <h3>MCP Connection URL</h3>
        <p>Add MCP connector in Claude App with URL:</p>
        <code style="background: #f0f0f0; padding: 8px; display: block;">
        https://cores3-gateway.zeabur.app/mcp/sse
        </code>
    </body>
    </html>
    """.format(status_text, device_status.get("ip", "None"), len(pending_commands))
    return HTMLResponse(html)


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
