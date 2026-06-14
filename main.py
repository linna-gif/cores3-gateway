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


def check_auth(request)
