"""
DIA v2 - Microsoft Teams Bot
===============================
Minimal Teams bot that calls the same FastAPI backend.
Shows enterprise integration capability during the demo.

Setup:
1. Register a Bot in Azure Bot Service (https://portal.azure.com)
2. Set TEAMS_APP_ID and TEAMS_APP_PASSWORD in .env
3. Run: python teams/bot.py
4. Configure messaging endpoint in Azure: https://your-url/api/messages
"""

import os
import json
import aiohttp
from aiohttp import web
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
    ActivityHandler,
)
from botbuilder.schema import Activity, ActivityTypes

# Configuration
APP_ID = os.getenv("TEAMS_APP_ID", "")
APP_PASSWORD = os.getenv("TEAMS_APP_PASSWORD", "")
DIA_API_URL = os.getenv("DIA_API_URL", "http://localhost:8000")

settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
adapter = BotFrameworkAdapter(settings)


class DIATeamsBot(ActivityHandler):
    """Teams bot that forwards queries to DIA backend."""

    async def on_message_activity(self, turn_context: TurnContext):
        query = turn_context.activity.text
        if not query:
            return

        # Show typing indicator
        typing_activity = Activity(type=ActivityTypes.typing)
        await turn_context.send_activity(typing_activity)

        # Call DIA backend
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{DIA_API_URL}/query",
                    json={"query": query, "session_id": f"teams-{turn_context.activity.from_property.id}"},
                    timeout=aiohttp.ClientTimeout(total=60),
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        answer = result.get("answer", "I couldn't process that query.")

                        # Format for Teams (Markdown supported)
                        response = f"**DIA** — Data Intelligence Agent\n\n{answer}"

                        # Add benchmark badge if available
                        benchmark = result.get("benchmark")
                        if benchmark:
                            emoji_map = {
                                "Excellent": "🟢",
                                "Good": "🔵",
                                "Average": "🟡",
                                "Poor": "🔴",
                            }
                            response += f"\n\n{emoji_map.get(benchmark, '⚪')} Benchmark: **{benchmark}**"

                        # Add SQL in collapsed format
                        sql = result.get("sql")
                        if sql:
                            response += f"\n\n<details><summary>View SQL</summary>\n\n```sql\n{sql}\n```\n</details>"

                        await turn_context.send_activity(response)
                    else:
                        await turn_context.send_activity(
                            "⚠️ I encountered an error processing your query. Please try again."
                        )

        except aiohttp.ClientError as e:
            await turn_context.send_activity(
                f"⚠️ Cannot connect to DIA backend. Error: {str(e)[:100]}"
            )

    async def on_members_added_activity(self, members_added, turn_context: TurnContext):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    "👋 Welcome to **DIA** — the Volvo Cars Data Intelligence Agent.\n\n"
                    "I can help you analyze email campaign performance across all markets. "
                    "Try asking:\n\n"
                    "• *What was the click rate for EX30 in Spain?*\n"
                    "• *Compare open rates across Nordic markets*\n"
                    "• *Any unusual patterns in Germany?*\n"
                    "• *Forecast click rate for next quarter*"
                )


# --- Web Server ---
bot = DIATeamsBot()


async def messages(req: web.Request) -> web.Response:
    """Handle incoming Teams messages."""
    if "application/json" not in req.content_type:
        return web.Response(status=415)

    body = await req.json()
    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    response = await adapter.process_activity(activity, auth_header, bot.on_turn)
    if response:
        return web.json_response(data=response.body, status=response.status)
    return web.Response(status=201)


async def health(req: web.Request) -> web.Response:
    return web.json_response({"status": "healthy", "bot": "dia-teams"})


app = web.Application()
app.router.add_post("/api/messages", messages)
app.router.add_get("/health", health)

if __name__ == "__main__":
    port = int(os.getenv("TEAMS_BOT_PORT", 3978))
    print(f"DIA Teams Bot starting on port {port}")
    print(f"  Messages endpoint: http://localhost:{port}/api/messages")
    print(f"  DIA API backend:   {DIA_API_URL}")
    web.run_app(app, host="0.0.0.0", port=port)
