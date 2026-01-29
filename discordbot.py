import discord
from discord.ext import commands, tasks
from datetime import datetime
from pymongo import MongoClient
from dbanalytics import DBAnalytics
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

TOKEN = os.getenv("TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))
MENTION_ID = int(os.getenv("MENTION_ID", "0"))

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

mongo_client = MongoClient(os.getenv("MONGO_URI"))
analytics = DBAnalytics(mongo_client)


def within_active_hours() -> bool:
    return 8 <= datetime.now().hour <= 23


@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user}")
    hourly_announcements.start()


@tasks.loop(minutes=60)
async def hourly_announcements():
    if not within_active_hours():
        return

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return

    async def send_new(doc):
        await channel.send(
            f"<@{MENTION_ID}>\n\n"
            "🏠 **New flat listed!**\n"
            f"📍 {doc.get('district')} – {doc.get('subdistrict')}\n"
            f"💰 {doc.get('main_price')} PLN "
            f"({doc.get('price_per_m2')} PLN/m²)\n"
            f"📐 {doc.get('area_m2')} m² | "
            f"🛏 {doc.get('number_of_rooms')} rooms | "
            f"🏢 floor {doc.get('floor_number')}\n"
            f"🔗 {doc.get('url')}"
        )

    async def send_price_change(doc):
        old_price = analytics.get_previous_price(doc["url"])
        new_price = doc["main_price"]
        diff = new_price - old_price if old_price else 0
        emoji = "📉" if diff < 0 else "📈"

        await channel.send(
            f"<@{MENTION_ID}>\n\n"
            f"{emoji} **Price change detected!**\n"
            f"📍 {doc.get('district')} – {doc.get('subdistrict')}\n"
            f"💰 {old_price} → **{new_price} PLN** ({diff:+} PLN)\n"
            f"📐 {doc.get('area_m2')} m²\n"
            f"🔗 {doc.get('url')}"
        )

    analytics.process_announcements(
        send_new=lambda d: bot.loop.create_task(send_new(d)),
        send_price_change=lambda d: bot.loop.create_task(send_price_change(d))
    )


@bot.command()
async def stats(ctx):
    count = analytics.count_active_offers()
    await ctx.send(f"<@{MENTION_ID}> 📊 **Active flats:** {count}")


@bot.command()
async def top(ctx, n: int):
    flats = analytics.get_top_active(n)

    if not flats:
        await ctx.send(f"<@{MENTION_ID}> ❌ No flats found.")
        return

    lines = []
    for i, flat in enumerate(flats, start=1):
        lines.append(
            f"**{i}.** {flat.get('main_price')} PLN | "
            f"{flat.get('area_m2')} m² | "
            f"{flat.get('district')} ({flat.get('subdistrict')})\n"
            f"{flat.get('url')}"
        )

    await ctx.send(
        f"<@{MENTION_ID}> 🏆 **Top {len(lines)} active flats:**\n\n"
        + "\n\n".join(lines)
    )


bot.run(TOKEN)
