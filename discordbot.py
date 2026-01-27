import discord
from discord.ext import commands, tasks
from datetime import datetime, timedelta
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

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

mongo_client = MongoClient(os.getenv("MONGO_URI"))
mention_id = int(os.getenv("MENTION_ID", "0"))
analytics = DBAnalytics(mongo_client)


def within_active_hours() -> bool:
    now = datetime.now().hour
    return 8 <= now <= 23


@bot.event
async def on_ready():
    logger.info(f"Logged in as {bot.user}")
    hourly_check.start()
    hourly_price_changes.start()


@tasks.loop(minutes=60)
async def hourly_check():
    if not within_active_hours():
        return

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.warning(
            "Discord channel with ID %s not found; skipping announcement.",
            CHANNEL_ID
        )
        return

    new_flats = analytics.get_new_unannounced_flats()

    if not new_flats:
        return

    sent_urls = set()
    lines = []

    for flat in new_flats:
        url = flat.get("url")
        if not url or url in sent_urls:
            continue

        sent_urls.add(url)

        lines.append(
            "🏠 **New flat listed!**\n"
            f"📍 {flat.get('district')} – {flat.get('subdistrict')}\n"
            f"💰 {flat.get('main_price')} PLN "
            f"({flat.get('price_per_m2')} PLN/m²)\n"
            f"📐 {flat.get('area_m2')} m² | "
            f"🛏 {flat.get('number_of_rooms')} rooms | "
            f"🏢 floor {flat.get('floor_number')}\n"
            f"🔗 {url}\n"
        )

    if not lines:
        return

    message = f"<@{mention_id}>\n\n" + "\n---\n".join(lines)

    await channel.send(message)

    for url in sent_urls:
        analytics.mark_as_announced(url)


@tasks.loop(minutes=60)
async def hourly_price_changes():
    if not within_active_hours():
        return

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.warning(
            "Discord channel with ID %s not found; skipping price change announcements.",
            CHANNEL_ID
        )
        return

    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)

    changes = analytics.price_changes_today()

    recent_changes = [
        c for c in changes
        if one_hour_ago <= c["effective_from"] <= now
    ]

    if not recent_changes:
        return

    sent_keys = set()
    lines = []

    for change in recent_changes:
        key = (change.get("url"), change.get("effective_from"))
        if key in sent_keys:
            continue

        sent_keys.add(key)

        diff = change["price_diff"]
        emoji = "📉" if diff < 0 else "📈"

        lines.append(
            f"{emoji} **Price change detected!**\n"
            f"📍 {change.get('district')} – {change.get('subdistrict')}\n"
            f"💰 {change['old_price']} → "
            f"**{change['new_price']} PLN** ({diff:+} PLN)\n"
            f"📐 {change.get('area_m2')} m²\n"
            f"🔗 {change.get('url')}\n"
        )

    if not lines:
        return

    message = f"<@{mention_id}>\n\n" + "\n---\n".join(lines)

    await channel.send(message)



@bot.command()
async def stats(ctx):
    count = analytics.count_active_offers()
    await ctx.send(f"<@{mention_id}> 📊 **Active flats:** {count}")


@bot.command()
async def top(ctx, n: int):
    flats = analytics.get_top_active(n)

    if not flats:
        await ctx.send(f"<@{mention_id}> ❌ No flats found.")
        return

    seen_urls = set()
    lines = []

    for i, flat in enumerate(flats, start=1):
        url = flat.get("url")
        if not url or url in seen_urls:
            continue

        seen_urls.add(url)

        lines.append(
            f"**{i}.** {flat.get('main_price')} PLN | "
            f"{flat.get('area_m2')} m² | "
            f"{flat.get('district')} ({flat.get('subdistrict')})\n"
            f"{url}"
        )

    if not lines:
        await ctx.send(f"<@{mention_id}> ❌ No unique flats found.")
        return

    message = (
        f"<@{mention_id}> 🏆 **Top {len(lines)} active flats:**\n\n"
        + "\n\n".join(lines)
    )

    await ctx.send(message)



bot.run(TOKEN)
