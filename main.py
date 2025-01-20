import os
import re
import sys
import logging
import json
import asyncio
import nest_asyncio

from dateutil import parser
from functools import wraps
from collections import defaultdict
from contextlib import asynccontextmanager

from datetime import datetime, timedelta, timezone
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands, BotCommand
from telegram.ext import (
    AIORateLimiter,
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    ContextTypes,
)

from openpyxl import Workbook

from io import BytesIO

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import select, update, delete, func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import UniqueConstraint

TG_BOT_NAME = None
TG_BOT_TOKEN = None

CONFIG = {}
ADMINS = set()

CONFIG_FILE = "config.json"
LOG_FILE = None
DB_FILE = None

DB_ENGINE = None
Session = None

user_last_action = {}

INVALID_FILENAME_CHARACTERS = r'[<>:"/\\|?*]+'  # Define invalid filename characters

(
    MENU_MAIN,
    CREATE_CAMPAIGN_ASK_FOR_DESCRIPTION,
    CREATE_CAMPAIGN_ASK_FOR_PERIOD,
    CREATE_CAMPAIGN_SAVE_TO_DB,
    UPDATE_CAMPAIGN_LIST_FIELDS,
    UPDATE_CAMPAIGN_GET_NEW_VALUE,
    UPDATE_CAMPAIGN_ASK_FOR_CONFIRMATION,
    UPDATE_CAMPAIGN_SAVE_NEW_VALUE,
    UPDATE_CAMPAIGN_NEW_NAME,
    UPDATE_CAMPAIGN_START,
    UPDATE_CAMPAIGN_END,
    DELETE_CAMPAIGN_ASK_FOR_CONFIRMATION,
    DELETE_CAMPAIGN_FROM_DB,
    SUBMIT_CONTENT_GET_INPUT,
    SUBMIT_CONTENT_SAVE_INPUT,
    EXPORT_SUBMISSIONS,
) = range(16)

# Callback data prefixes
UPDATE_PREFIX = "UPDATE_"
DELETE_PREFIX = "DELETE_"

MAIN_MENU_CONFIG = {
    "Create Campaign": "admin_menu_create_campaign",
    "Update Campaign": "admin_menu_update_campaign",
    "Delete Campaign": "admin_menu_delete_campaign",
    "Active Campaigns": "menu_list_active_campaigns",
    "All Campaigns": "menu_list_all_campaigns",
    "My Submissions": "menu_list_my_submissions",
    "All Submissions": "admin_menu_list_all_submissions",
    "Submit Content": "menu_submit_content",
    "Export Submissions": "admin_menu_export_submissions",
    "Reload Configuration": "admin_menu_reload_config",
    "Admins": "menu_list_admins"
}

ADMIN_FUNCTIONS = {action for name, action in MAIN_MENU_CONFIG.items() if "admin_" in action.lower()}

Base = declarative_base()

message_queue = asyncio.Queue()

class Campaign(Base):
    __tablename__ = "campaigns"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, index=True)
    description = Column(String)
    start_date = Column(DateTime)
    end_date = Column(DateTime)

class Submission(Base):
    __tablename__ = "submissions"
    id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(Integer, index=True)
    username = Column(String, index=True)
    content = Column(String)
    submission_date = Column(DateTime)
    
    __table_args__ = (
        UniqueConstraint('campaign_id', 'content', 'username', name='unique_campaign_content_per_user'),
    )

# ---------- START OF DECORATORS

def admin_only(func):
    """Decorator to restrict access to admin-only functions."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        username = update.effective_user.username or "Anonymous"
        if not is_admin(username):
            await update.effective_message.reply_text("You are not authorized to perform this action.", disable_web_page_preview=True)
            return MENU_MAIN
        return await func(update, context, *args, **kwargs)
    return wrapper

def safe_execute(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ValueError as ve:
            logging.warning(f"Handled ValueError in {func.__name__}: {ve}")
        except Exception as e:
            logging.error(f"Unhandled error in {func.__name__}: {e}", exc_info=True)
            return None
    return wrapper

def log_function_call(func):

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        func_name = func.__name__
        user_info = "Unknown User"
        if args and hasattr(args[0], "effective_user"):
            update = args[0]
            user_info = update.effective_user.username or "Anonymous"
        inputs = f"Args: {args}, Kwargs: {kwargs}"
        
        logging.info("")
        logging.info("---")

        logging.info(f"[INFO] Function '{func_name}' called by {user_info}. Inputs: {inputs}")
        try:
            result = await func(*args, **kwargs)  # Await the async function
            logging.info(f"[INFO] Function '{func_name}' executed successfully. Output: {result}")
            return result
        except Exception as e:
            logging.error(
                f"[ERROR] Error in function '{func_name}' called by {user_info}. "
                f"Inputs: {inputs}. Exception: {str(e)}",
                exc_info=True
            )
            raise

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        func_name = func.__name__
        inputs = f"Args: {args}, Kwargs: {kwargs}"
        
        logging.info("")
        logging.info("---")

        logging.info(f"[INFO] Function '{func_name}' called. Inputs: {inputs}")
        try:
            result = func(*args, **kwargs)  # Call the sync function
            logging.info(f"[INFO] Function '{func_name}' executed successfully. Output: {result}")
            return result
        except Exception as e:
            logging.error(
                f"[ERROR] Error in function '{func_name}'. Inputs: {inputs}. Exception: {str(e)}",
                exc_info=True
            )
            raise

    # Return appropriate wrapper based on whether the function is async
    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

# ---------- END OF DECORATORS

@log_function_call
@asynccontextmanager
async def get_session():
    async with Session() as session:
        async with session.begin():
            yield session

@log_function_call
async def insert_data(model, data):
    """Insert data into the database using AsyncSession."""
    async with get_session() as session:
        try:
            session.add(model(**data))
            await session.commit()
        except IntegrityError as e:
            logging.error(f"Duplicate entry detected: {e}")
            await session.rollback()
            raise ValueError("This content has already been submitted for the selected campaign.")
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            await session.rollback()
            raise

@log_function_call
async def update_data(model, filters, updates):
    """Update data in the database using AsyncSession."""
    async with get_session() as session:
        try:
            stmt = update(model).where(*(getattr(model, k) == v for k, v in filters.items())).values(**updates)
            await session.execute(stmt)
            await session.commit()
        except Exception as e:
            logging.error(f"Error updating data: {e}")
            await session.rollback()

@log_function_call
async def fetch_data(model, filters=None):
    async with get_session() as session:
        try:
            stmt = select(model)
            if filters:
                stmt = stmt.where(*(getattr(model, k) == v for k, v in filters.items()))
            results = await session.execute(stmt)
            return results.scalars().all()
        except Exception as e:
            logging.error(f"Error fetching data from {model.__tablename__}: {e}")
            return []

@log_function_call
async def delete_data(model, filters):
    """Delete data from the database using AsyncSession."""
    async with get_session() as session:
        try:
            stmt = delete(model).where(*(getattr(model, k) == v for k, v in filters.items()))
            await session.execute(stmt)
            await session.commit()
        except Exception as e:
            logging.error(f"Error deleting data: {e}")
            await session.rollback()

@log_function_call
def validate_table_name(table_name):
    allowed_tables = {"campaigns", "submissions"}
    if table_name not in allowed_tables:
        raise ValueError(f"Invalid table name: {table_name}")

@log_function_call
def sanitize_string(name: str) -> str:
    """Sanitize a string to make it filesystem-safe across platforms."""
    sanitized = re.sub(INVALID_FILENAME_CHARACTERS, '_', name)
    return sanitized.strip("_")

@log_function_call
def load_config():
    global CONFIG
    try:
        with open(CONFIG_FILE, "r") as file:
            CONFIG = json.load(file)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        CONFIG = {"ENV": "prod"}  # Default to production if config is missing
        logging.error(f"Error loading configuration: {e}")

@log_function_call
def set_variables():
    global TG_BOT_TOKEN, TG_BOT_NAME, GROUP_ID, TOPIC_ID, ADMINS, LOG_FILE, DB_FILE

    TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", CONFIG.get("TG_BOT_TOKEN"))
    TG_BOT_NAME = CONFIG.get("TG_BOT_NAME", "tg_bot_name")
    GROUP_ID = CONFIG.get("GROUP_ID")
    TOPIC_ID = CONFIG.get("TOPIC_ID")
    ADMINS = set(CONFIG.get("ADMINS", []))
    LOG_FILE = f"{TG_BOT_NAME}.log"
    DB_FILE = f"{TG_BOT_NAME}.db"

def clear(): os.system('cls' if os.name == 'nt' else 'clear'); print()

def is_admin(username: str) -> bool:
    """Check if the username belongs to an admin."""
    return username in ADMINS

@log_function_call
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Display the main menu with filtered buttons.
    """
    # context.chat_data["chat_id"] = update.effective_chat.id
    username = update.effective_user.username or "Anonymous"

    # Generate menu buttons for the main menu
    filtered_menu = generate_filtered_menu(username)
    main_menu_buttons = generate_menu_buttons(filtered_menu, is_main_menu=True)  # Flag as main menu
    title = "Available Actions"

    logging.debug(f"Main Menu Title: {title}")
    logging.debug(f"Main Menu Buttons: {main_menu_buttons}")

    await set_menu_button(context.application)

    try:
        is_callback = hasattr(update, "callback_query") and update.callback_query is not None
        if is_callback:
            await update.callback_query.answer()  # Acknowledge the callback

        await update_static_messages(
            update,
            context,
            title=title,
            content=main_menu_buttons,
            send_new=not is_callback,
        )
    except Exception as e:
        logging.error(f"Error sending main menu: {e}")

    return MENU_MAIN

@log_function_call
async def list_admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    List all admin users.
    """
    return_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
    ])
    if not ADMINS:
        await update_static_messages(
            update,
            context,
            title="There are no configured admin users.",
            content=return_button,
        )
    else:
        admin_list = "\n".join([f"@{admin}" for admin in ADMINS])
        await update_static_messages(
            update,
            context,
            title=admin_list,
            content=return_button,
        )
    return MENU_MAIN

@safe_execute
@log_function_call
async def list_my_submissions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    List all submissions made by the user.
    """
    username = update.effective_user.username or "Anonymous"
    submissions = await fetch_data(Submission, filters={"username": username})

    return_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
    ])

    if not submissions:
        await update_static_messages(
            update,
            context,
            title="You have not submitted any content yet.",
            content=return_button,
        )
        return MENU_MAIN

    campaign_map = {c.id: c.name for c in await fetch_data(Campaign)}

    grouped_submissions = defaultdict(list)
    for submission in submissions:
        campaign_name = campaign_map.get(submission.campaign_id, "Unknown Campaign")
        grouped_submissions[campaign_name].append(
            f"{format_datetime(submission.submission_date)} - {submission.content}"
        )

    submissions_text = "\n\n".join([
        f"🎁 {campaign_name}\n" + "\n".join(entries)
        for campaign_name, entries in grouped_submissions.items()
    ])

    await update_static_messages(
        update,
        context,
        title=submissions_text,
        content=return_button,
    )
    return MENU_MAIN

@admin_only
@safe_execute
@log_function_call
async def list_all_submissions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    List all submissions across campaigns.
    """
    submissions = await fetch_data(Submission)
    return_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
    ])

    if not submissions:
        await update_static_messages(
            update,
            context,
            title="There are no submissions yet.",
            content=return_button,
        )
        return MENU_MAIN

    campaign_map = {c.id: c.name for c in await fetch_data(Campaign)}

    grouped_submissions = defaultdict(list)
    for submission in submissions:
        campaign_name = campaign_map.get(submission.campaign_id, "Unknown Campaign")
        grouped_submissions[campaign_name].append(
            f"{format_datetime(submission.submission_date)} - @{submission.username} - {submission.content}"
        )

    submissions_text = "\n\n".join([
        f"🎁 {campaign_name}\n" + "\n".join(entries)
        for campaign_name, entries in grouped_submissions.items()
    ])
    
    await update_static_messages(
        update,
        context,
        title=submissions_text,
        content=return_button,
    )
    return MENU_MAIN

@admin_only
@log_function_call
async def reload_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
    ])

    try:
        load_config()
        set_variables()
        await update_static_messages(
            update,
            context,
            title="Configuration has been reloaded successfully.",
            content=return_button,
        )
    except Exception as e:
        logging.error(f"Error reloading config: {e}")
        await update_static_messages(
            update,
            context,
            title="Failed to reload configuration.",
            content=return_button,
        )
    return MENU_MAIN

def initialize_log():
    ENV = CONFIG.get("ENV", "prod")

    if ENV == "dev":
        log_level = logging.INFO
    elif ENV == "prod":
        log_level = logging.ERROR
    else:
        log_level = logging.ERROR  # Default fallback for unknown environments
    
    print(f"Environment: {ENV}")
    print(f"log_level: {log_level}")

    logging.StreamHandler(sys.stdout)

    handlers = [logging.StreamHandler()]

    if LOG_FILE:
        handlers.append(logging.FileHandler(LOG_FILE, encoding="utf-8"))
    else:
        logging.info("LOG_FILE is not set.")
    
    logging.getLogger().handlers.clear()

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=log_level,
        handlers=handlers,
    )

    logging.info("Logging initialized")

    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("http.client").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
    logging.getLogger("sqlite3").setLevel(logging.ERROR)
    # logging.getLogger("python-telegram-bot").setLevel(logging.ERROR)

@log_function_call
async def initialize_db():
    global DB_ENGINE, Session

    DB_ENGINE = create_async_engine(
        f"sqlite+aiosqlite:///{DB_FILE}",
        echo=False,
        connect_args={"timeout": 10},  # Timeout for database locks
    )

    Session = sessionmaker(bind=DB_ENGINE, expire_on_commit=False, class_=AsyncSession)

    # Automatically create tables from ORM models
    async with DB_ENGINE.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        await conn.execute(text("PRAGMA journal_mode=WAL;"))
        await conn.execute(text("PRAGMA synchronous = NORMAL;"))
        await conn.execute(text("PRAGMA foreign_keys = ON;"))
        await conn.execute(text("PRAGMA locking_mode = NORMAL;"))

    logging.info("Database initialized using SQLAlchemy ORM.")

@log_function_call
async def fetch_campaigns(status="all"):
    now = datetime.now(timezone.utc)
    try:
        async with Session() as session:
            async with session.begin():
                stmt = select(Campaign)
                if status == "active":
                    stmt = stmt.filter(Campaign.start_date <= now, now < Campaign.end_date)
                results = await session.execute(stmt)
                campaigns = results.scalars().all()

                logging.info("Fetched Campaigns:")
                for campaign in campaigns:
                    logging.info(
                        f"Campaign '{campaign.name}' - Start: {campaign.start_date}, "
                        f"End: {campaign.end_date}"
                    )

                return [
                    {
                        "id": c.id,
                        "name": c.name,
                        "description": c.description,
                        "start_date": c.start_date,
                        "end_date": c.end_date,
                    }
                    for c in campaigns
                ]
    except Exception as e:
        logging.error(f"Error fetching campaigns: {e}")
        return []

@log_function_call
def generate_excel_file(data, headers, filename="export.xlsx"):
    """Generate an Excel file from provided data and headers."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Export"

    # Add headers
    sheet.append([str(header) for header in headers])

    # Add rows
    for row in data:
        sheet.append(row)

    # Save to an in-memory file
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return output

@log_function_call
def format_datetime(dt: datetime) -> str:
    """Format a datetime object as UTC in 'dd.mm.yyyy HH:MM:SS'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M:%S")

@log_function_call
def parse_and_validate_date(date_str: str, field_name: str) -> datetime:
    """Parse and validate a date input, ensuring it is in UTC."""
    logging.info("")
    logging.info("------------------------------")
    logging.info("parse_and_validate_date")
    logging.info(f"Parsing {field_name}: {date_str}")

    if isinstance(date_str, datetime):
        # If already a datetime object, ensure it is in UTC, and reset microseconds
        if date_str.tzinfo is None:
            logging.warning(f"{field_name}: Naive datetime detected. Assuming UTC.")
            parsed_dt = date_str.replace(tzinfo=timezone.utc, microsecond=0)
        else:
            parsed_dt = date_str.astimezone(timezone.utc).replace(microsecond=0)
        logging.info(f"{field_name}: {date_str} is already a datetime object -> {parsed_dt}")
        return parsed_dt

    try:
        # Primary format: 'dd.mm.yyyy HH:MM:SS' (assumed UTC for naive input)
        parsed_dt = datetime.strptime(date_str, "%d.%m.%Y %H:%M:%S").replace(tzinfo=timezone.utc, microsecond=0)
    except ValueError:
        try:
            # Fallback 1: Format without seconds 'dd.mm.yyyy HH:MM' (assumed UTC for naive input)
            parsed_date = datetime.strptime(date_str, "%d.%m.%Y %H:%M")
            parsed_dt = parsed_date.replace(second=0, tzinfo=timezone.utc, microsecond=0)
        except ValueError:
            try:
                # Fallback 2: ISO 8601 format
                parsed_dt = datetime.fromisoformat(date_str)
                if parsed_dt.tzinfo is None:
                    logging.warning(f"{field_name}: Naive ISO 8601 datetime detected. Assuming UTC.")
                    parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                parsed_dt = parsed_dt.astimezone(timezone.utc).replace(microsecond=0)
            except ValueError:
                try:
                    # Fallback 3: dateutil's ISO 8601 parser
                    parsed_dt = parser.isoparse(date_str)
                    if parsed_dt.tzinfo is None:
                        logging.warning(f"{field_name}: Naive dateutil datetime detected. Assuming UTC.")
                        parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                    parsed_dt = parsed_dt.astimezone(timezone.utc).replace(microsecond=0)
                except ValueError as e:
                    logging.error(f"Failed to parse {field_name} '{date_str}': {e}")
                    raise ValueError(f"Invalid {field_name}. Please use the format 'dd.mm.yyyy HH:MM:SS' or ISO 8601.")

    logging.info(f"field: {field_name} - input: {date_str} -> output: {parsed_dt}")
    return parsed_dt

@log_function_call
def generate_filtered_menu(username: str) -> dict:
    """Generate a filtered main menu based on user type."""
    return {
        name: action
        for name, action in MAIN_MENU_CONFIG.items()
        if is_admin(username) or action not in ADMIN_FUNCTIONS
    }

@log_function_call
def generate_menu_buttons(menu_config: dict, is_main_menu: bool = False) -> InlineKeyboardMarkup:
    """
    Generate a list of inline keyboard buttons for a given menu configuration, 
    grouping them into rows with a specified number of buttons per row.

    Args:
        menu_config: A dictionary where keys are button text, and values are callback data.
        is_main_menu: Whether to include the "Return to Main Menu" button.
    
    Returns:
        InlineKeyboardMarkup: Inline keyboard with grouped buttons.
    """
    
    MAX_BUTTONS_PER_ROW = 2

    # Group buttons into rows based on MAX_BUTTONS_PER_ROW
    buttons = [
        InlineKeyboardButton(name, callback_data=action)
        for name, action in menu_config.items()
    ]
    button_rows = [buttons[i:i + MAX_BUTTONS_PER_ROW] for i in range(0, len(buttons), MAX_BUTTONS_PER_ROW)]

    # Add "Return to Main Menu" button if not the main menu
    if not is_main_menu:
        button_rows.append([InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")])

    return InlineKeyboardMarkup(button_rows)

@log_function_call
def get_campaign_status(start_date, end_date, current_time=None, tz=timezone.utc):
    """Determine the status of a campaign."""
    if not current_time:
        current_time = datetime.now(tz)

    # Ensure all datetime objects are timezone-aware
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=tz)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=tz)

    if current_time < start_date:
        return "Upcoming"
    elif start_date <= current_time < end_date:
        return "Ongoing"
    return "Completed"

@admin_only
@safe_execute
@log_function_call
async def list_campaigns_as_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaigns = await fetch_data(Campaign)
    if not campaigns:
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title="No campaigns available to update.",
            content=return_button,
        )
        return MENU_MAIN

    # Prepare button data for campaigns
    campaign_buttons = {campaign.name: f"{UPDATE_PREFIX}{campaign.id}" for campaign in campaigns}

    # Generate menu buttons with a specified number of buttons per row
    reply_markup = generate_menu_buttons(campaign_buttons, is_main_menu=False)

    # Send a single message with the buttons
    await update_static_messages(
        update,
        context,
        title="Select a campaign to update:",
        content=reply_markup,
    )

    return UPDATE_CAMPAIGN_LIST_FIELDS

@log_function_call
async def get_submission_counts(campaign_ids):
    """
    Fetch the number of submissions for each campaign ID.
    Args:
        campaign_ids (list): List of campaign IDs.
    Returns:
        dict: A dictionary mapping campaign IDs to their submission counts.
    """
    if not campaign_ids:
        logging.warning("No campaign IDs provided to fetch submission counts.")
        return {}

    logging.info(f"Fetching submission counts for campaign IDs: {campaign_ids}")

    async with get_session() as session:
        try:
            stmt = (
                select(Submission.campaign_id, func.count(Submission.id).label("submission_count"))
                .filter(Submission.campaign_id.in_(campaign_ids))
                .group_by(Submission.campaign_id)
            )
            results = await session.execute(stmt)
            
            # Convert results into a dictionary
            submission_counts = {row[0]: row[1] for row in results}
            logging.info(f"Submission counts fetched: {submission_counts}")
            return submission_counts
        except Exception as e:
            logging.error(f"Error fetching submission counts: {e}")
            return {}

@safe_execute
@log_function_call
async def list_campaigns_as_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    List all campaigns or active campaigns based on callback data.
    """
    callback_data = update.callback_query.data
    campaign_type = "active" if "menu_list_active_campaigns" in callback_data else "all"

    campaigns = await fetch_campaigns(campaign_type)
    current_time = datetime.now(timezone.utc)

    return_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
    ])

    # Prepare content
    if campaigns:
        submission_counts = await get_submission_counts([c["id"] for c in campaigns])

        campaigns_text = "\n\n".join([
            f"🎁 {c['name']}\n"
            f"Status: {get_campaign_status(c['start_date'], c['end_date'], current_time)}\n"
            f"Description: {c['description']}\n"
            f"Submissions: {submission_counts.get(c['id'], 0)}\n"
            f"Start: {format_datetime(c['start_date'])} UTC\n"
            f"End: {format_datetime(c['end_date'])} UTC"
            for c in campaigns
        ])
    else:
        campaigns_text = "No campaigns found." if campaign_type == "all" else "No active campaigns found."

    await update_static_messages(
        update,
        context,
        title=campaigns_text,
        content=return_button,
    )

    return MENU_MAIN

@admin_only
@safe_execute
@log_function_call
async def create_campaign_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # context.user_data.clear()  # Clear any residual data

    await update_static_messages(
        update,
        context,
        title="Enter the campaign name:",
        content=None,  # No buttons required here
        send_new=True  # Reuse the existing message if possible
    )
    return CREATE_CAMPAIGN_ASK_FOR_DESCRIPTION

@safe_execute
@log_function_call
async def create_campaign_get_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaign_name = update.effective_message.text.strip()
    context.user_data["campaign_name"] = campaign_name

    await update_static_messages(
        update,
        context,
        title="Enter the campaign description:",
        content=None,  # No buttons required here
        send_new=True
    )
    return CREATE_CAMPAIGN_ASK_FOR_PERIOD

@safe_execute
@log_function_call
async def create_campaign_get_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaign_description = update.effective_message.text.strip()
    context.user_data["campaign_description"] = campaign_description

    await update_static_messages(
        update,
        context,
        title="Enter the campaign period (in days):",
        content=None,
        send_new=True
    )
    return CREATE_CAMPAIGN_SAVE_TO_DB

@log_function_call
async def update_campaign_field(campaign_id, field, new_value):
    if field in ["start_date", "end_date"]:
        new_value = parse_and_validate_date(new_value, field)

    await update_data(
        Campaign,
        filters={"id": campaign_id},
        updates={field: new_value}
    )

@safe_execute
@log_function_call
async def delete_campaign(campaign_id, campaign_name):
    try:
        export_result = await export_submissions(campaign_id)
        
        await delete_data(Submission, {"campaign_id": campaign_id})
        await delete_data(Campaign, {"id": campaign_id})

        logging.info(f"Deleted campaign '{campaign_name}' (ID: {campaign_id}) and associated submissions.")
        return export_result
    except Exception as e:
        logging.error(f"Error deleting campaign: {e}")
        raise

@log_function_call
async def save_campaign(campaign_name, campaign_description, start_date, end_date):
    start_date = parse_and_validate_date(start_date, "start_date")
    end_date = parse_and_validate_date(end_date, "end_date")

    data = {
        "name": campaign_name,
        "description": campaign_description,
        "start_date": start_date,
        "end_date": end_date,
    }
    await insert_data(Campaign, data)

@safe_execute
@log_function_call
async def create_campaign_save_to_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        period = int(update.effective_message.text.strip())
        if period <= 0:
            raise ValueError("The period must be a positive integer.")

        campaign_name = context.user_data.get("campaign_name", "Unnamed Campaign")
        campaign_description = context.user_data.get("campaign_description", "")
        start_date = datetime.now(timezone.utc)
        end_date = start_date + timedelta(days=period)

        await save_campaign(campaign_name, campaign_description, start_date, end_date)

        start_date_str = format_datetime(start_date)
        end_date_str = format_datetime(end_date)

        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])

        await update_static_messages(
            update,
            context,
            title=f"New campaign created successfully! 🎉\n\n"
                  f"Name: {campaign_name}\n"
                  f"Description: {campaign_description}\n"
                  f"Start date: {start_date_str} UTC\n"
                  f"End date: {end_date_str} UTC",
            content=return_button,
            send_new=True
        )
        return MENU_MAIN

    except ValueError as e:
        await update_static_messages(
            update,
            context,
            title=str(e),
            content=None,
            send_new=False
        )
        return CREATE_CAMPAIGN_SAVE_TO_DB

@admin_only
@safe_execute
@log_function_call
async def select_campaign_to_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Display a list of campaigns for deletion.
    """
    campaigns = await fetch_data(Campaign)

    if not campaigns:
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title="No campaigns available for deletion.",
            content=return_button,
        )
        return MENU_MAIN

    # Prepare buttons for campaigns
    campaign_buttons = {campaign.name: f"{DELETE_PREFIX}{campaign.id}" for campaign in campaigns}
    reply_markup = generate_menu_buttons(campaign_buttons)

    await update_static_messages(
        update,
        context,
        title="Select a campaign to delete:",
        content=reply_markup,
    )

    return DELETE_CAMPAIGN_ASK_FOR_CONFIRMATION

@log_function_call
async def delete_campaign_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ask for confirmation before deleting the selected campaign.
    """
    campaign_id = int(update.callback_query.data[len(DELETE_PREFIX):])
    campaign = await get_campaign_data_by_id(campaign_id)
    campaign_name = campaign.name if campaign else "Unknown"

    # Store campaign details in user data
    context.user_data["campaign_id"] = campaign_id
    context.user_data["campaign_name"] = campaign_name

    # Prepare confirmation buttons
    confirmation_buttons = {
        "Approve": "confirm_yes",
        "Cancel": "confirm_no"
    }
    reply_markup = generate_menu_buttons(confirmation_buttons)

    context.user_data["static_message_id"] = None

    await update_static_messages(
        update,
        context,
        title=f"Are you sure you want to delete the campaign {campaign_name}?",
        content=reply_markup,
    )

    return DELETE_CAMPAIGN_FROM_DB

@log_function_call
async def get_campaign_data_by_id(campaign_id):
    """Fetch a single campaign by its ID."""
    try:
        campaigns = await fetch_data(Campaign, filters={"id": campaign_id})
        return campaigns[0] if campaigns else None
    except Exception as e:
        logging.error(f"Error fetching campaign by ID {campaign_id}: {e}")
        return None

@log_function_call
async def update_campaign_list_fields(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaign_id = int(update.callback_query.data[len(UPDATE_PREFIX):])
    context.user_data["campaign_id"] = campaign_id

    campaign = await get_campaign_data_by_id(campaign_id)
    campaign_name = campaign.name if campaign else "Unknown"
    context.user_data["campaign_name"] = campaign_name

    update_menu = {
        "Update Name": "name",
        "Update Description": "description",
        "Update Start Date": "start_date",
        "Update End Date": "end_date",
    }
    reply_markup = generate_menu_buttons(update_menu)

    await update_static_messages(
        update,
        context,
        title=f"What would you like to update for campaign '{campaign_name}'?",
        content=reply_markup,
    )
    return UPDATE_CAMPAIGN_GET_NEW_VALUE

@log_function_call
async def update_campaign_get_new_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    field = update.callback_query.data
    context.user_data["update_field"] = field

    await update_static_messages(
        update,
        context,
        title=f"Enter the new value for {field.replace('_', ' ')}:",
        content=None,
    )
    return UPDATE_CAMPAIGN_ASK_FOR_CONFIRMATION

@log_function_call
async def update_campaign_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaign_id = context.user_data["campaign_id"]
    field = context.user_data["update_field"]
    new_value = update.effective_message.text.strip()

    try:
        # Fetch the current campaign data
        campaign = await fetch_data(Campaign, filters={"id": campaign_id})
        if not campaign:
            raise ValueError("Campaign not found.")
        campaign = campaign[0]

        # Parse and validate current and new values
        current_value = getattr(campaign, field, None)
        if field in ["start_date", "end_date"]:
            current_value = parse_and_validate_date(current_value, field) if current_value else None
            new_value = parse_and_validate_date(new_value, field)
            
            formatted_current_value = format_datetime(current_value) if current_value else "N/A"
            formatted_new_value = format_datetime(new_value)

            # Additional checks for date validity
            if field == "end_date":
                current_start_date = parse_and_validate_date(campaign.start_date, "start_date") if campaign.start_date else None
                if new_value <= current_start_date:
                    await update_static_messages(
                        update,
                        context,
                        title="End date cannot be earlier than the start date.",
                        content=InlineKeyboardMarkup([[InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]]),
                        send_new=True,
                    )
                    return MENU_MAIN
        else:
            current_value = current_value or "N/A"
            formatted_current_value = current_value
            formatted_new_value = new_value
        
        # Store new value in user_data
        context.user_data["new_value"] = new_value

        # Prepare confirmation buttons
        reply_markup = generate_menu_buttons({
            "Approve": "confirm_yes",
            "Cancel": "confirm_no"
        })

        await update_static_messages(
            update,
            context,
            title=(
                f"The {field.replace('_', ' ')} will be updated from '{formatted_current_value}' "
                f"to '{formatted_new_value}'. Do you confirm?"
            ),
            content=reply_markup,
            send_new=True  # Always a new message
        )
        return UPDATE_CAMPAIGN_SAVE_NEW_VALUE

    except Exception as e:
        logging.error(f"Error during confirmation: {e}")
        await update_static_messages(
            update,
            context,
            title="Failed to validate the input. Please try again.",
            content=InlineKeyboardMarkup([[InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]]),
            send_new=True,
        )
        return UPDATE_CAMPAIGN_GET_NEW_VALUE

@log_function_call
async def update_campaign_save_new_value(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query.data == "confirm_yes":
        campaign_id = context.user_data["campaign_id"]
        field = context.user_data["update_field"]
        new_value = context.user_data["new_value"]

        # Update the campaign field in the database
        await update_campaign_field(campaign_id, field, new_value)

        message_text = f"Campaign {field.replace('_', ' ')} updated successfully."

        if field == "start_date":
            campaign = await fetch_data(Campaign, filters={"id": campaign_id})
            current_end_date = parse_and_validate_date(campaign[0].end_date, "end_date") if campaign else None

            if current_end_date and new_value >= current_end_date:
                new_end_date = new_value + timedelta(days=7)
                await update_campaign_field(campaign_id, "end_date", new_end_date)
                message_text += f"\n\nEnd date auto-adjusted to {format_datetime(new_end_date)} due to start date update."

        await update_static_messages(
            update,
            context,
            title=message_text,
            content=InlineKeyboardMarkup([[InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]]),
            send_new=False,  # Update the existing message
        )
    else:
        await update_static_messages(
            update,
            context,
            title="Update cancelled.",
            content=None,
        )

    return MENU_MAIN

@log_function_call
async def delete_campaign_from_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query.data != "confirm_yes":
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title="Deletion cancelled.",
            content=return_button,
        )
        return MENU_MAIN

    campaign_id = context.user_data["campaign_id"]
    campaign_name = context.user_data["campaign_name"]

    try:
        export_result = await delete_campaign(campaign_id, campaign_name)

        if export_result:
            output, filename = export_result
            await update.callback_query.message.reply_document(
                document=output,
                filename=filename,
                caption=f"Submissions exported for campaign '{campaign_name}'."
            )

        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title=f"Campaign '{campaign_name}' deleted successfully!",
            content=return_button,
        )
        return MENU_MAIN
    
    except Exception as e:
        logging.error(f"Error deleting campaign: {e}")
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title="An error occurred while deleting the campaign.",
            content=return_button,
        )
        return MENU_MAIN

@safe_execute
@log_function_call
async def submit_content_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return_button = InlineKeyboardMarkup([
        [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
    ])

    # context.chat_data["chat_id"] = update.effective_chat.id  # Ensure chat_id is initialized
    active_campaigns = await fetch_campaigns("active")

    if not active_campaigns:
        await update_static_messages(
            update,
            context,
            title="No active campaigns available for submission.",
            content=return_button,
        )
        return MENU_MAIN

    campaign_buttons = {campaign["name"]: f"submit_{campaign['id']}" for campaign in active_campaigns}
    reply_markup = generate_menu_buttons(campaign_buttons)

    await update_static_messages(
        update,
        context,
        title="Select a campaign to submit content for:",
        content=reply_markup,
    )
    
    return SUBMIT_CONTENT_GET_INPUT

@log_function_call
async def submit_content_get_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaign_id = int(update.callback_query.data.split("_")[1])
    context.user_data["campaign_id"] = campaign_id

    campaign = await get_campaign_data_by_id(campaign_id)
    campaign_name = campaign.name if campaign else "Unknown"
    context.user_data["campaign_name"] = campaign_name

    await update_static_messages(
        update,
        context,
        title=f"Enter the content for campaign '{campaign_name}':",
        content=None,
    )
    return SUBMIT_CONTENT_SAVE_INPUT

@log_function_call
async def submit_content_save_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    content = update.effective_message.text.strip()
    campaign_id = context.user_data.get("campaign_id")
    username = update.effective_user.username or "Anonymous"
    submission_date = datetime.now(timezone.utc)

    data = {
        "campaign_id": campaign_id,
        "username": username,
        "content": content,
        "submission_date": submission_date,
    }

    try:
        # Insert submission into the database
        await insert_data(Submission, data)

        # Notify group or topic if configured
        if GROUP_ID and TOPIC_ID:
            message = (
                f"New Submission from @{username} 🎉\n\n"
                f"Campaign: {context.user_data['campaign_name']}\n"
                f"Content: {content}"
            )
            await context.bot.send_message(
                chat_id=GROUP_ID,
                text=message,
                message_thread_id=TOPIC_ID,
                # parse_mode="MarkdownV2"
                parse_mode=None
            )
            logging.info(f"Submission published to group {GROUP_ID} in topic {TOPIC_ID}.")
        else:
            logging.warning("Group ID or Topic ID not configured. Skipping group notification.")

        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        # Notify user of successful submission
        await update_static_messages(
            update,
            context,
            title="Content submitted successfully!",
            content=return_button,
            send_new=True
        )
        return MENU_MAIN

    except ValueError as e:
        error_message = "You have already submitted this content for the selected campaign." if "already been submitted" in str(e) else str(e)
        logging.error(f"Submission error: {e}")
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title=error_message,
            content=return_button,
        )
    except Exception as e:
        logging.error(f"Unexpected error while saving submission: {e}")
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title="An error occurred while saving your submission.",
            content=return_button,
        )

    return MENU_MAIN

@log_function_call
async def export_submissions(campaign_id=None):
    filters = {"campaign_id": campaign_id} if campaign_id else {}
    submissions = await fetch_data(Submission, filters=filters)

    campaigns = {c.id: c.name for c in await fetch_data(Campaign)}

    if not submissions:
        return None

    rows = [
        [
            campaigns.get(submission.campaign_id, "Unknown"),
            submission.username,
            format_datetime(submission.submission_date),
            submission.content
        ]
        for submission in submissions
    ]
    headers = ["Campaign Name", "Username", "Submission Date", "Content"]
    filename = f"{sanitize_string(campaigns.get(campaign_id, 'Unknown'))}_submissions.xlsx" if campaign_id else "all_submissions.xlsx"
    file = generate_excel_file(rows, headers, filename)

    return file, filename

@admin_only
@safe_execute
@log_function_call
async def export_submissions_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # context.chat_data["chat_id"] = update.effective_chat.id  # Ensure chat_id is initialized
    campaigns = await fetch_data(Campaign)

    if not campaigns:
        return_button = InlineKeyboardMarkup([
            [InlineKeyboardButton("Return to Main Menu", callback_data="menu_main")]
        ])
        await update_static_messages(
            update,
            context,
            title="No campaigns available for export.",
            content=return_button,
        )
        return MENU_MAIN

    # Prepare buttons for each campaign and an 'All' option
    campaign_buttons = {"All": "export_all"}
    campaign_buttons.update({campaign.name: f"export_{campaign.id}" for campaign in campaigns})
    reply_markup = generate_menu_buttons(campaign_buttons)

    await update_static_messages(
        update,
        context,
        title="Select a campaign to export submissions for, or choose 'All':",
        content=reply_markup,
    )

    return EXPORT_SUBMISSIONS

@log_function_call
async def export_campaign_submissions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    campaign_id = update.callback_query.data.split("_")[1]
    is_all = campaign_id == "all"

    try:
        export_result = await export_submissions(int(campaign_id) if not is_all else None)

        if not export_result:
            await update_static_messages(
                update,
                context,
                title="No submissions found for export.",
                content=None,  # No buttons needed in this case
                send_new=False  # Use the existing message
            )
            return MENU_MAIN
        
        excel_file, filename = export_result
        campaigns = {c.id: c.name for c in await fetch_data(Campaign)}

        # Update the existing message to notify about successful export
        await update_static_messages(
            update,
            context,
            title=f"Submissions exported successfully for {'all campaigns' if is_all else campaigns.get(campaign_id, 'Unknown')}.\n\n"
                  f"The exported file will be sent as a document shortly.",
            content=None,  # Keep it simple, no buttons needed for now
            send_new=False
        )

        # Send the file as a new message
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=excel_file,
            filename=filename,
            caption=f"Exported file: {filename}"
        )
    except Exception as e:
        logging.error(f"Export error: {e}")
        await update_static_messages(
            update,
            context,
            title="An error occurred during export. Please try again later.",
            content=None,
            send_new=False
        )

    return MENU_MAIN

@log_function_call
async def vacuum_db():
    async with DB_ENGINE.begin() as conn:
        await conn.execute("VACUUM;")
    logging.info("Database vacuumed.")

@log_function_call
async def start_maintenance_tasks():
    while True:
        await asyncio.sleep(86400)  # Run every 24 hours
        await vacuum_db()

@log_function_call
async def start_workers(worker_count=5):
    workers = [asyncio.create_task(message_worker()) for _ in range(worker_count)]
    await asyncio.gather(*workers)

@log_function_call
async def message_worker():
    while True:
        context, chat_id, text, kwargs = await message_queue.get()
        try:
            for attempt in range(3):  # Retry up to 3 times
                try:
                    await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
                    break
                except Exception as e:
                    logging.error(f"Failed to send message on attempt {attempt + 1}: {e}")
                    if attempt == 2:  # On the final attempt, log failure
                        logging.error(f"Message permanently failed: {text}")
                    await asyncio.sleep(0.25)
        except Exception as e:
            logging.error(f"Unexpected error sending message: {e}")
        finally:
            message_queue.task_done()

@log_function_call
async def enqueue_message(context, chat_id, text, **kwargs):
    await message_queue.put((context, chat_id, text, kwargs))

@log_function_call
async def set_menu_button(app: Application):
    bot = app.bot

    # Define the /menu command
    menu_commands = [
        BotCommand("menu", "Show Main Menu"),
    ]

    # Set the command for the bot
    await bot.set_my_commands(menu_commands)

    # Set the menu button to display the /menu command
    await bot.set_chat_menu_button(
        menu_button=MenuButtonCommands()
    )

    logging.info("Menu button updated to include only the /menu command.")

@log_function_call
async def timeout_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "You took too long to respond. Please start again by typing /menu or /start."
    )
    return ConversationHandler.END

@log_function_call
async def update_static_messages(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    title: str,
    content: InlineKeyboardMarkup = None,
    send_new: bool = False,  # Explicitly force new messages if needed
    max_length: int = 4096  # Telegram's character limit for messages
):
    """
    Update or send a message, keeping campaign info together.

    Args:
        update: The Update object from Telegram.
        context: The context from Telegram's CallbackContext.
        title: The message text (can be long).
        content: InlineKeyboardMarkup for the last message.
        send_new: Whether to send a new message instead of updating an existing one.
        max_length: The maximum length of a single message.
    """
    chat_id = update.effective_chat.id
    previous_message_id = context.user_data.get("static_message_id")

    # Split campaign text into grouped chunks
    campaigns = title.split("\n\n")  # Assuming campaigns are separated by "\n\n"
    messages = []
    current_message = ""

    for campaign in campaigns:
        if len(current_message) + len(campaign) + 2 <= max_length:  # +2 for "\n\n"
            current_message += f"\n\n{campaign}" if current_message else campaign
        else:
            messages.append(current_message)
            current_message = campaign

    if current_message:
        messages.append(current_message)

    # Check if the current batch results in multiple messages
    is_multi_message = len(messages) > 1

    if send_new or is_multi_message or not previous_message_id:
        # Send new messages if forced, multi-message, or no previous message
        for i, msg in enumerate(messages):
            is_last_message = (i == len(messages) - 1)
            reply_markup = content if is_last_message else None

            sent_message = await context.bot.send_message(
                chat_id=chat_id,
                text=msg,
                reply_markup=reply_markup,
                parse_mode=None,  # Remove Markdown
                disable_web_page_preview=True,
            )

            # Store only the first message ID
            if i == 0:
                context.user_data["static_message_id"] = sent_message.message_id

        # Clear static_message_id if multiple messages were sent
        if is_multi_message:
            context.user_data["static_message_id"] = None
    else:
        # Edit existing single message
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=previous_message_id,
                text=messages[0],
                reply_markup=content,
                parse_mode=None,  # Remove Markdown
                disable_web_page_preview=True,
            )
        except Exception as e:
            logging.error(f"Error editing static message: {e}")

@log_function_call
async def main(TOKEN):
    initialize_log()
    await initialize_db()

    APP = Application.builder() \
                        .rate_limiter(AIORateLimiter()) \
                        .token(TOKEN) \
                        .http_version("1.1") \
                        .read_timeout(60) \
                        .build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", main_menu),
            CommandHandler("menu", main_menu),
            CommandHandler("help", main_menu),
        ],
        states={
            MENU_MAIN: [
                CallbackQueryHandler(create_campaign_get_name, pattern="^admin_menu_create_campaign$"),
                CallbackQueryHandler(list_campaigns_as_menu, pattern="^admin_menu_update_campaign$"),
                CallbackQueryHandler(select_campaign_to_delete, pattern="^admin_menu_delete_campaign$"),
                CallbackQueryHandler(list_campaigns_as_message, pattern="^menu_list_active_campaigns$"),
                CallbackQueryHandler(list_campaigns_as_message, pattern="^menu_list_all_campaigns$"),      
                CallbackQueryHandler(submit_content_start, pattern="^menu_submit_content$"),
                CallbackQueryHandler(list_my_submissions, pattern="^menu_list_my_submissions$"),
                CallbackQueryHandler(list_all_submissions, pattern="^admin_menu_list_all_submissions$"),
                CallbackQueryHandler(export_submissions_start, pattern="^admin_menu_export_submissions$"),
                CallbackQueryHandler(reload_config, pattern="^admin_menu_reload_config$"),
                CallbackQueryHandler(list_admin_users, pattern="^menu_list_admins$"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),  # Handle "Return to Main Menu"
            ],
            CREATE_CAMPAIGN_ASK_FOR_DESCRIPTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_campaign_get_description),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            CREATE_CAMPAIGN_ASK_FOR_PERIOD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_campaign_get_period),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            CREATE_CAMPAIGN_SAVE_TO_DB: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_campaign_save_to_db),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            UPDATE_CAMPAIGN_LIST_FIELDS: [
                CallbackQueryHandler(update_campaign_list_fields, pattern=f"^{UPDATE_PREFIX}.*"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            UPDATE_CAMPAIGN_GET_NEW_VALUE: [
                CallbackQueryHandler(update_campaign_get_new_value, pattern="^(name|description|start_date|end_date)$"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            UPDATE_CAMPAIGN_ASK_FOR_CONFIRMATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, update_campaign_confirmation),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],            
            UPDATE_CAMPAIGN_SAVE_NEW_VALUE: [
                CallbackQueryHandler(update_campaign_save_new_value, pattern="^confirm_.*"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            DELETE_CAMPAIGN_ASK_FOR_CONFIRMATION: [
                CallbackQueryHandler(delete_campaign_confirmation, pattern=f"^{DELETE_PREFIX}.*"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            DELETE_CAMPAIGN_FROM_DB: [
                CallbackQueryHandler(delete_campaign_from_db, pattern="^confirm_.*"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            SUBMIT_CONTENT_GET_INPUT: [
                CallbackQueryHandler(submit_content_get_input, pattern="^submit_.*"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            SUBMIT_CONTENT_SAVE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, submit_content_save_input),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
            EXPORT_SUBMISSIONS: [
                CallbackQueryHandler(export_campaign_submissions, pattern="^export_.*"),
                CallbackQueryHandler(main_menu, pattern="^menu_main$"),
            ],
        },
        fallbacks=[
            CommandHandler("start", main_menu),
            CommandHandler("menu", main_menu),
            CommandHandler("help", main_menu),
            MessageHandler(filters.ALL, timeout_handler),
        ],
        allow_reentry=True,
        per_message=False,
        # conversation_timeout=60,
    )

    APP.add_handler(conv_handler)
    APP.bot.request.timeout = 30

    asyncio.create_task(start_maintenance_tasks())
    asyncio.create_task(start_workers(5))  # Schedule workers to run concurrently

    try:
        await APP.run_polling()
    finally:
        await APP.shutdown()

if __name__ == '__main__':
    clear()

    load_config()
    set_variables()

    nest_asyncio.apply()

    asyncio.run(main(TG_BOT_TOKEN))
