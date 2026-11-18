from telethon import TelegramClient


def make_client(cfg: dict) -> TelegramClient:
    tg = cfg["telegram"]
    return TelegramClient(
        tg["session_name"],
        tg["api_id"],
        tg["api_hash"],
        auto_reconnect=True,
        retry_delay=5,
        request_retries=5,
        connection_retries=-1,
        flood_sleep_threshold=60,
        catch_up=True,
    )
