import asyncio
import getpass
from pathlib import Path
import qrcode
from telethon.errors import SessionPasswordNeededError


async def qr_login(client, png_path: str = "login_qr.png"):
    if await client.is_user_authorized():
        return

    while True:
        qr = await client.qr_login()
        _print_qr(qr.url)
        _save_png(qr.url, png_path)
        print(f"\nQR сохранён в файл: {Path(png_path).resolve()}")
        print("Отсканируй в Telegram: Settings → Devices → Link Desktop Device")
        print("Либо на телефоне наведи камеру на QR в терминале.\n")
        try:
            await qr.wait()
            break
        except SessionPasswordNeededError:
            pwd = getpass.getpass("Пароль 2FA: ")
            await client.sign_in(password=pwd)
            break
        except asyncio.TimeoutError:
            print("QR истёк, генерирую новый...")
            continue

    me = await client.get_me()
    print(f"[auth] вошёл как @{me.username or me.id}")


def _print_qr(data: str):
    q = qrcode.QRCode(border=1)
    q.add_data(data)
    q.make()
    q.print_ascii(invert=True)


def _save_png(data: str, path: str):
    img = qrcode.make(data)
    img.save(path)
