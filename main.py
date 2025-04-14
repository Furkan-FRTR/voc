import sys
import json
import time
import requests
import asyncio
import websockets
import threading
import os
import logging
from flask import Flask

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)

app = Flask(__name__)

GUILD_ID = os.getenv("GUILD_ID")
CHANNEL_ID = os.getenv("CHANNEL_ID")
USERTOKEN = os.getenv("USERTOKEN")

STATUS = "online"
SELF_MUTE = True
SELF_DEAF = True

HEADERS = {"Authorization": USERTOKEN, "Content-Type": "application/json"}

response = requests.get('https://discordapp.com/api/v9/users/@me', headers=HEADERS)
if response.status_code != 200:
    logging.error("Token invalide. Vérifiez vos identifiants.")
    sys.exit()

userinfo = response.json()
USERNAME = userinfo["username"]
DISCRIMINATOR = userinfo["discriminator"]
USERID = userinfo["id"]

reconnect_lock = asyncio.Lock()

async def heartbeat(ws, interval):
    """Envoi périodique d'un heartbeat au WebSocket."""
    try:
        while True:
            await asyncio.sleep(interval / 1000)
            await ws.send(json.dumps({"op": 1, "d": None}))
    except Exception as e:
        logging.error("Erreur lors du heartbeat : %s", e)

async def connect_voice():
    """Connexion au canal vocal via websocket en mode boucle, avec reconnexion en backoff."""
    backoff = 10 
    while True:
        try:
            async with websockets.connect(
                "wss://gateway.discord.gg/?v=9&encoding=json",
                max_size=None  
            ) as ws:
                start_payload = json.loads(await ws.recv())
                heartbeat_interval = start_payload['d']['heartbeat_interval']

                auth = {
                    "op": 2,
                    "d": {
                        "token": USERTOKEN,
                        "properties": {
                            "$os": os.name,
                            "$browser": "Chrome",
                            "$device": os.name
                        },
                        "presence": {
                            "status": STATUS,
                            "afk": False
                        }
                    }
                }

                vc_payload = {
                    "op": 4,
                    "d": {
                        "guild_id": GUILD_ID,
                        "channel_id": CHANNEL_ID,
                        "self_mute": SELF_MUTE,
                        "self_deaf": SELF_DEAF
                    }
                }

                await ws.send(json.dumps(auth))
                await ws.send(json.dumps(vc_payload))

                logging.info("Connecté en vocal en tant que %s#%s (%s)", USERNAME, DISCRIMINATOR, USERID)

                heartbeat_task = asyncio.create_task(heartbeat(ws, heartbeat_interval))
                backoff = 5

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)

                    if data.get("op") == 10:
                        heartbeat_interval = data['d']['heartbeat_interval']

                    elif data.get("t") == "VOICE_STATE_UPDATE":
                        state = data.get("d", {})
                        if state.get("user_id") == USERID and state.get("channel_id") is None:
                            async with reconnect_lock:
                                logging.warning("Expulsé du vocal ! Tentative de reconnexion...")
                                await asyncio.sleep(2)
                                await ws.send(json.dumps(vc_payload))
                                logging.info("Reconnexion envoyée.")
        except Exception as e:
            logging.error("Déconnexion WebSocket : %s. Nouvelle tentative dans %s secondes.", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

def start_flask():
    """Démarrage de l'application Flask en arrière-plan."""
    app.run(host='0.0.0.0', port=8080)

if __name__ == "__main__":
    threading.Thread(target=start_flask, daemon=True).start()
    asyncio.run(connect_voice())
