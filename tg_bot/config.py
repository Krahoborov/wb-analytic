from dataclasses import dataclass
import os

@dataclass
class TgBot:
    token: str




@dataclass
class Config:
    tg_bot: TgBot



def load_config(path: str = None):

    return Config(

        tg_bot=TgBot(
            token=os.getenv("BOT_TOKEN"),
        )
    )
