import asyncio
import logging
import os
from dotenv import load_dotenv

from dbsession import create_all
from store import Store

load_dotenv(override=True)

logging.basicConfig(
    format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
    level=logging.DEBUG if os.getenv("DEBUG", False) else logging.INFO,
    handlers=[logging.StreamHandler()],
)

logging.getLogger("sqlalchemy").setLevel(logging.ERROR)


_logger = logging.getLogger(__name__)
create_all(drop=False)

TARGET_BLOCK_COUNT = 7907964


async def main():
    # data_dir =  "Desktop/datadir/checked/"
    # folders = os.listdir(data_dir)
    # for folder in folders:
    #     path = os.path.join(data_dir, folder)
    #     if os.path.isdir(path):
    #         print(f"Processing {path}")
    #         store = Store(path)
    #         store.load_blocks(after_pruning_point=False)
    #         break

    store = Store("Desktop/datadir/checked/1")
    store.load_blocks(after_pruning_point=False)


if __name__ == "__main__":
    asyncio.run(main())
