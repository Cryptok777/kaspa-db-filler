import asyncio
import logging
import os
from dotenv import load_dotenv

from BlocksProcessor import BlocksProcessor
from VirtualChainProcessor import VirtualChainProcessor
from dbsession import create_all, session_maker
from kaspad.KaspadMultiClient import KaspadMultiClient
from models.Transaction import Transaction

load_dotenv(override=True)

logging.basicConfig(
    format="%(asctime)s::%(levelname)s::%(name)s::%(message)s",
    level=logging.DEBUG if os.getenv("DEBUG", False) else logging.INFO,
    handlers=[logging.StreamHandler()],
)

# disable sqlalchemy notifications
logging.getLogger("sqlalchemy").setLevel(logging.ERROR)

# get file logger
_logger = logging.getLogger(__name__)

# create tables in database
create_all(drop=False)

kaspad_hosts = []

for i in range(100):
    try:
        kaspad_hosts.append(os.environ[f"KASPAD_HOST_{i + 1}"].strip())
    except KeyError:
        break

if not kaspad_hosts:
    raise Exception("Please set at least KASPAD_HOST_1 environment variable.")

# create Kaspad client
client = KaspadMultiClient(kaspad_hosts)
task_runner = None

async def get_start_block_hash():
    return '4b08c924f6649d562972ae188226ffab9061e4481e7b98fd7934759047bfc9a1'
    with session_maker() as s:
        try:
            return (
                s.query(Transaction)
                .where(Transaction.is_accepted == True)
                .order_by(Transaction.block_time.desc())
                .limit(1)
                .first()
                .accepting_block_hash
            )
        except AttributeError:
            return None
    
    
async def main():
    # initialize kaspads
    await client.initialize_all()

    # find last acceptedTx's block hash, when restarting this tool
    start_hash = await get_start_block_hash()

    # if there is nothing in the db, just get latest block.
    if not start_hash:
        daginfo = await client.request("getBlockDagInfoRequest", {})
        start_hash = daginfo["getBlockDagInfoResponse"]["pruningPointHash"]

    _logger.info(f"Start hash: {start_hash}")

    # create instances of blocksprocessor and virtualchainprocessor
    bp = BlocksProcessor(client)
    vcp = VirtualChainProcessor(client, start_hash)

    async def handle_blocks_commited(e):
        """
        this function is executed, when a new cluster of blocks were added to the database
        """
        global task_runner
        if task_runner and not task_runner.done():
            return

        if task_runner and task_runner.done():
            # check if there are exceptions in VCP task...
            _logger.debug("Checking exceptions in VCP")
            task_runner.result()

        _logger.debug('Update is_accepted for TXs.')
        task_runner = asyncio.create_task(vcp.update_accepted_info())

    # set up event to fire after adding new blocks
    bp.on_commited += handle_blocks_commited

    # start blocks processor working concurrent
    while True:
        try:
            await bp.loop(start_hash)
        except Exception:
            _logger.exception("Exception occured and script crashed. Restart in 1m")
            bp.synced = False
            await asyncio.sleep(60)
            start_hash = await get_start_block_hash()


if __name__ == "__main__":
    asyncio.run(main())
