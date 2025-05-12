import asyncio
import logging
import sys
from okx_data_pipeline.data_collector import OKXDataCollector
from okx_data_pipeline.data_processor import OKXPerformanceTest
from okx_data_pipeline.config import Config
from okx_data_pipeline.bigquery_client import BigQueryClient
from okx_data_pipeline.notifier import TelegramNotifier, exception_handler

logger = logging.getLogger(__name__)

async def main():
    config = Config()
    notifier = TelegramNotifier(config)
    sys.excepthook = exception_handler(notifier)
    bigquery_client = BigQueryClient(config, notifier)
    queue = asyncio.Queue()
    collector = OKXDataCollector(config, bigquery_client, notifier, queue)
    processor = OKXPerformanceTest(config, bigquery_client, notifier, queue)
    tasks = [
        collector.collect_data(),
        processor.run_test()
    ]
    await asyncio.gather(*tasks)

async def main_with_retry():
    config = Config()
    notifier = TelegramNotifier(config)
    while True:
        try:
            logger.info("Starting OKX Data Pipeline")
            await main()
        except KeyboardInterrupt:
            logger.info("Program stopped by user")
            sys.exit(0)
        except Exception as e:
            error_msg = f"Program crashed: {e}\nRestarting in 5 seconds..."
            logger.error(error_msg)
            await notifier.send_message(error_msg)
            await asyncio.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main_with_retry())
