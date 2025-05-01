"""
Main entry point for the crypto data collection application.
"""
import asyncio
import logging
from data_collector import DataCollector
from config import BUFFER_SIZE, DEBUG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

async def main():
    """Main application entry point."""
    try:
        logger.info("Starting crypto data collector")
        collector = DataCollector(buffer_size=BUFFER_SIZE, debug=DEBUG)
        await collector.collect_data(interval=60)
    except RuntimeError as e:
        logger.error(f"Application terminated due to initialization error: {e}")
        exit(1)
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        exit(1)

if __name__ == "__main__":
    logger.info("Initializing application")
    asyncio.run(main())