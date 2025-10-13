# Initialize logger (will be reconfigured in main())
"""
Main runner for TidePool Data Collector
Automatically restarts the collector if it exits or crashes.
"""
import asyncio
import logging
import signal
import sys
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from src.collector import TidePoolCollector

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load configuration from YAML file"""
    config_file = Path(config_path)
    
    if not config_file.exists():
        logger.error(f"Configuration file {config_path} not found!")
        logger.info("Creating default config.yaml file...")
        create_default_config(config_path)
        logger.info("Please edit config.yaml and restart the application.")
        sys.exit(1)
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML config: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        sys.exit(1)

def create_default_config(config_path: str):
    """Create a default configuration file"""
    default_config = {
        'markets': [
            'BTC/USD',
            'ETH/USD',
            'ADA/USD'
        ],
        'restart_delay': 5,
        'log_level': 'INFO',
        'logging': {
            'file': 'tidepool.log',
            'format': '%(asctime)s - %(levelname)s - %(message)s'
        },
        'data': {
            'output_directory': 'data/markets/orderbook',
            'file_rotation': '24h',
            'max_rows_per_file': 1000
        }
    }
    
    with open(config_path, 'w') as f:
        yaml.dump(default_config, f, default_flow_style=False, indent=2)

def setup_logging(config: Dict[str, Any]):
    """Setup logging based on config"""
    log_config = config.get('logging', {})
    log_level = getattr(logging, config.get('log_level', 'INFO').upper())
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    log_file = log_config.get('file', 'tidepool.log')
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

class TidePoolRunner:
    def __init__(self, config: Dict[str, Any]):
        self.markets = config.get('markets', [])
        self.restart_delay = config.get('restart_delay', 5)
        self.log_level = config.get('log_level', 'INFO')
        self.collector: Optional[TidePoolCollector] = None
        self.should_restart = True
        
    async def run_collector(self):
        """Run a single instance of the collector"""
        try:
            logger.info("Starting TidePool Collector...")
            self.collector = TidePoolCollector(self.markets)
            await self.collector.run()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            self.should_restart = False
        except Exception as e:
            logger.error(f"Collector crashed with error: {e}")
            logger.exception("Full traceback:")
        finally:
            if self.collector:
                # Add any cleanup code here if needed
                pass
    
    async def run_with_restart(self):
        """Run the collector with automatic restart on failure"""
        while self.should_restart:
            try:
                await self.run_collector()
            except Exception as e:
                logger.error(f"Unexpected error in runner: {e}")
            
            if self.should_restart:
                logger.info(f"Restarting in {self.restart_delay} seconds...")
                await asyncio.sleep(self.restart_delay)
            else:
                logger.info("Shutdown requested, not restarting.")
                break
    
    def stop(self):
        """Stop the runner (called by signal handler)"""
        logger.info("Stop signal received")
        sys.exit(0)

# Global runner instance for signal handling
runner_instance: Optional[TidePoolRunner] = None

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}")
    if runner_instance:
        runner_instance.stop()

async def main():
    """Main entry point"""
    global runner_instance
    
    # Load configuration
    config = load_config()
    
    # Setup logging based on config
    setup_logging(config)
    
    # Create logger after logging is configured
    global logger
    logger = logging.getLogger(__name__)
    
    # Validate markets configuration
    markets = config.get('markets', [])
    if not markets:
        logger.error("No markets specified in config.yaml!")
        sys.exit(1)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run the collector with restart capability
    runner_instance = TidePoolRunner(config)
    logger.info(f"Starting TidePool Runner for markets: {', '.join(markets)}")
    logger.info(f"Restart delay: {config.get('restart_delay', 5)} seconds")
    logger.info(f"Log level: {config.get('log_level', 'INFO')}")
    
    try:
        await runner_instance.run_with_restart()
    except KeyboardInterrupt:
        logger.info("Main process interrupted")
    finally:
        logger.info("TidePool Runner shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process terminated by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)