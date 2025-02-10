import sys
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main entry point of the program."""
    logger.info("Starting application...")

    # Example: Read command-line arguments
    parser = argparse.ArgumentParser(description="Example script with - and -- arguments")

    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose mode")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("-n", "--number", type=int, help="Specify a number")

    args = parser.parse_args()
    if args:
        logger.info(f"Arguments received: {args}")

    if args.verbose:
        logger.info("Verbose mode enabled")
    if args.debug:
        logger.info("Debug mode activated")
    if args.number is not None:
        print(f"Number provided: {args.number}")

    run_application()

    logger.info(f"Closing application..")


def run_application():
    """Placeholder function for application logic."""


# Ensure this script only runs when executed directly
if __name__ == "__main__":
    main()
