import argparse

from utils.config import Settings
from utils.logger import setup_logger
from job import JOB_REGISTRY


def main():
    parser = argparse.ArgumentParser(description="ETL Template Runner")
    parser.add_argument("--job", type=str, default=None, help="Job name to run")
    parser.add_argument("--list", action="store_true", help="List available jobs")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
    args = parser.parse_args()

    setup_logger(args.log_level)

    if args.list:
        print("Available Jobs:")
        for name in JOB_REGISTRY:
            print(f"  - {name}")
        return

    if not args.job:
        parser.error("--job is required (use --list to see available jobs)")

    if args.job not in JOB_REGISTRY:
        print(f"Unknown job: {args.job}. Use --list to see available jobs.")
        raise SystemExit(1)

    settings = Settings()
    job = JOB_REGISTRY[args.job]()
    job(settings)


if __name__ == "__main__":
    main()
