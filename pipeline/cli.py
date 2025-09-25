import argparse
import os

from pipeline import __version__
from pipeline.logging import get_logger
from pipeline.orchestrator import run

log = get_logger("pipeline.cli") #Example of a log: 2025-01-01 00:00:00,000 - pipeline.cli - INFO - starting pipeline run

def main(argv: list[str] | None = None) -> int: #examples of argv: ['--version'], ['run', '--config', 'myconfig.yaml'], ['--env', 'dev']
    parser = argparse.ArgumentParser(description="Run the Pipeline(ML*) orchestrator")
    parser.add_argument("command", nargs="?", default="run", help="Command to execute (only 'run' is supported)")
    parser.add_argument("--config", dest="config_path", help="Path to a config YAML file")
    parser.add_argument("--env", dest="env_name", help="Named environment to resolve config.<env>.yaml")
    parser.add_argument("--version", action="store_true", help="Print version and exit")

    args = parser.parse_args(argv)

    if args.version:
        print(f"Pipeline(ML*) {__version__}")
        return 0

    if args.config_path:
        os.environ["PIPELINE_CONFIG_PATH"] = args.config_path
    if args.env_name:
        os.environ["PIPELINE_ENV"] = args.env_name

    if args.command != "run":
        parser.error("Unsupported command; only 'run' is available")

    log.info("starting pipeline run")
    run()
    log.info("pipeline run finished")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
