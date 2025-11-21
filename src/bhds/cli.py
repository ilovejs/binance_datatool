"""
BIDS CLI - Binance Historical Data Service

Command-line interface for downloading, processing, and maintaining
Binance cryptocurrency market data using AWS historical archives and Binance APIs.
"""

import asyncio

import typer

from bdt_common.log_kit import logger
from bhds.tasks.aws_download import AwsDownloadTask
from bhds.tasks.holo_1m_kline import GenHolo1mKlineTask
from bhds.tasks.holo_resample import HoloResampleTask
from bhds.tasks.parse_aws_data import ParseAwsDataTask
from bhds.tasks.failed_files_task import FailedFilesTask

from . import __version__

app = typer.Typer(name="bhds", help="Binance Historical Data Service - CLI tool", add_completion=False)


@app.command()
def version():
    """Show version information."""
    typer.echo(f"Binance Historical Data Service - Version {__version__}")


@app.command()
def aws_download(config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for AWS download tasks")):
    """Run AWS download tasks from YAML configuration files."""
    for config_path in config_paths:
        try:
            task = AwsDownloadTask(config_path)
            asyncio.run(task.run())
        except Exception as e:
            logger.exception(f"Error running AWS download task for {config_path}: {e}")
            raise typer.Exit(1)


@app.command()
def parse_aws_data(
    config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for parse AWS data tasks")
):
    """Parse AWS downloaded data from CSV to Parquet with optional API completion."""
    for config_path in config_paths:
        try:
            task = ParseAwsDataTask(config_path)
            asyncio.run(task.run())
        except Exception as e:
            logger.exception(f"Error running parse AWS data task for {config_path}: {e}")
            raise typer.Exit(1)


@app.command()
def holo_1m_kline(
    config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for generate holo 1m kline tasks")
):
    """Generate holo 1m kline from parsed data."""
    for config_path in config_paths:
        try:
            GenHolo1mKlineTask(config_path).run()
        except Exception as e:
            logger.exception(f"Error running holo 1m kline task for {config_path}: {e}")
            raise typer.Exit(1)


@app.command()
def resample(config_paths: list[str] = typer.Argument(..., help="Paths to YAML configs for holo kline resample tasks")):
    """Resample holo 1m klines to higher time frames."""
    for config_path in config_paths:
        try:
            HoloResampleTask(config_path).run()
        except Exception as e:
            logger.exception(f"Error running resample task for {config_path}: {e}")
            raise typer.Exit(1)


@app.command()
def failed_files(
    list_files: bool = typer.Option(False, "--list", "-l", help="List all failed files"),
    retry: bool = typer.Option(False, "--retry", "-r", help="Retry downloading failed files"),
    clear: bool = typer.Option(False, "--clear", "-c", help="Clear the failed files tracker"),
):
    """Manage failed files (list, retry, clear)."""
    task = FailedFilesTask()
    
    if list_files:
        task.list_failed()
    
    if retry:
        task.retry()
        
    if clear:
        task.clear()
        
    if not (list_files or retry or clear):
        typer.echo("Please specify an action: --list, --retry, or --clear")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
