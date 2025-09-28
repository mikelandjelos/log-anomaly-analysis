"""
Command-line interface for log anomaly analysis pipeline
"""

import sys
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from .core.modular_pipeline import ModularPipeline
from .visualization.plots import create_analysis_dashboard

console = Console()


@click.group()
@click.version_option()
def main():
    """Log Anomaly Analysis - Modular pipeline for log anomaly detection"""
    pass


@main.command()
@click.argument("config_path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--data-path",
    type=click.Path(exists=True, path_type=Path),
    help="Override dataset path from config",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    help="Override output directory from config",
)
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def run(
    config_path: Path,
    data_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    verbose: bool = False,
):
    """Run the anomaly detection pipeline"""

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:

            # Initialize pipeline
            task = progress.add_task("Initializing pipeline...", total=None)
            pipeline = ModularPipeline(str(config_path))

            # Override config if provided
            if data_path:
                pipeline.config.dataset.path = str(data_path)
            if output_dir:
                pipeline.config.output.directory = str(output_dir)

            progress.update(task, description="Running pipeline...")
            results, summary = pipeline.run()

            progress.update(task, description="Pipeline completed!", completed=True)

        # Display results
        _display_summary(summary)

        console.print(f"\n[green]✓[/green] Results saved to: {pipeline.output_dir}")

    except Exception as e:
        console.print(f"[red]✗[/red] Pipeline failed: {str(e)}")
        if verbose:
            console.print_exception()
        sys.exit(1)


@main.command()
@click.argument("results_dir", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--format", "output_format", default="png", help="Output format (png, html)"
)
@click.option("--interactive", is_flag=True, help="Create interactive plots")
def visualize(results_dir: Path, output_format: str, interactive: bool):
    """Create visualizations from pipeline results"""

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:

            task = progress.add_task("Creating visualizations...", total=None)

            output_file = create_analysis_dashboard(
                str(results_dir), output_format=output_format, interactive=interactive
            )

            progress.update(task, description="Visualizations created!", completed=True)

        console.print(f"[green]✓[/green] Visualizations saved to: {output_file}")

    except Exception as e:
        console.print(f"[red]✗[/red] Visualization failed: {str(e)}")
        sys.exit(1)


def _display_summary(summary: dict):
    """Display pipeline summary in a nice table"""
    table = Table(title="Pipeline Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Total Logs", f"{summary['total_logs']:,}")
    table.add_row("Unique Templates", f"{summary['unique_templates']:,}")
    table.add_row("Total Windows", f"{summary['total_windows']:,}")
    table.add_row("Anomalous Windows", f"{summary['anomalous_windows']:,}")
    table.add_row("Anomaly Rate", f"{summary['anomaly_rate']:.2%}")

    console.print(table)


if __name__ == "__main__":
    main()
