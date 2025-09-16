"""
Interactive dashboard utilities
"""

from pathlib import Path
from typing import Optional

import polars as pl

from .plots import create_analysis_dashboard


def main():
    """Main function for visualization CLI"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: log-anomaly-visualize <results_dir>")
        sys.exit(1)

    results_dir = sys.argv[1]
    output_file = create_analysis_dashboard(results_dir)
    print(f"Dashboard created: {output_file}")


if __name__ == "__main__":
    main()
