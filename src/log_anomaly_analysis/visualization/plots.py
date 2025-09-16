"""
Visualization and plotting utilities
"""

from pathlib import Path
from typing import Optional, Union

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
import seaborn as sns
from sklearn.manifold import TSNE


def plot_timeseries(
    anomaly_df: pl.DataFrame,
    time_based_index: bool = True,
    output_path: Optional[str] = None,
):
    """Plot time series of anomaly scores with anomalies highlighted"""

    # Convert to pandas for matplotlib
    df_pd = anomaly_df.to_pandas()

    if "AnomalyScore" not in df_pd.columns:
        raise ValueError("DataFrame must contain 'AnomalyScore' column")

    anomaly_scores = df_pd["AnomalyScore"]
    is_anomaly = df_pd.get("IsAnomaly", pd.Series([False] * len(df_pd)))

    plt.figure(figsize=(12, 6))
    plt.plot(
        anomaly_scores.index,
        anomaly_scores,
        color="blue",
        alpha=0.7,
        label="Anomaly Score",
    )

    # Highlight anomalies
    anomaly_points = df_pd[is_anomaly].index
    if len(anomaly_points) > 0:
        plt.scatter(
            anomaly_points,
            df_pd.loc[anomaly_points, "AnomalyScore"],
            color="red",
            s=50,
            label="Anomaly",
            zorder=5,
        )

    # Add threshold line if anomalies exist
    if is_anomaly.any():
        threshold = df_pd[is_anomaly]["AnomalyScore"].min() * 0.95
        plt.axhline(
            y=threshold, color="r", linestyle="--", alpha=0.5, label="Threshold"
        )

    plt.title("Anomaly Scores Over Windows")
    plt.xlabel("Window")
    plt.ylabel("Anomaly Score")
    plt.legend()
    plt.grid(True, alpha=0.3)

    if time_based_index and "WindowStart" in df_pd.columns:
        plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
        plt.gcf().autofmt_xdate()

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def plot_histogram(
    anomaly_scores: Union[pl.Series, np.ndarray], output_path: Optional[str] = None
):
    """Plot histogram of anomaly scores"""

    if isinstance(anomaly_scores, pl.Series):
        scores = anomaly_scores.to_numpy()
    else:
        scores = anomaly_scores

    plt.figure(figsize=(10, 6))
    sns.histplot(scores, bins=30, kde=True)
    plt.axvline(scores.mean(), color="red", linestyle="--", label="Mean")

    plt.title("Distribution of Anomaly Scores")
    plt.xlabel("Anomaly Score")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def plot_event_heatmap(
    event_count_matrix: pl.DataFrame,
    time_based_index: bool = True,
    output_path: Optional[str] = None,
):
    """Plot heatmap of top events by variance"""

    # Convert to pandas for plotting
    df_pd = event_count_matrix.to_pandas()

    # Get event columns (exclude metadata)
    metadata_cols = ["WindowStart", "WindowEnd", "LogCount"]
    event_cols = [col for col in df_pd.columns if col not in metadata_cols]

    if not event_cols:
        raise ValueError("No event columns found in matrix")

    event_data = df_pd[event_cols]

    # Select top events by variance
    top_events = event_data.var().sort_values(ascending=False).head(15).index.tolist()

    plt.figure(figsize=(12, 8))
    sns.heatmap(
        event_data[top_events].T,
        cmap="YlOrRd",
        xticklabels=30 if len(event_data) > 30 else True,
        yticklabels=True,
        cbar_kws={"label": "Event Count"},
    )

    plt.title("Event Frequency Heatmap (Top Events by Variance)")
    plt.xlabel("Time Window" if time_based_index else "Window Index")
    plt.ylabel("Event Template")
    plt.xticks(rotation=45)
    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def plot_tsne(
    event_count_matrix: pl.DataFrame,
    anomaly_scores: Union[pl.Series, np.ndarray],
    is_anomaly: Union[pl.Series, np.ndarray],
    output_path: Optional[str] = None,
):
    """Plot t-SNE visualization of event count matrix"""

    # Convert to pandas/numpy for sklearn
    df_pd = event_count_matrix.to_pandas()

    # Get event columns
    metadata_cols = ["WindowStart", "WindowEnd", "LogCount"]
    event_cols = [col for col in df_pd.columns if col not in metadata_cols]
    event_data = df_pd[event_cols].values

    if isinstance(anomaly_scores, pl.Series):
        scores = anomaly_scores.to_numpy()
    else:
        scores = anomaly_scores

    if isinstance(is_anomaly, pl.Series):
        anomalies = is_anomaly.to_numpy()
    else:
        anomalies = is_anomaly

    # Apply t-SNE
    tsne = TSNE(
        n_components=2, random_state=42, perplexity=min(30, len(event_data) - 1)
    )
    tsne_results = tsne.fit_transform(event_data)

    plt.figure(figsize=(10, 8))
    scatter = plt.scatter(
        tsne_results[:, 0],
        tsne_results[:, 1],
        c=scores,
        cmap="viridis",
        alpha=0.7,
        s=60,
    )

    # Mark anomalies with red border
    if anomalies.any():
        anomaly_indices = np.where(anomalies)[0]
        plt.scatter(
            tsne_results[anomaly_indices, 0],
            tsne_results[anomaly_indices, 1],
            facecolors="none",
            edgecolors="red",
            marker="o",
            s=120,
            linewidth=2,
            label="Anomaly",
        )

    plt.colorbar(scatter, label="Anomaly Score")
    plt.title("t-SNE Visualization of Event Count Matrix")
    plt.xlabel("t-SNE Dimension 1")
    plt.ylabel("t-SNE Dimension 2")

    if anomalies.any():
        plt.legend()

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches="tight")
        plt.close()
    else:
        plt.show()


def create_analysis_dashboard(
    results_dir: str, output_format: str = "png", interactive: bool = False
) -> str:
    """Create a comprehensive analysis dashboard"""

    results_path = Path(results_dir)

    # Load results
    try:
        anomalies = pl.read_parquet(results_path / "anomalies.parquet")
    except FileNotFoundError:
        raise FileNotFoundError(f"Anomaly results not found in {results_dir}")

    # Create output directory for plots
    plots_dir = results_path / "plots"
    plots_dir.mkdir(exist_ok=True)

    # Generate all plots
    if interactive:
        # Use plotly for interactive plots
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots
        except ImportError:
            raise ImportError(
                "plotly is required for interactive plots. Install with: poetry install --with visualization"
            )

        # Convert to pandas for plotly
        df_pd = anomalies.to_pandas()

        # Create subplots
        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=(
                "Anomaly Scores Over Time",
                "Score Distribution",
                "Anomaly Timeline",
                "Score vs Log Count",
            ),
            specs=[
                [{"secondary_y": False}, {"secondary_y": False}],
                [{"secondary_y": False}, {"secondary_y": False}],
            ],
        )

        # Plot 1: Time series
        fig.add_trace(
            go.Scatter(
                x=df_pd.index,
                y=df_pd["AnomalyScore"],
                mode="lines",
                name="Anomaly Score",
                line=dict(color="blue", width=1),
            ),
            row=1,
            col=1,
        )

        if "IsAnomaly" in df_pd.columns:
            anomaly_points = df_pd[df_pd["IsAnomaly"]]
            fig.add_trace(
                go.Scatter(
                    x=anomaly_points.index,
                    y=anomaly_points["AnomalyScore"],
                    mode="markers",
                    name="Anomalies",
                    marker=dict(color="red", size=8),
                ),
                row=1,
                col=1,
            )

        # Plot 2: Histogram
        fig.add_trace(
            go.Histogram(x=df_pd["AnomalyScore"], name="Score Distribution", nbinsx=30),
            row=1,
            col=2,
        )

        # Save interactive plot
        output_file = plots_dir / f"dashboard.html"
        fig.write_html(str(output_file))

    else:
        # Create static plots
        plt.style.use("default")
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle("Log Anomaly Analysis Dashboard", fontsize=16, fontweight="bold")

        # Convert to pandas for matplotlib
        df_pd = anomalies.to_pandas()

        # Plot 1: Time series
        axes[0, 0].plot(
            df_pd.index, df_pd["AnomalyScore"], color="blue", alpha=0.7, linewidth=1
        )
        if "IsAnomaly" in df_pd.columns:
            anomaly_points = df_pd[df_pd["IsAnomaly"]]
            axes[0, 0].scatter(
                anomaly_points.index,
                anomaly_points["AnomalyScore"],
                color="red",
                s=30,
                alpha=0.8,
                zorder=5,
            )
        axes[0, 0].set_title("Anomaly Scores Over Windows")
        axes[0, 0].set_xlabel("Window Index")
        axes[0, 0].set_ylabel("Anomaly Score")
        axes[0, 0].grid(True, alpha=0.3)

        # Plot 2: Histogram
        axes[0, 1].hist(
            df_pd["AnomalyScore"],
            bins=30,
            alpha=0.7,
            color="skyblue",
            edgecolor="black",
        )
        axes[0, 1].axvline(
            df_pd["AnomalyScore"].mean(), color="red", linestyle="--", label="Mean"
        )
        axes[0, 1].set_title("Anomaly Score Distribution")
        axes[0, 1].set_xlabel("Anomaly Score")
        axes[0, 1].set_ylabel("Frequency")
        axes[0, 1].legend()
        axes[0, 1].grid(True, alpha=0.3)

        # Plot 3: Box plot by anomaly status
        if "IsAnomaly" in df_pd.columns:
            df_pd["AnomalyType"] = df_pd["IsAnomaly"].map(
                {True: "Anomaly", False: "Normal"}
            )
            box_data = [
                df_pd[df_pd["AnomalyType"] == "Normal"]["AnomalyScore"].values,
                df_pd[df_pd["AnomalyType"] == "Anomaly"]["AnomalyScore"].values,
            ]
            axes[1, 0].boxplot(box_data, labels=["Normal", "Anomaly"])
            axes[1, 0].set_title("Anomaly Score Distribution by Type")
            axes[1, 0].set_ylabel("Anomaly Score")
            axes[1, 0].grid(True, alpha=0.3)
        else:
            axes[1, 0].text(
                0.5,
                0.5,
                "No anomaly labels available",
                transform=axes[1, 0].transAxes,
                ha="center",
                va="center",
            )

        # Plot 4: Log count vs anomaly score
        if "LogCount" in df_pd.columns:
            scatter = axes[1, 1].scatter(
                df_pd["LogCount"],
                df_pd["AnomalyScore"],
                c=df_pd.get("IsAnomaly", [False] * len(df_pd)),
                cmap="coolwarm",
                alpha=0.6,
            )
            axes[1, 1].set_title("Anomaly Score vs Log Count")
            axes[1, 1].set_xlabel("Logs per Window")
            axes[1, 1].set_ylabel("Anomaly Score")
            axes[1, 1].grid(True, alpha=0.3)
        else:
            axes[1, 1].text(
                0.5,
                0.5,
                "No log count data available",
                transform=axes[1, 1].transAxes,
                ha="center",
                va="center",
            )

        plt.tight_layout()
        output_file = plots_dir / f"dashboard.{output_format}"
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        plt.close()

    return str(output_file)
