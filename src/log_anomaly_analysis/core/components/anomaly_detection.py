"""
Anomaly detection component using PCA subspace method
"""

from typing import Dict

import numpy as np
import polars as pl
from loguru import logger
from scipy.stats import norm
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.preprocessing import StandardScaler

from .base import BaseComponent


class AnomalyDetectorComponent(BaseComponent):
    """Configurable PCA-based anomaly detection"""

    def _validate_config(self):
        pass

    def __init__(self, config: Dict):
        super().__init__(config)
        self.variance_threshold = config.get("variance_threshold", 0.95)
        self.alpha = config.get("alpha", 0.001)
        self.use_tfidf = config.get("use_tfidf", False)
        self.use_scaling = config.get("use_scaling", True)

    def process(self, data: pl.DataFrame) -> pl.DataFrame:
        """Detect anomalies using PCA subspace method"""
        if data.is_empty():
            return data

        logger.info("Starting PCA-based anomaly detection")

        # Separate metadata from event counts
        metadata_cols = ["WindowStart", "WindowEnd", "LogCount"]
        available_metadata = [col for col in metadata_cols if col in data.columns]

        # Get event count columns (exclude Window and metadata columns)
        exclude_cols = ["Window"] + available_metadata
        event_cols = [col for col in data.columns if col not in exclude_cols]

        if not event_cols:
            logger.warning("No event columns found for anomaly detection")
            return data.with_columns(
                [pl.lit(0.0).alias("AnomalyScore"), pl.lit(False).alias("IsAnomaly")]
            )

        # Extract event count matrix
        event_matrix = data.select(event_cols).to_numpy()

        if len(event_matrix) < 2:
            logger.warning("Insufficient data for anomaly detection")
            return data.with_columns(
                [pl.lit(0.0).alias("AnomalyScore"), pl.lit(False).alias("IsAnomaly")]
            )

        logger.info(f"Event matrix shape: {event_matrix.shape}")

        # Apply TF-IDF transformation if enabled
        if self.use_tfidf:
            logger.info("Applying TF-IDF transformation")
            tfidf_transformer = TfidfTransformer(
                norm="l2", use_idf=True, smooth_idf=True
            )
            event_matrix = tfidf_transformer.fit_transform(event_matrix).toarray()  # type: ignore

        # Apply standardization if enabled
        if self.use_scaling:
            logger.info("Applying standardization")
            scaler = StandardScaler()
            event_matrix = scaler.fit_transform(event_matrix)

        # Apply PCA
        logger.info(f"Applying PCA with variance threshold: {self.variance_threshold}")
        pca_full = PCA(svd_solver="full")
        pca_full.fit(event_matrix)

        eigenvalues = pca_full.explained_variance_
        if eigenvalues.size == 0:
            logger.warning("No variance in data; marking all windows as normal")
            return data.with_columns(
                [pl.lit(0.0).alias("AnomalyScore"), pl.lit(False).alias("IsAnomaly")]
            )

        cumulative_variance = np.cumsum(pca_full.explained_variance_ratio_)
        k = int(np.searchsorted(cumulative_variance, self.variance_threshold, side="left") + 1)
        k = max(1, min(k, eigenvalues.shape[0]))
        logger.info(f"Selected {k} principal components")

        principal_components = pca_full.components_[:k]
        P = principal_components.T
        I = np.identity(event_matrix.shape[1])

        centered = event_matrix - pca_full.mean_
        projection_matrix = I - P @ P.T
        residuals = centered @ projection_matrix
        anomaly_scores = np.linalg.norm(residuals, axis=1) ** 2

        residual_eigenvalues = eigenvalues[k:]
        if residual_eigenvalues.size == 0 or np.allclose(residual_eigenvalues, 0):
            logger.warning(
                "Residual eigenvalues degenerate; defaulting anomaly threshold to infinity"
            )
            threshold = float("inf")
        else:
            theta1 = residual_eigenvalues.sum()
            theta2 = np.sum(residual_eigenvalues ** 2)
            theta3 = np.sum(residual_eigenvalues ** 3)

            if theta2 <= 0:
                logger.warning(
                    "Residual variance too small; defaulting anomaly threshold to infinity"
                )
                threshold = float("inf")
            else:
                h0 = 1 - (2 * theta1 * theta3) / (3 * theta2 ** 2)
                if h0 <= 0:
                    logger.warning(
                        "Jackson–Mudholkar h0 <= 0; defaulting anomaly threshold to infinity"
                    )
                    threshold = float("inf")
                else:
                    z_alpha = norm.ppf(1 - self.alpha)
                    term = (
                        1
                        + (z_alpha * np.sqrt(2 * theta2 * h0 ** 2)) / theta1
                        + (theta2 * h0 * (h0 - 1)) / (theta1 ** 2)
                    )
                    threshold = theta1 * (term ** (1 / h0))
                    logger.info(f"Anomaly threshold (J-M): {threshold:.4f}")

        # Add results to original data
        result = data.with_columns(
            [
                pl.Series("AnomalyScore", anomaly_scores),
                pl.Series("IsAnomaly", anomaly_scores > threshold),
            ]
        )

        num_anomalies = (anomaly_scores > threshold).sum()
        logger.info(
            f"Detected {num_anomalies} anomalous windows out of {len(anomaly_scores)}"
        )

        return result
