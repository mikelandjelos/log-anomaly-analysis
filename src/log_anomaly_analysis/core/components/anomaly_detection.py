"""
Anomaly detection component using PCA subspace method
"""

from typing import Dict

import numpy as np
import polars as pl
from loguru import logger
from scipy.stats import chi2
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
        self.use_tfidf = config.get("use_tfidf", True)
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
        pca = PCA(n_components=self.variance_threshold)
        pca.fit(event_matrix)

        # Compute anomaly scores using reconstruction error
        k = pca.n_components_
        logger.info(f"Selected {k} principal components")

        P = pca.components_.T  # Principal components matrix
        I = np.identity(event_matrix.shape[1])  # Identity matrix

        # Projection to anomaly subspace
        projection_matrix = I - P @ P.T
        projections = (projection_matrix @ event_matrix.T).T
        anomaly_scores = np.linalg.norm(projections, axis=1) ** 2

        # Compute threshold using chi-squared distribution
        threshold = chi2.ppf(1 - self.alpha, df=event_matrix.shape[1] - k)
        logger.info(f"Anomaly threshold: {threshold:.4f}")

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
