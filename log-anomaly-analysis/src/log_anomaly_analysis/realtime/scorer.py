"""
Incremental PCA (streaming) scorer using SPE (squared prediction error).

Warmup phase:
- Incrementally fit StandardScaler and IncrementalPCA via partial_fit
- After `warmup_windows` rows, freeze the model
- Select k via cumulative variance threshold
- Compute SPE threshold using Jackson–Mudholkar approximation with `alpha`

Scoring phase:
- Transform each batch, compute SPE, compare with threshold
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
from loguru import logger
from numpy.linalg import LinAlgError
from sklearn.decomposition import IncrementalPCA
from sklearn.preprocessing import StandardScaler


def _select_k(ev_ratio: np.ndarray, thr: float) -> int:
    c = np.cumsum(ev_ratio)
    k = int(np.searchsorted(c, thr, side="left") + 1)
    return max(1, min(k, ev_ratio.shape[0]))


def _spe_scores(X: np.ndarray, comps: np.ndarray, mean_vec: np.ndarray) -> np.ndarray:
    # numerically stable SPE (residual energy)
    C = X - mean_vec  # (n,p)
    T = C @ comps.T  # (n,k)
    R = C - T @ comps  # (n,p)
    return np.einsum("ij,ij->i", R, R).astype(np.float32)


def _jm_spe_threshold(eigs_resid: np.ndarray, alpha: float) -> float:
    if eigs_resid.size == 0 or np.allclose(eigs_resid, 0):
        return float("inf")
    theta1 = eigs_resid.sum()
    theta2 = np.sum(eigs_resid**2)
    theta3 = np.sum(eigs_resid**3)
    if theta2 <= 0:
        return float("inf")
    h0 = 1.0 - (2.0 * theta1 * theta3) / (3.0 * (theta2**2))
    if h0 <= 0:
        return float("inf")
    from scipy.stats import norm

    # Normal quantile at 1 - alpha
    z_alpha = float(norm.ppf(1.0 - alpha))
    term = (
        1.0
        + (z_alpha * np.sqrt(2.0 * theta2) * h0) / theta1
        + (theta2 * h0 * (h0 - 1.0)) / (theta1**2)
    )
    return float(theta1 * (term ** (1.0 / h0)))


@dataclass
class PCAScorerConfig:
    # Defaults aligned with examples/notebooks/bgl_realtime_rtclasses.ipynb
    variance_threshold: float = 0.90
    alpha: float = 0.001  # SPE tail probability
    use_scaling: bool = True
    warmup_windows: int = 4000
    max_components: Optional[int] = 128
    # ensure residual has at least this many eigenvalues
    min_residual_eigs: int = 64


class StreamingPCAScorer:
    """Incremental PCA scorer with warmup + freeze, SPE-only."""

    def __init__(self, config: PCAScorerConfig | dict | None = None):
        """Initialize PCA scorer with a config dict or dataclass.

        Accepts dict input to mirror other components' config loading. Missing
        keys fall back to sensible defaults used in the reference notebook.
        """
        if config is None:
            config = PCAScorerConfig()
        elif isinstance(config, dict):
            config = PCAScorerConfig(**config)

        self.variance_threshold = float(config.variance_threshold)
        self.alpha = float(config.alpha)
        self.use_scaling = bool(config.use_scaling)
        self.warmup_windows = int(config.warmup_windows)
        self.max_components = config.max_components
        self.min_residual_eigs = int(config.min_residual_eigs)

        self._scaler: Optional[StandardScaler] = (
            StandardScaler() if self.use_scaling else None
        )
        self._ipca: Optional[IncrementalPCA] = None
        self._n_features: Optional[int] = None
        self._n_components_target: Optional[int] = None

        self._buffer: list[np.ndarray] = []  # accumulate until batch >= n_components
        self._seen_windows: int = 0
        self._warmed: bool = False
        self._k: int = 0
        self._spe_threshold: float = float("inf")
        self._t2_threshold: float = float("inf")
        self._warm_count: int = 0

    @property
    def warmed(self) -> bool:
        return self._warmed

    @property
    def k(self) -> int:
        return self._k

    @property
    def spe_threshold(self) -> float:
        return self._spe_threshold

    def _ensure_ipca(self, p: int, batch_rows: int) -> None:
        if self._ipca is not None:
            return
        ncomp_cap = self.max_components if self.max_components is not None else 512
        # Choose target components independent of early batch size; delay partial_fit until enough rows
        n_components = int(min(ncomp_cap, p))
        self._ipca = IncrementalPCA(n_components=n_components)
        self._n_components_target = n_components
        self._n_features = p

    def _fit_partial(self, X: np.ndarray) -> None:
        if X.size == 0:
            return
        if self._scaler is not None:
            self._scaler = self._scaler.partial_fit(X)
            X = self._scaler.transform(X)

        X = X.astype(np.float32, copy=False)
        self._buffer.append(X)
        total_rows = sum(b.shape[0] for b in self._buffer)
        if self._ipca is None:
            self._ensure_ipca(p=X.shape[1], batch_rows=total_rows)

        # Only partial_fit when we have enough rows >= n_components (prefer margin)
        assert self._ipca is not None
        n_components = int(
            getattr(self._ipca, "n_components_", self._ipca.n_components)
        )
        # Require a safety margin to avoid ill-conditioned SVD
        if total_rows < max(n_components + 8, int(1.5 * n_components)):
            return

        buf = np.vstack(self._buffer).astype(np.float32)
        self._buffer.clear()
        # Robust partial_fit with backoff on SVD failure
        attempts = 0
        while True:
            try:
                self._ipca.partial_fit(buf)
                break
            except LinAlgError:
                attempts += 1
                old_nc = int(
                    getattr(self._ipca, "n_components_", self._ipca.n_components)
                )
                new_nc = max(2, int(old_nc * 0.75))
                logger.warning(
                    "IPCA partial_fit SVD did not converge (n_components=%d, rows=%d); reducing to %d",
                    old_nc,
                    buf.shape[0],
                    new_nc,
                )
                if new_nc >= old_nc or new_nc < 2 or attempts >= 3:
                    raise
                # Re-initialize IPCA with fewer components
                self._ipca = IncrementalPCA(n_components=new_nc)

    def partial_fit_warmup(self, X: np.ndarray) -> None:
        """Consume a batch during warmup (incremental fit)."""
        if self._warmed:
            return
        self._seen_windows += int(X.shape[0])
        self._warm_count = self._seen_windows
        self._fit_partial(X)
        if self._seen_windows >= self.warmup_windows:
            self._finalize_warmup()

    def _finalize_warmup(self) -> None:
        if self._ipca is None:
            # nothing to do
            self._warmed = True
            self._spe_threshold = float("inf")
            self._k = 0
            return
        # If buffer remains and has enough rows, include it in a last partial_fit to settle components.
        if self._buffer:
            pending = np.vstack(self._buffer).astype(np.float32)
            self._buffer.clear()
            try:
                self._ipca.partial_fit(pending)
            except Exception:
                # ignore if batch is too small for another partial update
                pass

        evr = getattr(self._ipca, "explained_variance_ratio_", None)
        ev = getattr(self._ipca, "explained_variance_", None)
        if evr is None or ev is None or ev.size == 0:
            self._k = 0
            self._spe_threshold = float("inf")
            self._warmed = True
            return

        self._k = _select_k(evr, self.variance_threshold)
        # Ensure residual exists and has enough DOF
        n_avail = int(ev.shape[0])
        if n_avail <= 1:
            self._k = 1
        else:
            max_k = max(1, n_avail - max(1, self.min_residual_eigs))
            self._k = max(1, min(self._k, max_k))
        resid = ev[self._k :]
        self._spe_threshold = _jm_spe_threshold(resid, self.alpha)
        # T^2 threshold based on F-approximation
        k = int(self._k)
        N = int(max(self._warm_count, k + 1))
        try:
            from scipy.stats import f as f_dist

            if N > k:
                self._t2_threshold = (
                    (k * (N - 1))
                    / max(1, (N - k))
                    * float(f_dist.ppf(1.0 - self.alpha, k, max(1, N - k)))
                )
            else:
                self._t2_threshold = float("inf")
        except Exception:
            # Fallback: chi-square approx with k dof
            try:
                from scipy.stats import chi2

                self._t2_threshold = float(chi2.ppf(1.0 - self.alpha, df=k))
            except Exception:
                self._t2_threshold = float("inf")
        # Instrument calibration summary
        try:
            n_components = getattr(self._ipca, "n_components_", self._ipca.n_components)
            logger.info(
                "PCA warmup finalized: p={}, n_components={}, k={}, residual_dof={}, SPE_threshold={:.6f}, T2_threshold={:.6f}",
                self._n_features,
                n_components,
                self._k,
                int(resid.shape[0]),
                self._spe_threshold,
                self._t2_threshold,
            )
        except Exception:
            pass
        self._warmed = True

    def score(self, X: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Compute SPE and T2 scores and anomaly flags for a batch.

        Returns (spe_scores, t2_scores, is_anomaly)
        """
        if X.size == 0:
            return (
                np.array([], dtype=np.float32),
                np.array([], dtype=np.float32),
                np.array([], dtype=bool),
            )

        if not self._warmed:
            # Still warming: fit incrementally, but do not score
            self.partial_fit_warmup(X)
            n = X.shape[0]
            return (
                np.zeros((n,), dtype=np.float32),
                np.zeros((n,), dtype=np.float32),
                np.zeros((n,), dtype=bool),
            )

        assert self._ipca is not None
        Xt = (
            self._scaler.transform(X).astype(np.float32)
            if self._scaler
            else X.astype(np.float32)
        )
        P = self._ipca.components_[: self._k]
        mu = (
            np.zeros_like(self._ipca.mean_, dtype=np.float32)
            if self._scaler
            else self._ipca.mean_
        )
        S = _spe_scores(Xt, P, mu)

        T = (Xt - mu) @ P.T
        lam = np.maximum(self._ipca.explained_variance_[: self._k], 1e-12)
        T2 = np.sum((T**2) / lam, axis=1, dtype=np.float32)

        flags = (S > self._spe_threshold) | (T2 > self._t2_threshold)
        return S.astype(np.float32), T2, flags
