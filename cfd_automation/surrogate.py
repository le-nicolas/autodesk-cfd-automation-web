from __future__ import annotations

from dataclasses import dataclass
import math
import random
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from .config_io import cases_to_csv
from .runner import AutomationRunner
from .utils import ensure_dir, now_utc_stamp, read_json, stable_hash, to_float, write_json

ProgressFn = Callable[[dict[str, Any]], None]


def _emit(callback: ProgressFn | None, **event: Any) -> None:
    if callback:
        callback(event)


def _operator_holds(value: float | None, operator: str, threshold: float) -> bool:
    if value is None:
        return False
    if operator == "<":
        return value < threshold
    if operator == "<=":
        return value <= threshold
    if operator == ">":
        return value > threshold
    if operator == ">=":
        return value >= threshold
    if operator == "==":
        return value == threshold
    if operator == "!=":
        return value != threshold
    return False


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _normalize_case_value(value: Any) -> Any:
    truth = _as_bool(value)
    if truth is not None:
        return truth
    numeric = to_float(value)
    if numeric is not None:
        return round(float(numeric), 9)
    if value is None:
        return ""
    return str(value).strip()


def _to_series_numeric(series: pd.Series) -> pd.Series:
    def _coerce(value: Any) -> float | None:
        truth = _as_bool(value)
        if truth is not None:
            return 1.0 if truth else 0.0
        return to_float(value)

    return series.map(_coerce)


@dataclass
class HarvestResult:
    rows: list[dict[str, Any]]
    deduplicated_count: int
    sources: dict[str, int]
    parameter_aliases: list[str]
    metric_aliases: list[str]


class SurrogateEngine:
    def __init__(self, project_root: Path, runner: AutomationRunner):
        self.project_root = project_root
        self.runner = runner
        self.runtime_dir = ensure_dir(project_root / "runtime")
        self.surrogate_dir = ensure_dir(self.runtime_dir / "surrogate")
        self.training_data_path = self.surrogate_dir / "training_data.csv"
        self.model_path = self.surrogate_dir / "model.pkl"
        self.metadata_path = self.surrogate_dir / "metadata.json"
        self.coverage_path = self.surrogate_dir / "coverage.json"
        self._bundle_cache: dict[str, Any] | None = None

    @staticmethod
    def _default_objective_alias(config: dict[str, Any]) -> str:
        ranking = config.get("ranking", []) if isinstance(config.get("ranking", []), list) else []
        for item in ranking:
            if isinstance(item, dict) and str(item.get("alias", "")).strip():
                return str(item.get("alias", "")).strip()
        metrics = config.get("metrics", []) if isinstance(config.get("metrics", []), list) else []
        for item in metrics:
            if isinstance(item, dict) and str(item.get("alias", "")).strip():
                return str(item.get("alias", "")).strip()
        return ""

    def _resolve_runtime_path(self, path_value: str, run_dir: Path | None = None) -> Path | None:
        text = str(path_value or "").strip()
        if not text:
            return None
        raw = Path(text)
        candidates: list[Path] = []
        if raw.is_absolute():
            candidates.append(raw)
        else:
            candidates.append(self.project_root / raw)
            candidates.append(self.runtime_dir / raw)
            if run_dir is not None:
                candidates.append(run_dir / raw)
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _normalize_params(params: dict[str, Any]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, value in params.items():
            name = str(key).strip()
            if not name or name == "case_id":
                continue
            out[name] = _normalize_case_value(value)
        return out

    @staticmethod
    def _normalize_metrics(metrics: dict[str, Any]) -> dict[str, float]:
        out: dict[str, float] = {}
        for key, value in metrics.items():
            alias = str(key).strip()
            if not alias:
                continue
            numeric = to_float(value)
            if numeric is None or math.isnan(float(numeric)) or math.isinf(float(numeric)):
                continue
            out[alias] = float(numeric)
        return out

    @staticmethod
    def _record_dedupe_key(params: dict[str, Any]) -> str:
        return stable_hash({"params": params})

    def harvest_training_rows(
        self,
        *,
        include_design_loops: bool = True,
        objective_alias: str | None = None,
    ) -> HarvestResult:
        dedup: dict[str, dict[str, Any]] = {}
        source_counts = {"runs": 0, "design_loops": 0}

        runs_root = self.runtime_dir / "runs"
        if runs_root.exists():
            for run_dir in sorted([p for p in runs_root.iterdir() if p.is_dir()]):
                summary_path = run_dir / "run_summary.json"
                if not summary_path.exists():
                    continue
                summary = read_json(summary_path, default={}) or {}
                if not isinstance(summary, dict):
                    continue
                run_id = str(summary.get("run_id", run_dir.name))
                for case_result in summary.get("case_results", []):
                    if not isinstance(case_result, dict) or not case_result.get("success"):
                        continue
                    metrics = self._normalize_metrics(
                        case_result.get("metrics", {})
                        if isinstance(case_result.get("metrics", {}), dict)
                        else {}
                    )
                    if not metrics:
                        continue

                    payload_path = self._resolve_runtime_path(
                        str(case_result.get("payload_path", "")),
                        run_dir=run_dir,
                    )
                    payload = read_json(payload_path, default={}) if payload_path else {}
                    case_payload = payload.get("case", {}) if isinstance(payload, dict) else {}
                    if not isinstance(case_payload, dict):
                        case_payload = {}
                    params = self._normalize_params(case_payload)
                    if not params:
                        continue

                    case_id = str(case_result.get("case_id", "")).strip()
                    row = {
                        "source": "run",
                        "source_id": run_id,
                        "run_id": run_id,
                        "loop_id": "",
                        "batch_index": "",
                        "case_id": case_id,
                        "params": params,
                        "metrics": metrics,
                    }
                    key = self._record_dedupe_key(params)
                    dedup[key] = row
                    source_counts["runs"] += 1

        if include_design_loops:
            loops_root = self.runtime_dir / "design_loops"
            if loops_root.exists():
                for loop_dir in sorted([p for p in loops_root.iterdir() if p.is_dir()]):
                    loop_id = loop_dir.name
                    for batch_path in sorted(loop_dir.glob("batch_*/batch_summary.json")):
                        batch = read_json(batch_path, default={}) or {}
                        if not isinstance(batch, dict):
                            continue
                        batch_index = _safe_int(batch.get("batch_index", 0), 0)
                        for case_item in batch.get("cases", []):
                            if not isinstance(case_item, dict) or not case_item.get("success"):
                                continue
                            params_raw = case_item.get("params", {}) if isinstance(case_item.get("params", {}), dict) else {}
                            metrics_raw = case_item.get("metrics", {}) if isinstance(case_item.get("metrics", {}), dict) else {}
                            params = self._normalize_params(params_raw)
                            metrics = self._normalize_metrics(metrics_raw)
                            if not params or not metrics:
                                continue
                            row = {
                                "source": "design_loop",
                                "source_id": f"{loop_id}/batch_{batch_index:02d}",
                                "run_id": str(batch.get("run_id", "")).strip(),
                                "loop_id": loop_id,
                                "batch_index": batch_index,
                                "case_id": str(case_item.get("case_id", "")).strip(),
                                "params": params,
                                "metrics": metrics,
                            }
                            key = self._record_dedupe_key(params)
                            dedup[key] = row
                            source_counts["design_loops"] += 1

        rows = list(dedup.values())
        rows.sort(key=lambda item: (str(item.get("source_id", "")), str(item.get("case_id", ""))))

        all_param_aliases = sorted(
            {alias for item in rows for alias in item.get("params", {}).keys()}
        )
        all_metric_aliases = sorted(
            {alias for item in rows for alias in item.get("metrics", {}).keys()}
        )

        if objective_alias:
            objective = str(objective_alias).strip()
            rows = [item for item in rows if objective in item.get("metrics", {})]

        return HarvestResult(
            rows=rows,
            deduplicated_count=len(rows),
            sources=source_counts,
            parameter_aliases=all_param_aliases,
            metric_aliases=all_metric_aliases,
        )

    @staticmethod
    def _compute_derived_features(flat_record: dict[str, Any]) -> dict[str, Any]:
        # Optional derived Reynolds number when required columns are present.
        velocity = to_float(flat_record.get("param__inlet_velocity_ms"))
        length = to_float(flat_record.get("param__characteristic_length_m"))
        if length is None:
            length = to_float(flat_record.get("param__hydraulic_diameter_m"))
        nu = to_float(flat_record.get("param__kinematic_viscosity_m2_s"))
        if nu is None:
            nu = to_float(flat_record.get("param__fluid_kinematic_viscosity_m2_s"))
        if velocity is not None and length is not None and nu is not None and nu != 0:
            flat_record["param__derived_reynolds_number"] = (velocity * length) / nu
        return flat_record

    @staticmethod
    def _flatten_rows(rows: list[dict[str, Any]]) -> tuple[pd.DataFrame, list[str], list[str]]:
        records: list[dict[str, Any]] = []
        param_aliases = sorted({alias for item in rows for alias in item.get("params", {}).keys()})
        metric_aliases = sorted({alias for item in rows for alias in item.get("metrics", {}).keys()})

        for item in rows:
            record: dict[str, Any] = {
                "source": item.get("source", ""),
                "source_id": item.get("source_id", ""),
                "run_id": item.get("run_id", ""),
                "loop_id": item.get("loop_id", ""),
                "batch_index": item.get("batch_index", ""),
                "case_id": item.get("case_id", ""),
            }
            params = item.get("params", {}) if isinstance(item.get("params", {}), dict) else {}
            metrics = item.get("metrics", {}) if isinstance(item.get("metrics", {}), dict) else {}
            for alias in param_aliases:
                record[f"param__{alias}"] = params.get(alias)
            for alias in metric_aliases:
                record[f"metric__{alias}"] = metrics.get(alias)
            record = SurrogateEngine._compute_derived_features(record)
            records.append(record)

        frame = pd.DataFrame(records)
        derived_cols = sorted([c for c in frame.columns if c.startswith("param__derived_")])
        for col in derived_cols:
            base_alias = col.replace("param__", "", 1)
            if base_alias not in param_aliases:
                param_aliases.append(base_alias)
        return frame, sorted(param_aliases), metric_aliases
    @staticmethod
    def _detect_feature_types(frame: pd.DataFrame, param_aliases: list[str]) -> tuple[list[str], list[str]]:
        numeric_cols: list[str] = []
        categorical_cols: list[str] = []
        for alias in param_aliases:
            col = f"param__{alias}"
            if col not in frame.columns:
                continue
            series = frame[col]
            numeric = _to_series_numeric(series)
            valid_ratio = float(numeric.notna().mean()) if len(numeric) else 0.0
            if valid_ratio >= 0.9:
                numeric_cols.append(col)
            else:
                categorical_cols.append(col)
        return numeric_cols, categorical_cols

    @staticmethod
    def _build_schema(
        frame: pd.DataFrame,
        *,
        numeric_cols: list[str],
        categorical_cols: list[str],
    ) -> dict[str, Any]:
        schema: dict[str, Any] = {
            "numeric": {},
            "categorical": {},
            "encoded_columns": [],
        }

        for col in numeric_cols:
            series = _to_series_numeric(frame[col])
            valid = series.dropna()
            if valid.empty:
                stats = {"min": 0.0, "max": 1.0, "median": 0.0}
            else:
                stats = {
                    "min": float(valid.min()),
                    "max": float(valid.max()),
                    "median": float(valid.median()),
                }
            schema["numeric"][col] = stats
            schema["encoded_columns"].append(f"num::{col}")

        for col in categorical_cols:
            categories = sorted({str(value).strip().lower() for value in frame[col].fillna("").tolist() if str(value).strip()})
            categories = ["__missing__"] + [item for item in categories if item != "__missing__"]
            if "__unknown__" not in categories:
                categories.append("__unknown__")
            schema["categorical"][col] = categories
            for category in categories:
                schema["encoded_columns"].append(f"cat::{col}::{category}")

        return schema

    @staticmethod
    def _encode_frame(frame: pd.DataFrame, schema: dict[str, Any]) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
        encoded_records: list[dict[str, float]] = []
        row_meta: list[dict[str, Any]] = []
        numeric_schema = schema.get("numeric", {}) if isinstance(schema.get("numeric", {}), dict) else {}
        categorical_schema = schema.get("categorical", {}) if isinstance(schema.get("categorical", {}), dict) else {}
        encoded_columns = list(schema.get("encoded_columns", []))

        for _, row in frame.iterrows():
            encoded: dict[str, float] = {name: 0.0 for name in encoded_columns}

            for col, stats in numeric_schema.items():
                raw = row.get(col)
                numeric = to_float(raw)
                if numeric is None:
                    numeric = _safe_float(stats.get("median"), 0.0)
                min_val = _safe_float(stats.get("min"), 0.0)
                max_val = _safe_float(stats.get("max"), 1.0)
                if max_val <= min_val:
                    norm = 0.0
                else:
                    norm = (float(numeric) - min_val) / (max_val - min_val)
                norm = max(0.0, min(1.0, float(norm)))
                encoded[f"num::{col}"] = norm

            for col, categories in categorical_schema.items():
                raw = row.get(col)
                token = str(raw).strip().lower() if str(raw).strip() else "__missing__"
                if token not in categories:
                    token = "__unknown__"
                encoded[f"cat::{col}::{token}"] = 1.0

            encoded_records.append(encoded)
            row_meta.append(
                {
                    "case_id": row.get("case_id", ""),
                    "source_id": row.get("source_id", ""),
                }
            )

        encoded_frame = pd.DataFrame(encoded_records)
        if encoded_frame.empty:
            encoded_frame = pd.DataFrame(columns=encoded_columns)
        return encoded_frame, row_meta

    @staticmethod
    def _coverage_from_encoded(X: pd.DataFrame, schema: dict[str, Any]) -> dict[str, Any]:
        numeric_cols = list((schema.get("numeric") or {}).keys())
        feature_bins: dict[str, float] = {}
        for col in numeric_cols:
            encoded_col = f"num::{col}"
            if encoded_col not in X.columns or X.empty:
                feature_bins[col.replace("param__", "", 1)] = 0.0
                continue
            values = X[encoded_col]
            occupied = set(int(min(9, max(0, math.floor(float(v) * 10)))) for v in values.tolist())
            feature_bins[col.replace("param__", "", 1)] = float(len(occupied) / 10.0)

        overall = float(sum(feature_bins.values()) / len(feature_bins)) if feature_bins else 0.0

        map_payload: dict[str, Any] = {
            "x_feature": "",
            "y_feature": "",
            "x_bins": [],
            "y_bins": [],
            "cells": [],
            "legend": {"0": "low", "1": "medium", "2": "high"},
        }
        if not X.empty and len(numeric_cols) >= 1:
            x_col = f"num::{numeric_cols[0]}"
            y_col = f"num::{numeric_cols[1 if len(numeric_cols) > 1 else 0]}"
            grid = 18
            counts = [[0 for _ in range(grid)] for _ in range(grid)]
            for _, row in X.iterrows():
                xv = float(row.get(x_col, 0.0))
                yv = float(row.get(y_col, 0.0))
                xi = min(grid - 1, max(0, int(math.floor(xv * grid))))
                yi = min(grid - 1, max(0, int(math.floor(yv * grid))))
                counts[grid - 1 - yi][xi] += 1

            non_zero = [count for line in counts for count in line if count > 0]
            low_threshold = 0
            high_threshold = 0
            if non_zero:
                ordered = sorted(non_zero)
                low_threshold = ordered[max(0, int(len(ordered) * 0.35) - 1)]
                high_threshold = ordered[max(0, int(len(ordered) * 0.7) - 1)]

            levels: list[list[int]] = []
            for line in counts:
                level_line: list[int] = []
                for count in line:
                    if count <= 0:
                        level_line.append(0)
                    elif count >= max(1, high_threshold):
                        level_line.append(2)
                    elif count >= max(1, low_threshold):
                        level_line.append(1)
                    else:
                        level_line.append(0)
                levels.append(level_line)

            map_payload = {
                "x_feature": numeric_cols[0].replace("param__", "", 1),
                "y_feature": numeric_cols[1 if len(numeric_cols) > 1 else 0].replace("param__", "", 1),
                "x_bins": [round(i / grid, 3) for i in range(grid + 1)],
                "y_bins": [round(i / grid, 3) for i in range(grid + 1)],
                "cells": levels,
                "legend": {"0": "low", "1": "medium", "2": "high"},
            }

        return {
            "overall": overall,
            "per_feature": feature_bins,
            "map": map_payload,
        }

    @staticmethod
    def _model_candidates(row_count: int) -> list[tuple[str, Any]]:
        try:
            from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import RBF, WhiteKernel
            from sklearn.neural_network import MLPRegressor
        except Exception as ex:
            raise RuntimeError(
                "scikit-learn is required for surrogate training. Install scikit-learn and retry."
            ) from ex

        models: list[tuple[str, Any]] = [
            (
                "gradient_boosting",
                GradientBoostingRegressor(random_state=42),
            ),
            (
                "random_forest",
                RandomForestRegressor(n_estimators=300, random_state=42),
            ),
            (
                "neural_net",
                MLPRegressor(hidden_layer_sizes=(64, 64), max_iter=900, random_state=42),
            ),
        ]

        if row_count <= 700:
            models.append(
                (
                    "gaussian_process",
                    GaussianProcessRegressor(
                        kernel=RBF(length_scale=1.0) + WhiteKernel(noise_level=1e-3),
                        normalize_y=True,
                        random_state=42,
                    ),
                )
            )

        try:
            from xgboost import XGBRegressor

            models.append(
                (
                    "xgboost",
                    XGBRegressor(
                        n_estimators=350,
                        learning_rate=0.05,
                        max_depth=6,
                        subsample=0.95,
                        colsample_bytree=0.95,
                        random_state=42,
                        objective="reg:squarederror",
                    ),
                )
            )
        except Exception:
            # Optional dependency.
            pass

        return models

    def _save_bundle(self, bundle: dict[str, Any]) -> None:
        try:
            import joblib
        except Exception as ex:
            raise RuntimeError("joblib is required for surrogate persistence.") from ex
        joblib.dump(bundle, self.model_path)
        self._bundle_cache = bundle

    def _load_bundle(self) -> dict[str, Any] | None:
        if self._bundle_cache is not None:
            return self._bundle_cache
        if not self.model_path.exists():
            return None
        try:
            import joblib
        except Exception:
            return None
        bundle = joblib.load(self.model_path)
        if isinstance(bundle, dict):
            self._bundle_cache = bundle
            return bundle
        return None
    def train(
        self,
        *,
        objective_alias: str | None = None,
        include_design_loops: bool = True,
        min_rows: int = 50,
    ) -> dict[str, Any]:
        config = self.runner.get_config()
        target_alias = str(objective_alias or self._default_objective_alias(config)).strip()
        if not target_alias:
            raise ValueError("No objective alias is configured. Set ranking/metrics aliases first.")

        harvest = self.harvest_training_rows(
            include_design_loops=include_design_loops,
            objective_alias=target_alias,
        )
        if len(harvest.rows) < min_rows:
            raise ValueError(
                f"Not enough successful historical rows to train surrogate: {len(harvest.rows)} found, {min_rows} required."
            )

        flat, param_aliases, metric_aliases = self._flatten_rows(harvest.rows)
        if flat.empty:
            raise ValueError("No rows available after flattening historical records.")

        if f"metric__{target_alias}" not in flat.columns:
            raise ValueError(
                f"Objective alias '{target_alias}' is missing from harvested metrics."
            )

        y = pd.to_numeric(flat[f"metric__{target_alias}"], errors="coerce")
        valid_mask = y.notna()
        flat = flat.loc[valid_mask].reset_index(drop=True)
        y = y.loc[valid_mask].reset_index(drop=True)
        if len(flat) < min_rows:
            raise ValueError(
                f"Not enough rows with valid objective metric '{target_alias}': {len(flat)} found, {min_rows} required."
            )

        self.training_data_path.write_text(flat.to_csv(index=False), encoding="utf-8")

        numeric_cols, categorical_cols = self._detect_feature_types(flat, param_aliases)
        if not numeric_cols and not categorical_cols:
            raise ValueError("No usable parameter columns found for surrogate features.")

        schema = self._build_schema(flat, numeric_cols=numeric_cols, categorical_cols=categorical_cols)
        X, _ = self._encode_frame(flat, schema)
        if X.empty:
            raise ValueError("Feature encoding produced an empty matrix.")

        try:
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
            from sklearn.model_selection import train_test_split
            from sklearn.neighbors import NearestNeighbors
        except Exception as ex:
            raise RuntimeError(
                "scikit-learn is required for surrogate training. Install scikit-learn and retry."
            ) from ex

        test_fraction = 0.2
        if len(X) < 25:
            test_fraction = 0.25
        if len(X) < 12:
            test_fraction = 0.34

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_fraction,
            random_state=42,
        )

        model_scores: list[dict[str, Any]] = []
        best_name = ""
        best_model = None
        best_r2 = -1e18

        for model_name, model in self._model_candidates(len(X)):
            try:
                model.fit(X_train, y_train)
                preds = model.predict(X_test)
                r2 = float(r2_score(y_test, preds)) if len(y_test) > 1 else 0.0
                mae = float(mean_absolute_error(y_test, preds))
                rmse = float(math.sqrt(mean_squared_error(y_test, preds)))
                score_row = {
                    "name": model_name,
                    "r2": r2,
                    "mae": mae,
                    "rmse": rmse,
                    "status": "ok",
                }
                model_scores.append(score_row)
                if r2 > best_r2:
                    best_r2 = r2
                    best_name = model_name
                    best_model = model
            except Exception as ex:
                model_scores.append(
                    {
                        "name": model_name,
                        "status": "failed",
                        "error": str(ex),
                    }
                )

        if best_model is None:
            raise RuntimeError("All surrogate model candidates failed to train.")

        best_model.fit(X, y)

        confidence_method = "nearest_neighbor"
        std_reference = 0.0
        nn_model = NearestNeighbors(n_neighbors=max(1, min(8, len(X))))
        nn_model.fit(X)
        nn_distances, _ = nn_model.kneighbors(X)
        if nn_distances.shape[1] > 1:
            train_distance = nn_distances[:, 1:].mean(axis=1)
        else:
            train_distance = nn_distances[:, 0]
        distance_scale = float(pd.Series(train_distance).quantile(0.9)) if len(train_distance) else 1.0
        distance_scale = max(distance_scale, 1e-9)

        try:
            _, std = best_model.predict(X, return_std=True)
            std_series = pd.Series(std)
            std_reference = max(1e-9, float(std_series.quantile(0.95)))
            confidence_method = "gaussian_std"
        except Exception:
            confidence_method = "nearest_neighbor"

        coverage = self._coverage_from_encoded(X, schema)
        ready = bool(len(flat) >= 50 and best_r2 >= 0.75)

        bundle = {
            "trained_at": now_utc_stamp(),
            "target_alias": target_alias,
            "model_name": best_name,
            "model": best_model,
            "schema": schema,
            "metric_aliases": metric_aliases,
            "parameter_aliases": param_aliases,
            "confidence": {
                "method": confidence_method,
                "std_reference": std_reference,
                "distance_scale": distance_scale,
            },
            "nn_model": nn_model,
            "train_row_count": int(len(X_train)),
            "test_row_count": int(len(X_test)),
            "row_count": int(len(flat)),
            "coverage": coverage,
            "model_scores": model_scores,
            "best_r2": float(best_r2),
            "ready": ready,
            "sources": harvest.sources,
        }
        self._save_bundle(bundle)

        metadata = {
            "trained": True,
            "trained_at": bundle["trained_at"],
            "target_alias": target_alias,
            "model_name": best_name,
            "row_count": int(len(flat)),
            "train_row_count": int(len(X_train)),
            "test_row_count": int(len(X_test)),
            "best_r2": float(best_r2),
            "ready": ready,
            "coverage": coverage,
            "model_scores": model_scores,
            "sources": harvest.sources,
            "parameter_aliases": param_aliases,
            "metric_aliases": metric_aliases,
            "objective_alias_requested": objective_alias or "",
        }
        write_json(self.metadata_path, metadata)
        write_json(self.coverage_path, coverage)
        return metadata

    def status(self) -> dict[str, Any]:
        if not self.metadata_path.exists():
            return {
                "trained": False,
                "ready": False,
                "row_count": 0,
                "model_name": "",
                "target_alias": "",
                "message": "No surrogate model has been trained yet.",
            }
        metadata = read_json(self.metadata_path, default={}) or {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.setdefault("trained", False)
        metadata.setdefault("ready", False)
        metadata.setdefault("row_count", 0)
        metadata.setdefault("model_name", "")
        metadata.setdefault("target_alias", "")
        metadata["training_data_csv"] = str(self.training_data_path) if self.training_data_path.exists() else ""
        metadata["model_path"] = str(self.model_path) if self.model_path.exists() else ""
        return metadata

    def coverage(self) -> dict[str, Any]:
        if self.coverage_path.exists():
            payload = read_json(self.coverage_path, default={}) or {}
            if isinstance(payload, dict):
                return payload
        status = self.status()
        coverage = status.get("coverage", {}) if isinstance(status.get("coverage", {}), dict) else {}
        if coverage:
            return coverage
        return {
            "overall": 0.0,
            "per_feature": {},
            "map": {
                "x_feature": "",
                "y_feature": "",
                "x_bins": [],
                "y_bins": [],
                "cells": [],
                "legend": {"0": "low", "1": "medium", "2": "high"},
            },
        }

    def _encode_input_rows(self, rows: list[dict[str, Any]]) -> tuple[pd.DataFrame, list[dict[str, Any]], list[str]]:
        bundle = self._load_bundle()
        if not bundle:
            raise ValueError("Surrogate model is not trained. Train first.")

        schema = bundle.get("schema", {}) if isinstance(bundle.get("schema", {}), dict) else {}
        parameter_aliases = bundle.get("parameter_aliases", []) if isinstance(bundle.get("parameter_aliases", []), list) else []

        records: list[dict[str, Any]] = []
        warnings: list[str] = []
        for idx, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                raise ValueError(f"Each row must be an object. Row index {idx} is invalid.")
            record: dict[str, Any] = {
                "case_id": str(row.get("case_id", f"PRED_{idx:05d}")),
                "source_id": "predict",
            }
            for alias in parameter_aliases:
                record[f"param__{alias}"] = row.get(alias, "")
            record = self._compute_derived_features(record)
            records.append(record)

            extra = sorted(set(row.keys()) - set(parameter_aliases) - {"case_id"})
            if extra:
                warnings.append(
                    f"Row {idx} has unmapped parameters ignored by surrogate: {', '.join(extra)}"
                )

        frame = pd.DataFrame(records)
        encoded, row_meta = self._encode_frame(frame, schema)
        return encoded, row_meta, warnings

    def _confidence_from_predictions(self, X: pd.DataFrame, predictions: Any) -> tuple[list[float], list[float]]:
        bundle = self._load_bundle()
        if not bundle:
            raise ValueError("Surrogate model is not trained. Train first.")

        model = bundle.get("model")
        conf_cfg = bundle.get("confidence", {}) if isinstance(bundle.get("confidence", {}), dict) else {}
        method = str(conf_cfg.get("method", "nearest_neighbor"))
        if method == "gaussian_std":
            try:
                _, std = model.predict(X, return_std=True)
                ref = max(1e-9, _safe_float(conf_cfg.get("std_reference"), 1.0))
                conf = [max(0.0, min(1.0, 1.0 - (float(item) / ref))) for item in std]
                return list(predictions), conf
            except Exception:
                pass

        nn_model = bundle.get("nn_model")
        if nn_model is None:
            return list(predictions), [0.5 for _ in range(len(X))]

        distances, _ = nn_model.kneighbors(X)
        if distances.shape[1] > 1:
            mean_distance = distances[:, 1:].mean(axis=1)
        else:
            mean_distance = distances[:, 0]
        scale = max(1e-9, _safe_float(conf_cfg.get("distance_scale"), 1.0))
        confidence = [float(math.exp(-float(item) / scale)) for item in mean_distance]
        confidence = [max(0.0, min(1.0, item)) for item in confidence]
        return list(predictions), confidence

    @staticmethod
    def _confidence_level(value: float) -> str:
        if value >= 0.8:
            return "high"
        if value >= 0.55:
            return "medium"
        return "low"

    @staticmethod
    def _score_prediction(
        *,
        prediction: float,
        objective_goal: str,
        target_alias: str,
        constraints: list[dict[str, Any]],
    ) -> tuple[float, bool, list[str]]:
        base = float(prediction if objective_goal == "min" else -prediction)
        violations: list[str] = []
        penalty = 0.0

        for item in constraints:
            if not isinstance(item, dict):
                continue
            alias = str(item.get("alias", "")).strip()
            operator = str(item.get("operator", "<=")).strip()
            threshold = to_float(item.get("threshold"))
            if not alias or threshold is None:
                continue
            if alias != target_alias:
                violations.append(f"{alias} unavailable_in_surrogate")
                penalty += 1e6
                continue
            if not _operator_holds(float(prediction), operator, float(threshold)):
                violations.append(f"{alias} {operator} {threshold} violated")
                penalty += 1e6

        return base + penalty, len(violations) == 0, violations
    @staticmethod
    def _sample_search_space(
        *,
        search_space: list[dict[str, Any]],
        sample_count: int,
        fixed_values: dict[str, Any],
        seed: int,
    ) -> list[dict[str, Any]]:
        if not isinstance(search_space, list) or not search_space:
            raise ValueError("search_space must be a non-empty list.")
        rng = random.Random(seed)
        rows: list[dict[str, Any]] = []

        for idx in range(sample_count):
            row: dict[str, Any] = {"case_id": f"PRED_{idx + 1:05d}"}
            for dim in search_space:
                if not isinstance(dim, dict):
                    raise ValueError("Each search_space item must be an object.")
                name = str(dim.get("name", "")).strip()
                kind = str(dim.get("type", "real")).strip().lower()
                if not name:
                    raise ValueError("Each search_space item requires 'name'.")
                if kind in {"real", "float"}:
                    low = to_float(dim.get("min"))
                    high = to_float(dim.get("max"))
                    if low is None or high is None or high <= low:
                        raise ValueError(f"Invalid real bounds for {name}.")
                    row[name] = round(rng.uniform(float(low), float(high)), 6)
                elif kind in {"int", "integer"}:
                    low = _safe_int(dim.get("min"), 0)
                    high = _safe_int(dim.get("max"), 0)
                    if high < low:
                        raise ValueError(f"Invalid integer bounds for {name}.")
                    row[name] = rng.randint(low, high)
                elif kind in {"categorical", "category"}:
                    choices = dim.get("choices", [])
                    if not isinstance(choices, list) or not choices:
                        raise ValueError(f"Categorical dimension {name} requires non-empty 'choices'.")
                    row[name] = choices[rng.randrange(len(choices))]
                elif kind in {"bool", "boolean"}:
                    row[name] = bool(rng.randint(0, 1))
                else:
                    raise ValueError(f"Unsupported search_space type for {name}: {kind}")
            for key, value in fixed_values.items():
                row[str(key)] = value
            rows.append(row)

        return rows

    def predict_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        objective_alias: str | None = None,
        objective_goal: str = "min",
        constraints: list[dict[str, Any]] | None = None,
        top_n: int = 20,
    ) -> dict[str, Any]:
        bundle = self._load_bundle()
        if not bundle:
            raise ValueError("Surrogate model is not trained. Train first.")

        objective_goal = "max" if str(objective_goal).strip().lower() == "max" else "min"
        constraints = constraints if isinstance(constraints, list) else []

        target_alias = str(objective_alias or bundle.get("target_alias", "")).strip()
        if target_alias != str(bundle.get("target_alias", "")).strip():
            raise ValueError(
                f"This surrogate currently predicts '{bundle.get('target_alias')}'. Retrain for '{target_alias}' if needed."
            )

        X, row_meta, warnings = self._encode_input_rows(rows)
        if X.empty:
            return {
                "objective_alias": target_alias,
                "objective_goal": objective_goal,
                "rows_evaluated": 0,
                "top_candidates": [],
                "warnings": warnings,
            }

        model = bundle.get("model")
        predictions_raw = model.predict(X)
        predictions, confidence = self._confidence_from_predictions(X, predictions_raw)

        candidates: list[dict[str, Any]] = []
        for idx, (meta, pred_value, conf_value) in enumerate(zip(row_meta, predictions, confidence), start=1):
            score, constraints_pass, violations = self._score_prediction(
                prediction=float(pred_value),
                objective_goal=objective_goal,
                target_alias=target_alias,
                constraints=constraints,
            )
            params = {k: v for k, v in rows[idx - 1].items() if k != "case_id"}
            candidates.append(
                {
                    "case_id": str(meta.get("case_id", f"PRED_{idx:05d}")),
                    "params": params,
                    "prediction": float(pred_value),
                    "predicted_metrics": {target_alias: float(pred_value)},
                    "confidence": float(conf_value),
                    "confidence_level": self._confidence_level(float(conf_value)),
                    "constraints_pass": constraints_pass,
                    "constraint_violations": violations,
                    "score": float(score),
                }
            )

        ranked = sorted(candidates, key=lambda item: float(item.get("score", 1e30)))
        top_n = max(1, min(int(top_n), len(ranked)))
        top = ranked[:top_n]
        for rank, item in enumerate(top, start=1):
            item["rank"] = rank

        low_confidence_count = sum(1 for item in ranked if item.get("confidence", 0.0) < 0.55)
        return {
            "objective_alias": target_alias,
            "objective_goal": objective_goal,
            "rows_evaluated": len(rows),
            "model_name": str(bundle.get("model_name", "")),
            "row_count": int(bundle.get("row_count", 0)),
            "best_r2": float(bundle.get("best_r2", 0.0)),
            "top_candidates": top,
            "low_confidence_count": int(low_confidence_count),
            "warnings": warnings,
        }

    def predict_mode(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a JSON object.")

        rows = payload.get("rows") if isinstance(payload.get("rows"), list) else None
        fixed_values = payload.get("fixed_values", {}) if isinstance(payload.get("fixed_values", {}), dict) else {}
        search_space = payload.get("search_space") if isinstance(payload.get("search_space"), list) else None
        sample_count = max(1, min(_safe_int(payload.get("sample_count"), 1000), 100000))
        top_n = max(1, _safe_int(payload.get("top_n"), 25))
        seed = _safe_int(payload.get("random_seed"), 42)

        if rows is None:
            if not search_space:
                raise ValueError("Provide either 'rows' or 'search_space' for predict mode.")
            rows = self._sample_search_space(
                search_space=search_space,
                sample_count=sample_count,
                fixed_values=fixed_values,
                seed=seed,
            )

        result = self.predict_rows(
            rows=rows,
            objective_alias=str(payload.get("objective_alias", "")).strip() or None,
            objective_goal=str(payload.get("objective_goal", "min")),
            constraints=payload.get("constraints") if isinstance(payload.get("constraints"), list) else [],
            top_n=top_n,
        )
        result["sample_count"] = len(rows)
        result["generated_at"] = now_utc_stamp()
        return result

    def validate_mode(self, payload: dict[str, Any], progress: ProgressFn | None = None) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("Payload must be a JSON object.")

        predict_payload = dict(payload)
        top_n = max(1, _safe_int(payload.get("validate_top_n"), _safe_int(payload.get("top_n"), 3)))
        auto_retrain = bool(payload.get("auto_retrain", True))
        retrain_min_rows = max(5, _safe_int(payload.get("retrain_min_rows"), 50))

        candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else None
        if candidates:
            prediction_result = self.predict_rows(
                rows=candidates,
                objective_alias=str(payload.get("objective_alias", "")).strip() or None,
                objective_goal=str(payload.get("objective_goal", "min")),
                constraints=payload.get("constraints") if isinstance(payload.get("constraints"), list) else [],
                top_n=top_n,
            )
        else:
            prediction_result = self.predict_mode(predict_payload)

        chosen = list(prediction_result.get("top_candidates", []))[:top_n]
        if not chosen:
            raise ValueError("No candidates available to validate.")

        validation_rows: list[dict[str, Any]] = []
        prediction_by_case: dict[str, dict[str, Any]] = {}
        for idx, item in enumerate(chosen, start=1):
            case_id = f"SURR_VAL_{idx:03d}"
            params = item.get("params", {}) if isinstance(item.get("params", {}), dict) else {}
            row = {"case_id": case_id}
            row.update(params)
            validation_rows.append(row)
            prediction_by_case[case_id] = item

        original_cases_csv = self.runner.get_cases_csv()
        self.runner.save_cases_csv(cases_to_csv(validation_rows))
        _emit(
            progress,
            type="case_log",
            case_id="surrogate",
            attempt=1,
            source="surrogate",
            line=f"Validate mode: running {len(validation_rows)} candidate(s) with real CFD.",
        )

        try:
            run_summary = self.runner.run(mode="all", progress=progress)
        finally:
            self.runner.save_cases_csv(original_cases_csv)

        target_alias = str(prediction_result.get("objective_alias", "")).strip()
        validation_table: list[dict[str, Any]] = []
        for case_result in run_summary.get("case_results", []):
            if not isinstance(case_result, dict):
                continue
            case_id = str(case_result.get("case_id", "")).strip()
            if case_id not in prediction_by_case:
                continue
            pred = prediction_by_case[case_id]
            predicted_value = to_float(pred.get("prediction"))
            actual_value = None
            metrics = case_result.get("metrics", {}) if isinstance(case_result.get("metrics", {}), dict) else {}
            if target_alias:
                actual_value = to_float(metrics.get(target_alias))
            abs_error = None
            if predicted_value is not None and actual_value is not None:
                abs_error = abs(float(actual_value) - float(predicted_value))
            validation_table.append(
                {
                    "case_id": case_id,
                    "success": bool(case_result.get("success")),
                    "predicted": predicted_value,
                    "actual": actual_value,
                    "abs_error": abs_error,
                    "confidence": pred.get("confidence", 0.0),
                    "confidence_level": pred.get("confidence_level", "low"),
                    "failure_type": case_result.get("failure_type", ""),
                    "failure_reason": case_result.get("failure_reason", case_result.get("error", "")),
                }
            )

        retrain_result = None
        retrain_error = ""
        if auto_retrain:
            try:
                retrain_result = self.train(
                    objective_alias=target_alias or None,
                    include_design_loops=True,
                    min_rows=retrain_min_rows,
                )
            except Exception as ex:
                retrain_error = str(ex)

        output = {
            "mode": "validate",
            "validated_count": len(validation_rows),
            "objective_alias": target_alias,
            "prediction": {
                "model_name": prediction_result.get("model_name", ""),
                "best_r2": prediction_result.get("best_r2", 0.0),
                "top_candidates": chosen,
            },
            "run_summary": run_summary,
            "validation_table": validation_table,
            "auto_retrained": auto_retrain,
            "retrain": retrain_result,
            "retrain_error": retrain_error,
        }
        return output
