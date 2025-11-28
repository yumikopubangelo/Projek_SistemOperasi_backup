"""
MLService - Robust wrapper around Elasticsearch ML queries for AquaGuard

Perbaikan dan fitur:
- Toleran terhadap variasi bentuk response Elasticsearch (hits.total dict vs int).
- Konversi timestamp epoch (ms / s) -> ISO8601 UTC (Z-suffixed) for frontend consistency.
- Request timeouts and ignore_unavailable for ES queries.
- Defensive programming & logging and compatibility with wrapper client objects.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)


def _ms_to_iso(ms_value: Union[int, str, None]) -> Optional[str]:
    """Convert epoch milliseconds or seconds to ISO8601 (UTC, Z suffix).
    Return None if conversion fails or value is falsy.
    Heuristic:
      - > 1e12 -> treat as milliseconds
      - > 1e9  -> treat as seconds
    """
    if ms_value is None or ms_value == "":
        return None
    try:
        ms = int(ms_value)
        # heuristics to decide ms vs s
        if ms > 1_000_000_000_000:
            dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        elif ms > 1_000_000_000:
            dt = datetime.fromtimestamp(ms, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(ms, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        try:
            # maybe it's already an ISO string
            parsed = datetime.fromisoformat(str(ms_value).rstrip("Z"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None


class MLService:
    """
    Service untuk Machine Learning anomaly detection (Elasticsearch ML).
    Defensive terhadap variasi ES client/wrapper and ES responses.
    """

    def __init__(self, es_client, config):
        """
        es_client : either an elasticsearch.Elasticsearch instance OR a wrapper that
                    exposes `.client` property returning a real client.
        config    : modul / objek Config (may have ML_JOB_IDS)
        """
        # Defensive unwrap: if a wrapper object (ElasticsearchClient), unwrap underlying .client
        try:
            if hasattr(es_client, "client"):
                # prefer the underlying raw client
                self.es = getattr(es_client, "client")
                logger.debug("[ML SERVICE] Unwrapped es_client.client for direct ES API access")
            else:
                self.es = es_client
        except Exception:
            # fallback to the provided object
            self.es = es_client

        self.config = config

        # Support multiple ML jobs
        self.job_ids: List[str] = getattr(config, "ML_JOB_IDS", ["prediksi_tds_jenuh", "anomali_kekeruhan"])
        if not isinstance(self.job_ids, list):
            self.job_ids = [self.job_ids]

        self.default_job_id = self.job_ids[0] if self.job_ids else "prediksi_tds_jenuh"

        logger.info(f"[ML SERVICE] Initialized with jobs: {', '.join(self.job_ids)}")

    # --------- internal helpers ----------
    def _es_search(self, index: str, body: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Compatibility wrapper for different ES client signatures.
        Preferred call: client.search(index=index, body=body, **kwargs)
        Fallbacks try other common signatures if TypeError occurs.
        """
        try:
            return self.es.search(index=index, body=body, **kwargs)
        except TypeError:
            # Some clients (or versions) accept 'query' keyword instead of 'body'
            try:
                return self.es.search(index=index, query=body.get("query"), size=body.get("size"), **kwargs)
            except Exception:
                # last resort: call positional (body, index) for some wrappers
                try:
                    return self.es.search(body, index=index, **kwargs)
                except Exception:
                    raise
        except Exception:
            raise

    # ===================== ANOMALIES LIST =====================
    def get_anomalies(self, size: int = 50, min_score: float = 0, hours_back: int = 24, job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Ambil daftar anomaly dari index .ml-anomalies-*.
        """
        try:
            end_time = datetime.utcnow().replace(tzinfo=timezone.utc)
            start_time = end_time - timedelta(hours=hours_back)

            # Build query filters
            time_filter = {
                "range": {
                    "timestamp": {
                        "gte": start_time.isoformat(),
                        "lte": end_time.isoformat(),
                    }
                }
            }

            filters = [time_filter]

            # Filter by specific job or all configured jobs
            if job_id:
                filters.append({"term": {"job_id": job_id}})
            else:
                if self.job_ids:
                    filters.append({"terms": {"job_id": self.job_ids}})

            if min_score and min_score > 0:
                filters.append({"range": {"record_score": {"gte": min_score}}})

            query = {
                "size": size,
                "query": {
                    "bool": {
                        "filter": filters
                    }
                },
                "sort": [
                    {"record_score": {"order": "desc"}},
                    {"timestamp": {"order": "desc"}}
                ],
            }

            logger.info(f"[ML ANOMALIES] Querying ES (size={size}, min_score={min_score}, hours_back={hours_back}, job_id={job_id})")
            logger.debug(f"[ML ANOMALIES] Query body: {query}")

            result = self._es_search(index=".ml-anomalies-*", body=query, request_timeout=5, ignore_unavailable=True)

            # Normalize total hits across ES versions
            total_hits = 0
            try:
                hits_total = result.get("hits", {}).get("total", 0)
                if isinstance(hits_total, dict):
                    total_hits = hits_total.get("value", 0)
                else:
                    total_hits = int(hits_total)
            except Exception:
                total_hits = 0

            hits = result.get("hits", {}).get("hits", []) or []
            anomalies: List[Dict[str, Any]] = []

            for hit in hits:
                src = hit.get("_source", {}) or {}

                # prefer record_score, fallback to anomaly_score
                score = src.get("record_score")
                if score is None:
                    score = src.get("anomaly_score", 0)

                # normalize timestamp to ISO if possible
                ts = src.get("timestamp") or src.get("@timestamp") or src.get("time") or src.get("ts")
                ts_iso = _ms_to_iso(ts) or ts

                anomalies.append({
                    "timestamp": ts_iso,
                    "job_id": src.get("job_id"),
                    "record_score": score,
                    "typical": src.get("typical", []),
                    "actual": src.get("actual", []),
                    "function": src.get("function"),
                    "field_name": src.get("field_name"),
                    "by_field_name": src.get("by_field_name"),
                    "by_field_value": src.get("by_field_value"),
                    "detector_index": src.get("detector_index"),
                    "is_interim": src.get("is_interim", False),
                    "doc_id": hit.get("_id")
                })

            logger.info(f"[ML ANOMALIES] Returning {len(anomalies)} anomalies (total_hits={total_hits})")

            return {
                "status": "success",
                "total": total_hits,
                "returned": len(anomalies),
                "anomalies": anomalies,
                "query_params": {
                    "size": size,
                    "min_score": min_score,
                    "hours_back": hours_back,
                    "job_id": job_id or "all",
                    "job_ids": self.job_ids,
                },
            }

        except Exception as e:
            logger.exception("[ML ANOMALIES ERROR] Exception while fetching anomalies")
            return {
                "status": "error",
                "message": str(e),
                "anomalies": [],
                "total": 0,
                "returned": 0,
            }

    # ===================== STATUS (Multi-Job) =====================
    def get_status(self) -> Dict[str, Any]:
        try:
            all_jobs_status: List[Dict[str, Any]] = []
            overall_status = "NORMAL"
            total_processed = 0
            total_anomalies_24h = 0
            critical_anomalies_24h = 0

            for job_id in self.job_ids:
                try:
                    logger.info(f"[ML STATUS] Checking job: {job_id}")

                    # Prefer narrow API call; fallback to listing jobs
                    try:
                        job_result = self.es.ml.get_jobs(job_id=job_id, request_timeout=5)
                    except Exception as ex_get:
                        logger.warning(f"[ML STATUS] get_jobs(job_id=...) failed for {job_id}: {ex_get}. Trying to list all jobs.")
                        job_list = self.es.ml.get_jobs(request_timeout=5)
                        job_result = job_list

                    jobs = job_result.get("jobs") or job_result.get("jobs", []) or []
                    if isinstance(jobs, dict):
                        jobs = [jobs]

                    if not jobs:
                        logger.warning(f"[ML STATUS] Job {job_id} not found in ES response")
                        all_jobs_status.append({
                            "job_id": job_id,
                            "status": "NOT_FOUND",
                            "message": "Job not found in Elasticsearch"
                        })
                        overall_status = "WARNING"
                        continue

                    job = jobs[0]
                    job_state = job.get("state", job.get("job_state", "unknown"))

                    # Get job stats (defensive)
                    try:
                        stats_resp = self.es.ml.get_job_stats(job_id=job_id, request_timeout=5)
                        job_stats = (stats_resp.get("jobs") or [{}])[0]
                        state = job_stats.get("state", job_state)
                        data_counts = job_stats.get("data_counts", {}) or {}
                        processed = int(data_counts.get("processed_record_count", 0) or 0)
                    except Exception as ex_stats:
                        logger.warning(f"[ML STATUS] get_job_stats failed for {job_id}: {ex_stats}")
                        state = job_state
                        processed = 0

                    total_processed += processed

                    state_lower = str(state).lower()
                    if state_lower in ["opened", "open", "started", "running"]:
                        job_status = "RUNNING"
                    elif state_lower in ["closed", "stopped"]:
                        job_status = "STOPPED"
                        if overall_status == "NORMAL":
                            overall_status = "WARNING"
                    else:
                        job_status = f"UNKNOWN ({state})"
                        overall_status = "WARNING"

                    all_jobs_status.append({
                        "job_id": job_id,
                        "status": job_status,
                        "state": state,
                        "processed_records": processed
                    })

                except Exception as job_error:
                    logger.exception(f"[ML STATUS] Error checking job {job_id}")
                    all_jobs_status.append({
                        "job_id": job_id,
                        "status": "ERROR",
                        "error": str(job_error),
                        "message": f"Failed to check job: {str(job_error)}"
                    })
                    overall_status = "WARNING"

            # Get anomaly summary (best-effort)
            try:
                summary = self.get_summary(hours_back=24)
                if isinstance(summary, dict):
                    total_anomalies_24h = int(summary.get("total_anomalies", 0) or 0)
                    critical_anomalies_24h = int(summary.get("severity_breakdown", {}).get("critical", 0) or 0)
                    if critical_anomalies_24h > 10:
                        overall_status = "CRITICAL"
                    elif critical_anomalies_24h > 5 and overall_status != "CRITICAL":
                        overall_status = "WARNING"
            except Exception as summary_error:
                logger.warning(f"[ML STATUS] Error obtaining summary: {summary_error}")

            running_count = sum(1 for j in all_jobs_status if j.get("status") == "RUNNING")
            if running_count == len(self.job_ids):
                message = f"All {len(self.job_ids)} ML jobs running. {total_processed} records analyzed."
            else:
                message = f"{running_count}/{len(self.job_ids)} jobs running. Check individual job status."

            return {
                "status": overall_status,
                "message": message,
                "jobs": all_jobs_status,
                "total_processed_records": total_processed,
                "total_anomalies_24h": total_anomalies_24h,
                "critical_anomalies_24h": critical_anomalies_24h,
                "last_updated": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
            }

        except Exception as e:
            logger.exception("[ML STATUS ERROR] Unexpected failure")
            return {
                "status": "ERROR",
                "message": "ML service unavailable or not configured",
                "error": str(e),
                "jobs": [],
            }

    # ===================== SUMMARY (Multi-Job) =====================
    def get_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        try:
            end_time = datetime.utcnow().replace(tzinfo=timezone.utc)
            start_time = end_time - timedelta(hours=hours_back)

            query = {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"terms": {"job_id": self.job_ids}},
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": start_time.isoformat(),
                                        "lte": end_time.isoformat(),
                                    }
                                }
                            },
                        ]
                    }
                },
                "aggs": {
                    "total_anomalies": {"value_count": {"field": "record_score"}},
                    "by_job": {
                        "terms": {"field": "job_id", "size": 10},
                        "aggs": {
                            "avg_score": {"avg": {"field": "record_score"}},
                            "max_score": {"max": {"field": "record_score"}}
                        }
                    },
                    "severity_low": {"filter": {"range": {"record_score": {"gte": 0, "lt": 25}}}},
                    "severity_medium": {"filter": {"range": {"record_score": {"gte": 25, "lt": 50}}}},
                    "severity_high": {"filter": {"range": {"record_score": {"gte": 50, "lt": 75}}}},
                    "severity_critical": {"filter": {"range": {"record_score": {"gte": 75}}}},
                    "timeline": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}},
                    "avg_score": {"avg": {"field": "record_score"}},
                    "max_score": {"max": {"field": "record_score"}},
                }
            }

            result = self._es_search(index=".ml-anomalies-*", body=query, request_timeout=10, ignore_unavailable=True)
            aggs = result.get("aggregations", {}) or {}

            # Process job breakdown
            jobs_breakdown: List[Dict[str, Any]] = []
            for bucket in aggs.get("by_job", {}).get("buckets", []):
                jobs_breakdown.append({
                    "job_id": bucket.get("key"),
                    "count": bucket.get("doc_count", 0),
                    "avg_score": round(bucket.get("avg_score", {}).get("value", 0) or 0, 2),
                    "max_score": round(bucket.get("max_score", {}).get("value", 0) or 0, 2),
                })

            # Process timeline
            timeline: List[Dict[str, Any]] = []
            for bucket in aggs.get("timeline", {}).get("buckets", []):
                ts = bucket.get("key_as_string") or _ms_to_iso(bucket.get("key"))
                timeline.append({
                    "timestamp": ts,
                    "count": bucket.get("doc_count", 0)
                })

            total_anomalies = int(aggs.get("total_anomalies", {}).get("value", 0) or 0)

            return {
                "status": "success",
                "hours_back": hours_back,
                "total_anomalies": total_anomalies,
                "severity_breakdown": {
                    "low": int(aggs.get("severity_low", {}).get("doc_count", 0) or 0),
                    "medium": int(aggs.get("severity_medium", {}).get("doc_count", 0) or 0),
                    "high": int(aggs.get("severity_high", {}).get("doc_count", 0) or 0),
                    "critical": int(aggs.get("severity_critical", {}).get("doc_count", 0) or 0),
                },
                "jobs": jobs_breakdown,
                "timeline": timeline,
                "avg_score": round(aggs.get("avg_score", {}).get("value", 0) or 0, 2),
                "max_score": round(aggs.get("max_score", {}).get("value", 0) or 0, 2),
                "time_range": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat(),
                },
            }

        except Exception as e:
            logger.exception("[ML SUMMARY ERROR] Exception while generating summary")
            return {
                "status": "error",
                "message": str(e),
                "total_anomalies": 0,
                "severity_breakdown": {"low": 0, "medium": 0, "high": 0, "critical": 0},
                "jobs": [],
                "timeline": [],
            }

    # ===================== HEALTH CHECK =====================
    def check_health(self) -> Dict[str, Any]:
        try:
            healthy_jobs = []
            unhealthy_jobs = []

            for job_id in self.job_ids:
                try:
                    # defensive: some wrappers will forward .ml to the underlying client
                    res = self.es.ml.get_jobs(job_id=job_id, request_timeout=3)
                    jobs = res.get("jobs") or []
                    if jobs:
                        healthy_jobs.append(job_id)
                    else:
                        unhealthy_jobs.append(job_id)
                except Exception as e:
                    logger.warning(f"[ML HEALTH] Job {job_id} check failed: {e}")
                    unhealthy_jobs.append(job_id)

            all_healthy = len(unhealthy_jobs) == 0
            return {
                "healthy": all_healthy,
                "total_jobs": len(self.job_ids),
                "healthy_jobs": healthy_jobs,
                "unhealthy_jobs": unhealthy_jobs,
            }

        except Exception as e:
            logger.exception("[ML HEALTH CHECK ERROR] Unexpected failure")
            return {
                "healthy": False,
                "error": str(e),
                "total_jobs": len(self.job_ids),
            }

    # ===================== INDIVIDUAL JOB STATUS =====================
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        try:
            if job_id not in self.job_ids:
                return {
                    "status": "error",
                    "message": f"Job ID '{job_id}' not configured. Available: {', '.join(self.job_ids)}"
                }

            result = self.es.ml.get_jobs(job_id=job_id, request_timeout=5)
            jobs = result.get("jobs") or []
            if not jobs:
                return {
                    "status": "NOT_FOUND",
                    "message": f"ML job '{job_id}' not found",
                    "job_id": job_id,
                }

            job = jobs[0]
            job_state = job.get("state", job.get("job_state", "unknown"))

            stats_resp = self.es.ml.get_job_stats(job_id=job_id, request_timeout=5)
            job_stats = (stats_resp.get("jobs") or [{}])[0]
            data_counts = job_stats.get("data_counts") or {}

            latest_ts = data_counts.get("latest_record_timestamp")
            last_data_time_iso = _ms_to_iso(latest_ts) or latest_ts

            return {
                "status": "success",
                "job_id": job_id,
                "state": job_state,
                "processed_records": int(data_counts.get("processed_record_count", 0) or 0),
                "last_data_time": last_data_time_iso,
            }

        except Exception as e:
            logger.exception(f"[ML JOB STATUS ERROR] {job_id}")
            return {
                "status": "error",
                "message": str(e),
                "job_id": job_id,
            }
