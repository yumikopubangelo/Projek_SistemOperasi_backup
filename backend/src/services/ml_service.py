import logging
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


class MLService:
    """
    Service untuk Machine Learning anomaly detection
    Menggunakan Elasticsearch ML Jobs - Support Multiple Jobs
    """

    def __init__(self, es_client: Elasticsearch, config):
        """
        es_client : instance Elasticsearch
        config    : modul / objek Config (punya ML_JOB_IDS)
        """
        self.es = es_client
        self.config = config
        
        # Support multiple ML jobs
        self.job_ids = getattr(config, "ML_JOB_IDS", ["prediksi_tds_jenuh", "anomali_kekeruhan"])
        
        # Default job untuk backward compatibility
        self.default_job_id = self.job_ids[0] if self.job_ids else "prediksi_tds_jenuh"
        
        logger.info(f"[ML SERVICE] Initialized with jobs: {', '.join(self.job_ids)}")

    # ===================== ANOMALIES LIST =====================

    def get_anomalies(self, size=50, min_score=0, hours_back=24, job_id=None):
        """
        Ambil daftar anomaly dari index .ml-anomalies-*
        
        Args:
            size       : jumlah anomaly dikembalikan
            min_score  : minimum record_score (bukan anomaly_score)
            hours_back : rentang waktu ke belakang
            job_id     : specific job ID, jika None maka ambil dari semua jobs
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)

            # Build query filters
            filters = [
                {
                    "range": {
                        "timestamp": {
                            "gte": start_time.isoformat(),
                            "lte": end_time.isoformat(),
                        }
                    }
                }
            ]
            
            # Filter by specific job or all configured jobs
            if job_id:
                filters.append({"term": {"job_id": job_id}})
            else:
                # Include all configured jobs
                filters.append({"terms": {"job_id": self.job_ids}})
            
            # Add score filter if min_score > 0
            if min_score > 0:
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

            logger.info(f"[ML ANOMALIES] Querying with: size={size}, min_score={min_score}, hours_back={hours_back}, job_id={job_id}")
            logger.debug(f"[ML ANOMALIES] Query: {query}")

            result = self.es.search(
                index=".ml-anomalies-*", 
                body=query, 
                request_timeout=10,
                ignore_unavailable=True  # Ignore if index doesn't exist
            )

            total_hits = result.get("hits", {}).get("total", {})
            if isinstance(total_hits, dict):
                total_count = total_hits.get("value", 0)
            else:
                total_count = total_hits

            logger.info(f"[ML ANOMALIES] Found {total_count} total anomalies")

            hits = result.get("hits", {}).get("hits", [])
            
            # Transform anomalies for better frontend consumption
            anomalies = []
            for hit in hits:
                source = hit["_source"]
                
                # Handle both record_score and anomaly_score
                score = source.get("record_score") or source.get("anomaly_score", 0)
                
                anomalies.append({
                    "timestamp": source.get("timestamp"),
                    "job_id": source.get("job_id"),
                    "record_score": score,
                    "typical": source.get("typical", []),
                    "actual": source.get("actual", []),
                    "function": source.get("function"),
                    "field_name": source.get("field_name"),
                    "by_field_name": source.get("by_field_name"),
                    "by_field_value": source.get("by_field_value"),
                    "detector_index": source.get("detector_index"),
                    "is_interim": source.get("is_interim", False),
                })

            logger.info(f"[ML ANOMALIES] Returning {len(anomalies)} anomalies")

            return {
                "status": "success",
                "total": total_count,
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
            logger.error(f"[ML ANOMALIES ERROR] {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "anomalies": [],
                "total": 0,
                "returned": 0,
            }

    # ===================== STATUS (Multi-Job) =====================

    def get_status(self):
        """
        Get ML job status untuk semua configured jobs
        Returns aggregated status dari semua jobs
        """
        try:
            all_jobs_status = []
            overall_status = "NORMAL"
            total_processed = 0
            total_anomalies_24h = 0
            critical_anomalies_24h = 0
            
            for job_id in self.job_ids:
                try:
                    # Check job existence and state
                    logger.info(f"[ML STATUS] Checking job: {job_id}")
                    
                    # Try different API methods
                    try:
                        job_result = self.es.ml.get_jobs(job_id=job_id, request_timeout=5)
                    except Exception as e:
                        # Fallback: try getting all jobs and filter
                        logger.warning(f"[ML STATUS] Direct get failed for {job_id}, trying get_jobs(): {e}")
                        job_result = self.es.ml.get_jobs(request_timeout=5)
                        # Filter for our job
                        filtered_jobs = [j for j in job_result.get("jobs", []) if j.get("job_id") == job_id]
                        job_result = {"jobs": filtered_jobs, "count": len(filtered_jobs)}
                    
                    logger.info(f"[ML STATUS] Job result for {job_id}: {job_result.get('count', 0)} jobs found")
                    
                    if not job_result.get("jobs") or len(job_result.get("jobs", [])) == 0:
                        logger.warning(f"[ML STATUS] Job {job_id} not found in response")
                        all_jobs_status.append({
                            "job_id": job_id,
                            "status": "NOT_FOUND",
                            "message": f"Job not found in Elasticsearch"
                        })
                        overall_status = "WARNING"
                        continue
                    
                    job = job_result["jobs"][0]
                    job_state = job.get("state", "unknown")
                    
                    logger.info(f"[ML STATUS] Job {job_id} state: {job_state}")
                    
                    # Get job stats
                    try:
                        stats = self.es.ml.get_job_stats(job_id=job_id, request_timeout=5)
                        job_stats = stats.get("jobs", [{}])[0]
                        state = job_stats.get("state", job_state)  # Get state from stats as fallback
                        data_counts = job_stats.get("data_counts", {})
                        processed = data_counts.get("processed_record_count", 0)
                        
                        logger.info(f"[ML STATUS] Job {job_id} stats - state: {state}, processed: {processed}")
                    except Exception as stats_error:
                        logger.warning(f"[ML STATUS] Failed to get stats for {job_id}: {stats_error}")
                        state = job_state
                        processed = 0
                    
                    total_processed += processed
                    
                    # Determine job status - be more flexible with state checking
                    state_lower = str(state).lower()
                    if state_lower in ["opened", "open", "started"]:
                        job_status = "RUNNING"
                    elif state_lower in ["closed", "stopped"]:
                        job_status = "STOPPED"
                        if overall_status == "NORMAL":
                            overall_status = "WARNING"
                    else:
                        logger.warning(f"[ML STATUS] Unknown state '{state}' for job {job_id}")
                        job_status = f"UNKNOWN ({state})"
                        overall_status = "WARNING"
                    
                    all_jobs_status.append({
                        "job_id": job_id,
                        "status": job_status,
                        "state": state,
                        "processed_records": processed
                    })
                    
                except Exception as job_error:
                    logger.error(f"[ML STATUS] Error checking job {job_id}: {job_error}", exc_info=True)
                    all_jobs_status.append({
                        "job_id": job_id,
                        "status": "ERROR",
                        "error": str(job_error),
                        "message": f"Failed to check job: {str(job_error)}"
                    })
                    overall_status = "WARNING"
            
            # Get anomaly summary for all jobs
            try:
                summary = self.get_summary(hours_back=24)
                total_anomalies_24h = summary.get("total_anomalies", 0)
                critical_anomalies_24h = summary.get("severity_breakdown", {}).get("critical", 0)
                
                # Update overall status based on anomalies
                if critical_anomalies_24h > 10:
                    overall_status = "CRITICAL"
                elif critical_anomalies_24h > 5:
                    overall_status = "WARNING"
                    
            except Exception as summary_error:
                logger.warning(f"[ML STATUS] Error getting summary: {summary_error}")
            
            # Generate message
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
                "last_updated": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"[ML STATUS ERROR] {e}", exc_info=True)
            return {
                "status": "ERROR",
                "message": "ML service unavailable or not configured",
                "error": str(e),
                "jobs": [],
            }

    # ===================== SUMMARY (Multi-Job) =====================

    def get_summary(self, hours_back=24):
        """
        Get summary statistik anomali untuk semua jobs dalam rentang waktu tertentu
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours_back)

            # Query untuk semua configured jobs
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
                    "total_anomalies": {
                        "value_count": {"field": "record_score"}
                    },
                    "by_job": {
                        "terms": {
                            "field": "job_id",
                            "size": 10
                        },
                        "aggs": {
                            "avg_score": {"avg": {"field": "record_score"}},
                            "max_score": {"max": {"field": "record_score"}},
                        }
                    },
                    "severity_low": {
                        "filter": {"range": {"record_score": {"gte": 0, "lt": 25}}}
                    },
                    "severity_medium": {
                        "filter": {"range": {"record_score": {"gte": 25, "lt": 50}}}
                    },
                    "severity_high": {
                        "filter": {"range": {"record_score": {"gte": 50, "lt": 75}}}
                    },
                    "severity_critical": {
                        "filter": {"range": {"record_score": {"gte": 75}}}
                    },
                    "timeline": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": "1h"
                        }
                    },
                    "avg_score": {"avg": {"field": "record_score"}},
                    "max_score": {"max": {"field": "record_score"}},
                }
            }

            result = self.es.search(
                index=".ml-anomalies-*", 
                body=query, 
                request_timeout=10
            )

            aggs = result.get("aggregations", {})
            
            # Process job breakdown
            jobs_breakdown = []
            for bucket in aggs.get("by_job", {}).get("buckets", []):
                jobs_breakdown.append({
                    "job_id": bucket["key"],
                    "count": bucket["doc_count"],
                    "avg_score": round(bucket.get("avg_score", {}).get("value", 0) or 0, 2),
                    "max_score": round(bucket.get("max_score", {}).get("value", 0) or 0, 2),
                })
            
            # Process timeline
            timeline = []
            for bucket in aggs.get("timeline", {}).get("buckets", []):
                timeline.append({
                    "timestamp": bucket["key_as_string"],
                    "count": bucket["doc_count"]
                })

            return {
                "status": "success",
                "hours_back": hours_back,
                "total_anomalies": aggs.get("total_anomalies", {}).get("value", 0),
                "severity_breakdown": {
                    "low": aggs.get("severity_low", {}).get("doc_count", 0),
                    "medium": aggs.get("severity_medium", {}).get("doc_count", 0),
                    "high": aggs.get("severity_high", {}).get("doc_count", 0),
                    "critical": aggs.get("severity_critical", {}).get("doc_count", 0),
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
            logger.error(f"[ML SUMMARY ERROR] {e}", exc_info=True)
            return {
                "status": "error",
                "message": str(e),
                "total_anomalies": 0,
                "severity_breakdown": {
                    "low": 0,
                    "medium": 0,
                    "high": 0,
                    "critical": 0,
                },
                "jobs": [],
                "timeline": [],
            }

    # ===================== HEALTH CHECK =====================

    def check_health(self):
        """
        Quick health check untuk semua ML jobs
        """
        try:
            healthy_jobs = []
            unhealthy_jobs = []
            
            for job_id in self.job_ids:
                try:
                    result = self.es.ml.get_jobs(job_id=job_id, request_timeout=3)
                    if result.get("jobs"):
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
            logger.error(f"[ML HEALTH CHECK ERROR] {e}")
            return {
                "healthy": False, 
                "error": str(e),
                "total_jobs": len(self.job_ids),
            }

    # ===================== INDIVIDUAL JOB STATUS =====================

    def get_job_status(self, job_id):
        """
        Get status untuk specific job
        """
        try:
            if job_id not in self.job_ids:
                return {
                    "status": "error",
                    "message": f"Job ID '{job_id}' not configured. Available: {', '.join(self.job_ids)}"
                }
            
            result = self.es.ml.get_jobs(job_id=job_id, request_timeout=5)
            
            if not result.get("jobs"):
                return {
                    "status": "NOT_FOUND",
                    "message": f"ML job '{job_id}' not found",
                    "job_id": job_id,
                }
            
            job = result["jobs"][0]
            job_state = job.get("state", "unknown")
            
            stats = self.es.ml.get_job_stats(job_id=job_id, request_timeout=5)
            job_stats = stats.get("jobs", [{}])[0]
            data_counts = job_stats.get("data_counts", {})
            
            return {
                "status": "success",
                "job_id": job_id,
                "state": job_state,
                "processed_records": data_counts.get("processed_record_count", 0),
                "last_data_time": data_counts.get("latest_record_timestamp"),
            }
            
        except Exception as e:
            logger.error(f"[ML JOB STATUS ERROR] {job_id}: {e}")
            return {
                "status": "error",
                "message": str(e),
                "job_id": job_id,
            }