"""Queue Manager - Processing Queue Management"""

import json
import os
import logging
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    id: str
    filename: str
    input_path: str
    output_path: str
    status: JobStatus
    priority: int = 2
    created_at: str = ""
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    chunks_total: int = 0
    chunks_processed: int = 0
    cost_usd: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: Dict) -> "Job":
        d["status"] = JobStatus(d["status"])
        return cls(**d)


class QueueManager:
    def __init__(self, status_file: str = "logs/queue_status.json"):
        self.status_file = Path(status_file)
        self.jobs: Dict[str, Job] = {}
        self._load_state()

    def _load_state(self):
        if self.status_file.exists():
            try:
                with open(self.status_file, "r") as f:
                    data = json.load(f)
                for job_data in data.get("jobs", []):
                    job = Job.from_dict(job_data)
                    self.jobs[job.id] = job
            except Exception as e:
                logger.warning(f"Failed to load queue state: {e}")

    def _save_state(self):
        self.status_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "updated_at": datetime.now().isoformat(),
            "jobs": [j.to_dict() for j in self.jobs.values()],
            "stats": self.get_stats()
        }
        with open(self.status_file, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    def add_job(self, filename: str, input_path: str, output_path: str, priority: int = 2) -> Job:
        job_id = f"{datetime.now().strftime("%Y%m%d%H%M%S")}_{filename}"
        job = Job(
            id=job_id, filename=filename, input_path=input_path,
            output_path=output_path, status=JobStatus.PENDING,
            priority=priority, created_at=datetime.now().isoformat()
        )
        self.jobs[job_id] = job
        self._save_state()
        logger.info(f"Added job: {job_id}")
        return job

    def get_next_job(self) -> Optional[Job]:
        pending = [j for j in self.jobs.values() if j.status == JobStatus.PENDING]
        if not pending:
            return None
        pending.sort(key=lambda x: (x.priority, x.created_at))
        return pending[0]

    def start_job(self, job_id: str):
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.IN_PROGRESS
            self.jobs[job_id].started_at = datetime.now().isoformat()
            self._save_state()

    def complete_job(self, job_id: str, cost_usd: float = 0.0):
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.COMPLETED
            self.jobs[job_id].completed_at = datetime.now().isoformat()
            self.jobs[job_id].cost_usd = cost_usd
            self._save_state()
            logger.info(f"Completed job: {job_id}")

    def fail_job(self, job_id: str, error: str):
        if job_id in self.jobs:
            self.jobs[job_id].status = JobStatus.FAILED
            self.jobs[job_id].error_message = error
            self.jobs[job_id].completed_at = datetime.now().isoformat()
            self._save_state()
            logger.error(f"Failed job: {job_id} - {error}")

    def update_progress(self, job_id: str, chunks_processed: int, chunks_total: int):
        if job_id in self.jobs:
            self.jobs[job_id].chunks_processed = chunks_processed
            self.jobs[job_id].chunks_total = chunks_total
            self._save_state()

    def get_stats(self) -> Dict[str, Any]:
        statuses = {}
        total_cost = 0.0
        for job in self.jobs.values():
            status = job.status.value
            statuses[status] = statuses.get(status, 0) + 1
            total_cost += job.cost_usd
        return {"by_status": statuses, "total_jobs": len(self.jobs), "total_cost_usd": total_cost}

    def scan_input_dir(self, input_dir: str, output_dir: str) -> List[Job]:
        new_jobs = []
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        existing = {j.filename for j in self.jobs.values()}
        for pdf in input_path.glob("*.pdf"):
            if pdf.name not in existing:
                out_file = output_path / pdf.name.replace(".pdf", ".md")
                job = self.add_job(pdf.name, str(pdf), str(out_file))
                new_jobs.append(job)
        return new_jobs


def create_queue_manager_from_config(config: Dict) -> QueueManager:
    queue_config = config.get("queue", {})
    status_file = queue_config.get("status_file", "logs/queue_status.json")
    return QueueManager(status_file)
