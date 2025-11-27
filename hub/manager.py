"""
Worker Manager for Hub Server
Manages worker registration, job dispatch, and file transfers
"""
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import asyncio
import base64
import json
import logging
from typing import Dict, Optional
from shared.redis_client import r

logger = logging.getLogger(__name__)

# Job storage directory
JOB_DIR = Path("./jobs")
JOB_DIR.mkdir(exist_ok=True)

def get_job_folder(job_id: str) -> Path:
    """Get or create job folder"""
    folder = JOB_DIR / job_id
    folder.mkdir(exist_ok=True)
    return folder

@dataclass
class Worker:
    """Worker instance data"""
    worker_id: str
    ws: object  # WebSocket connection
    worker_url: str
    busy: bool = False
    last_heartbeat: datetime = None
    current_job: Optional[str] = None

class WorkerManager:
    """Manages all worker connections and job dispatching"""
    
    def __init__(self):
        self.workers: Dict[str, Worker] = {}
        self.file_buffers: Dict[tuple, dict] = {}  # (job_id, filename) -> buffer
    
    async def register(self, worker_id: str, ws, worker_url: str):
        """Register a new worker"""
        self.workers[worker_id] = Worker(
            worker_id=worker_id,
            ws=ws,
            worker_url=worker_url,
            last_heartbeat=datetime.now()
        )
        logger.info(f"[MANAGER] Worker {worker_id} registered")
    
    async def try_dispatch(self):
        """Try to assign a pending job to an idle worker"""
        # Find idle worker
        idle_worker = None
        for worker in self.workers.values():
            if not worker.busy:
                idle_worker = worker
                break
        
        if not idle_worker:
            logger.debug("[MANAGER] No idle workers available")
            return
        
        # Get job from queue
        job_id = r.rpop("job_queue")
        if not job_id:
            logger.debug("[MANAGER] No pending jobs in queue")
            return

# job_id is already a string due to decode_responses=True
        
        logger.info(f"[MANAGER] Dispatching job {job_id} to worker {idle_worker.worker_id}")
        
        # Mark worker as busy
        idle_worker.busy = True
        idle_worker.current_job = job_id
        
        # Update job metadata
        r.hset(f"job:{job_id}", mapping={
            "worker_id": idle_worker.worker_id,
            "status": "assigned"
        })
        
        # Get job data
        job_data = r.hgetall(f"job:{job_id}")
        
        try:
            # Send job assignment
            await idle_worker.ws.send_json({
                "type": "assign_job",
                "job_id": job_id,
                "job_type": job_data.get("job_type", "teeth_segmentation")
            })
            
            # Send input files in chunks
            await self.send_job_files(idle_worker, job_id)
            
            logger.info(f"[MANAGER] Job {job_id} dispatched successfully")
            
        except Exception as e:
            logger.error(f"[MANAGER] Failed to dispatch job {job_id}: {e}")
            # Requeue job
            r.lpush("job_queue", job_id)
            r.hset(f"job:{job_id}", "status", "pending")
            idle_worker.busy = False
            idle_worker.current_job = None
    
    async def send_job_files(self, worker: Worker, job_id: str, chunk_size: int = 1024 * 1024):
        """Send input files to worker in chunks"""
        job_folder = get_job_folder(job_id)
        
        # Files to send
        files_to_send = [
            ("lower.obj", job_folder / "lower.obj"),
            ("upper.obj", job_folder / "upper.obj")
        ]
        
        for filename, file_path in files_to_send:
            if not file_path.exists():
                logger.error(f"[MANAGER] Input file not found: {file_path}")
                continue
            
            # Read file
            with open(file_path, "rb") as f:
                data = f.read()
            
            total_size = len(data)
            total_chunks = (total_size + chunk_size - 1) // chunk_size
            
            logger.info(f"[MANAGER] Sending {filename} ({total_size} bytes) in {total_chunks} chunks")
            
            # Send chunks
            for i in range(total_chunks):
                start = i * chunk_size
                end = min(start + chunk_size, total_size)
                chunk = data[start:end]
                
                # Encode chunk as base64
                chunk_b64 = base64.b64encode(chunk).decode('utf-8')
                
                await worker.ws.send_json({
                    "type": "file_chunk",
                    "job_id": job_id,
                    "filename": filename,
                    "data": chunk_b64,
                    "chunk_index": i,
                    "total_chunks": total_chunks
                })
                
                # Small delay to avoid overwhelming connection
                if i % 10 == 0:
                    await asyncio.sleep(0.01)
            
            logger.info(f"[MANAGER] File {filename} sent successfully")
    
    async def fetch_job_results(self, worker_id: str, job_id: str):
        """Request output files from worker"""
        worker = self.workers.get(worker_id)
        if not worker:
            logger.error(f"[MANAGER] Worker {worker_id} not found")
            return
        
        # Output files we expect
        output_files = [
            "input_lower.obj",
            "input_upper.obj",
            "input_lower.json",
            "input_upper.json"
        ]
        
        logger.info(f"[MANAGER] Requesting {len(output_files)} output files from worker {worker_id}")
        
        await worker.ws.send_json({
            "type": "request_outputs",
            "job_id": job_id,
            "files": output_files
        })
    
    async def receive_file_chunk(self, job_id: str, filename: str, chunk_data: str, 
                                 chunk_index: int, total_chunks: int):
        """Receive and assemble file chunks from worker"""
        key = (job_id, filename)
        
        # Initialize buffer
        if key not in self.file_buffers:
            self.file_buffers[key] = {
                "chunks": {},
                "total": total_chunks
            }
        
        # Decode and store chunk
        chunk_bytes = base64.b64decode(chunk_data)
        self.file_buffers[key]["chunks"][chunk_index] = chunk_bytes
        
        logger.debug(f"[MANAGER] Received chunk {chunk_index + 1}/{total_chunks} for {filename}")
        
        # Check if file is complete
        buffer = self.file_buffers[key]
        if len(buffer["chunks"]) == total_chunks:
            # Assemble file
            file_data = b"".join(buffer["chunks"][i] for i in range(total_chunks))
            
            # Save to job folder
            job_folder = get_job_folder(job_id)
            output_path = job_folder / filename
            
            with open(output_path, "wb") as f:
                f.write(file_data)
            
            logger.info(f"[MANAGER] File {filename} received and saved ({len(file_data)} bytes)")
            
            # Clean up buffer
            del self.file_buffers[key]
    
    async def mark_idle(self, worker_id: str):
        """Mark worker as idle and available for new jobs"""
        worker = self.workers.get(worker_id)
        if worker:
            worker.busy = False
            worker.current_job = None
            logger.info(f"[MANAGER] Worker {worker_id} marked as idle")
    
    async def update_heartbeat(self, worker_id: str):
        """Update worker's last heartbeat timestamp"""
        worker = self.workers.get(worker_id)
        if worker:
            worker.last_heartbeat = datetime.now()
    
    async def handle_worker_failure(self, worker_id: str):
        """Handle worker disconnect or failure"""
        worker = self.workers.get(worker_id)
        if not worker:
            return
        
        logger.warning(f"[MANAGER] Handling failure for worker {worker_id}")
        
        # Requeue current job if any
        if worker.current_job:
            job_id = worker.current_job
            logger.info(f"[MANAGER] Requeuing job {job_id} from failed worker")
            
            r.lpush("job_queue", job_id)
            r.hset(f"job:{job_id}", mapping={
                "status": "pending",
                "worker_id": ""
            })
        
        # Remove worker
        del self.workers[worker_id]
        logger.info(f"[MANAGER] Worker {worker_id} removed")
    
    async def get_workers_status(self):
        """Get status of all registered workers"""
        return [
            {
                "worker_id": w.worker_id,
                "worker_url": w.worker_url,
                "busy": w.busy,
                "current_job": w.current_job,
                "last_heartbeat": w.last_heartbeat.isoformat() if w.last_heartbeat else None
            }
            for w in self.workers.values()
        ]
    
    async def cleanup_stale_workers(self, timeout_seconds: int = 60):
        """Remove workers that haven't sent heartbeat recently"""
        now = datetime.now()
        stale_workers = []
        
        for worker_id, worker in self.workers.items():
            if worker.last_heartbeat:
                elapsed = (now - worker.last_heartbeat).total_seconds()
                if elapsed > timeout_seconds:
                    stale_workers.append(worker_id)
        
        for worker_id in stale_workers:
            logger.warning(f"[MANAGER] Removing stale worker {worker_id}")
            await self.handle_worker_failure(worker_id)