"""
Enhanced Hub Server for 3D Teeth Segmentation
Manages jobs across multiple Vast.AI worker instances
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from .manager import WorkerManager, get_job_folder
from shared.redis_client import r
import json
import uuid
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AISEG Hub")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

manager = WorkerManager()

# job_id -> WebSocket of client
clients = {}

# ---------- Client Job Submission ----------
@app.post("/jobs/submit")
async def submit_job(
    lower: UploadFile = File(..., description="Lower jaw OBJ file"),
    upper: UploadFile = File(..., description="Upper jaw OBJ file")
):
    """
    Submit a new segmentation job with input files.
    Files are saved locally and job is queued for processing.
    """
    # Validate file extensions
    if not lower.filename.endswith('.obj'):
        raise HTTPException(status_code=400, detail="Lower jaw file must be .obj format")
    if not upper.filename.endswith('.obj'):
        raise HTTPException(status_code=400, detail="Upper jaw file must be .obj format")
    
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    folder = get_job_folder(job_id)
    
    # Save input files
    lower_path = folder / "lower.obj"
    upper_path = folder / "upper.obj"
    
    try:
        with open(lower_path, "wb") as f:
            content = await lower.read()
            f.write(content)
        
        with open(upper_path, "wb") as f:
            content = await upper.read()
            f.write(content)
        
        logger.info(f"[HUB] Saved input files for job {job_id}")
        
        # Save job metadata in Redis
        r.hset(f"job:{job_id}", mapping={
            "job_type": "teeth_segmentation",
            "job_path": str(folder),
            "status": "pending",
            "lower_file": "lower.obj",
            "upper_file": "upper.obj"
        })
        
        # Add to job queue
        r.lpush("job_queue", job_id)
        
        logger.info(f"[HUB] Job submitted: {job_id}")
        
        # Try to dispatch immediately
        await manager.try_dispatch()
        
        return {
            "status": "ok",
            "job_id": job_id,
            "message": "Job submitted successfully"
        }
    
    except Exception as e:
        logger.error(f"[HUB] Error saving files for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/jobs/{job_id}/status")
def get_job_status(job_id: str):
    """Get the current status of a job"""
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(404, "Job not found")
    return {
        "job_id": job_id,
        "status": data.get("status", "unknown"),
        "worker_id": data.get("worker_id"),
        "job_type": data.get("job_type")
    }

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    """Get complete job information"""
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(404, "Job not found")
    return data

# ---------- File Download ----------
@app.get("/jobs/{job_id}/download/{filename}")
def download(job_id: str, filename: str):
    """Download output files after job completion"""
    # Security: Only allow specific output files
    allowed_files = [
        "input_lower.obj",
        "input_upper.obj",
        "input_lower.json",
        "input_upper.json"
    ]
    
    if filename not in allowed_files:
        raise HTTPException(400, "Invalid filename")
    
    file_path = get_job_folder(job_id) / filename
    if not file_path.exists():
        raise HTTPException(404, "File not found")
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/octet-stream"
    )

@app.get("/jobs/{job_id}/results")
def get_results(job_id: str):
    """Get all output file paths for a completed job"""
    data = r.hgetall(f"job:{job_id}")
    if not data:
        raise HTTPException(404, "Job not found")
    
    if data.get("status") != "done":
        raise HTTPException(400, f"Job status is {data.get('status')}, not done")
    
    folder = get_job_folder(job_id)
    output_files = {
        "lower_obj": f"/jobs/{job_id}/download/input_lower.obj",
        "upper_obj": f"/jobs/{job_id}/download/input_upper.obj",
        "lower_labels": f"/jobs/{job_id}/download/input_lower.json",
        "upper_labels": f"/jobs/{job_id}/download/input_upper.json"
    }
    
    # Verify files exist
    existing_files = {}
    for key, url in output_files.items():
        filename = url.split("/")[-1]
        if (folder / filename).exists():
            existing_files[key] = url
    
    return {
        "job_id": job_id,
        "status": "done",
        "outputs": existing_files
    }

# ---------- Worker WebSocket ----------
@app.websocket("/ws/worker")
async def worker_ws(ws: WebSocket):
    """
    WebSocket endpoint for worker instances.
    Workers register, receive jobs, and report results.
    """
    await ws.accept()
    worker_id = None
    
    try:
        # Wait for registration message
        msg = json.loads(await ws.receive_text())
        if msg.get("type") != "register":
            await ws.send_json({"type": "error", "message": "First message must be registration"})
            await ws.close()
            return
        
        worker_id = msg["worker_id"]
        worker_url = msg.get("worker_url")  # Base URL of the worker's FastAPI
        
        await manager.register(worker_id, ws, worker_url)
        await ws.send_json({"type": "registered", "worker_id": worker_id})
        
        logger.info(f"[HUB] Worker registered: {worker_id} at {worker_url}")
        
        # Try to dispatch a job immediately
        await manager.try_dispatch()
        
        # Message loop
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            
            if msg["type"] == "heartbeat":
                await manager.update_heartbeat(worker_id)
                await ws.send_json({"type": "heartbeat_ack"})
            
            elif msg["type"] == "job_started":
                job_id = msg["job_id"]
                logger.info(f"[HUB] Job {job_id} started on worker {worker_id}")
                r.hset(f"job:{job_id}", "status", "running")
            
            elif msg["type"] == "job_done":
                job_id = msg["job_id"]
                logger.info(f"[HUB] Job {job_id} completed on worker {worker_id}")
                
                # Request output files from worker
                await manager.fetch_job_results(worker_id, job_id)
                
                # Mark job as done
                r.hset(f"job:{job_id}", "status", "done")
                await manager.mark_idle(worker_id)
                
                # Notify client if subscribed
                client_ws = clients.get(job_id)
                if client_ws:
                    try:
                        await client_ws.send_json({
                            "type": "job_done",
                            "job_id": job_id,
                            "worker_id": worker_id
                        })
                    except:
                        pass
                    finally:
                        del clients[job_id]
                
                # Try to dispatch next job
                await manager.try_dispatch()
            
            elif msg["type"] == "job_failed":
                job_id = msg["job_id"]
                error = msg.get("error", "Unknown error")
                logger.error(f"[HUB] Job {job_id} failed on worker {worker_id}: {error}")
                
                # Requeue job
                r.hset(f"job:{job_id}", "status", "pending")
                r.lpush("job_queue", job_id)
                await manager.mark_idle(worker_id)
                
                # Notify client
                client_ws = clients.get(job_id)
                if client_ws:
                    try:
                        await client_ws.send_json({
                            "type": "job_failed",
                            "job_id": job_id,
                            "error": error
                        })
                    except:
                        pass
                
                # Try to dispatch with different worker
                await manager.try_dispatch()
            
            elif msg["type"] == "file_chunk":
                # Receive file chunks from worker
                job_id = msg["job_id"]
                filename = msg["filename"]
                chunk_data = msg["data"]
                chunk_index = msg["chunk_index"]
                total_chunks = msg["total_chunks"]
                
                await manager.receive_file_chunk(
                    job_id, filename, chunk_data, chunk_index, total_chunks
                )
    
    except WebSocketDisconnect:
        logger.warning(f"[HUB] Worker {worker_id} disconnected")
        if worker_id:
            await manager.handle_worker_failure(worker_id)
    
    except Exception as e:
        logger.error(f"[HUB] Error in worker websocket: {e}")
        if worker_id:
            await manager.handle_worker_failure(worker_id)

# ---------- Client WebSocket ----------
@app.websocket("/ws/client")
async def client_ws(ws: WebSocket):
    """
    WebSocket endpoint for clients to receive real-time job updates.
    """
    await ws.accept()
    subscribed_jobs = set()
    
    try:
        while True:
            msg = json.loads(await ws.receive_text())
            
            if msg.get("type") == "subscribe":
                job_id = msg["job_id"]
                clients[job_id] = ws
                subscribed_jobs.add(job_id)
                
                # Send current status
                job_data = r.hgetall(f"job:{job_id}")
                if job_data:
                    await ws.send_json({
                        "type": "status_update",
                        "job_id": job_id,
                        "status": job_data.get("status", "unknown")
                    })
    
    except WebSocketDisconnect:
        # Clean up subscriptions
        for job_id in subscribed_jobs:
            if clients.get(job_id) == ws:
                del clients[job_id]

# ---------- Worker Management Endpoints ----------
@app.get("/workers")
async def list_workers():
    """List all connected workers and their status"""
    return await manager.get_workers_status()

@app.get("/stats")
async def get_stats():
    """Get system statistics"""
    queue_length = r.llen("job_queue")
    workers = await manager.get_workers_status()
    
    return {
        "queue_length": queue_length,
        "total_workers": len(workers),
        "busy_workers": sum(1 for w in workers if w["busy"]),
        "idle_workers": sum(1 for w in workers if not w["busy"])
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)