"""
Worker Client for Vast.AI Instances - COMPATIBLE WITH YOUR FASTAPI
Connects to the hub, receives jobs, processes them using your existing FastAPI server
Run this on each Vast.AI instance alongside your FastAPI server
"""
import asyncio
import websockets
import json
import httpx
import base64
from pathlib import Path
import logging
import os
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkerClient:
    def __init__(self, hub_url: str, worker_url: str):
        """
        Args:
            hub_url: WebSocket URL of the hub (e.g., ws://hub.example.com:8001/ws/worker)
            worker_url: Base URL of this worker's FastAPI (e.g., http://localhost:8000)
        """
        self.hub_url = hub_url
        self.worker_url = worker_url
        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        self.ws = None
        self.running = False
        self.current_job = None
        
        # Temporary storage for incoming files
        self.temp_dir = Path("./worker_temp")
        self.temp_dir.mkdir(exist_ok=True)
        
        # File buffer for receiving chunks
        self.file_buffers = {}
    
    async def connect(self):
        """Connect to the hub"""
        logger.info(f"[WORKER] Connecting to hub at {self.hub_url}")
        
        try:
            self.ws = await websockets.connect(self.hub_url)
            
            # Register with hub
            await self.ws.send(json.dumps({
                "type": "register",
                "worker_id": self.worker_id,
                "worker_url": self.worker_url
            }))
            
            # Wait for registration confirmation
            response = json.loads(await self.ws.recv())
            if response.get("type") == "registered":
                logger.info(f"[WORKER] Registered successfully as {self.worker_id}")
                self.running = True
                return True
            else:
                logger.error(f"[WORKER] Registration failed: {response}")
                return False
        
        except Exception as e:
            logger.error(f"[WORKER] Connection failed: {e}")
            return False
    
    async def heartbeat_loop(self):
        """Send periodic heartbeats to hub"""
        while self.running:
            try:
                await self.ws.send(json.dumps({
                    "type": "heartbeat",
                    "worker_id": self.worker_id
                }))
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"[WORKER] Heartbeat error: {e}")
                break
    
    async def message_loop(self):
        """Main message processing loop"""
        try:
            while self.running:
                message = await self.ws.recv()
                msg = json.loads(message)
                
                msg_type = msg.get("type")
                
                if msg_type == "assign_job":
                    await self.handle_job_assignment(msg)
                
                elif msg_type == "file_chunk":
                    await self.handle_file_chunk(msg)
                
                elif msg_type == "request_outputs":
                    await self.handle_output_request(msg)
                
                elif msg_type == "heartbeat_ack":
                    pass  # Heartbeat acknowledged
                
                else:
                    logger.warning(f"[WORKER] Unknown message type: {msg_type}")
        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("[WORKER] Connection to hub closed")
            self.running = False
        except Exception as e:
            logger.error(f"[WORKER] Error in message loop: {e}")
            self.running = False
    
    async def handle_job_assignment(self, msg):
        """Handle new job assignment from hub"""
        job_id = msg["job_id"]
        job_type = msg["job_type"]
        
        logger.info(f"[WORKER] Received job: {job_id} (type: {job_type})")
        self.current_job = job_id
        
        # Create job directory
        job_dir = self.temp_dir / job_id
        job_dir.mkdir(exist_ok=True)
        
        # Files will be received via file_chunk messages
    
    async def handle_file_chunk(self, msg):
        """Handle incoming file chunk from hub"""
        job_id = msg["job_id"]
        filename = msg["filename"]
        chunk_data = msg["data"]
        chunk_index = msg["chunk_index"]
        total_chunks = msg["total_chunks"]
        
        key = (job_id, filename)
        
        # Initialize buffer if needed
        if key not in self.file_buffers:
            self.file_buffers[key] = {
                "chunks": {},
                "total": total_chunks
            }
        
        # Decode and store chunk
        chunk_bytes = base64.b64decode(chunk_data)
        self.file_buffers[key]["chunks"][chunk_index] = chunk_bytes
        
        logger.info(f"[WORKER] Received chunk {chunk_index + 1}/{total_chunks} for {filename}")
        
        # Check if file is complete
        if len(self.file_buffers[key]["chunks"]) == total_chunks:
            await self.assemble_and_process_file(job_id, filename, key)
    
    async def assemble_and_process_file(self, job_id: str, filename: str, key: tuple):
        """Assemble file from chunks and process job when both files are ready"""
        buffer_data = self.file_buffers[key]
        chunks = buffer_data["chunks"]
        total_chunks = buffer_data["total"]
        
        # Assemble file
        file_data = b"".join(chunks[i] for i in range(total_chunks))
        
        # Save to job directory
        job_dir = self.temp_dir / job_id
        output_path = job_dir / filename
        
        with open(output_path, "wb") as f:
            f.write(file_data)
        
        logger.info(f"[WORKER] File {filename} assembled ({len(file_data)} bytes)")
        
        # Clean up buffer
        del self.file_buffers[key]
        
        # Check if both files are ready
        lower_file = job_dir / "lower.obj"
        upper_file = job_dir / "upper.obj"
        
        if lower_file.exists() and upper_file.exists():
            logger.info(f"[WORKER] Both input files ready, processing job {job_id}")
            await self.process_job(job_id)
    
    async def process_job(self, job_id: str):
        """
        Process the segmentation job using your existing FastAPI server
        This is compatible with your server.py that expects /segment endpoint
        """
        job_dir = self.temp_dir / job_id
        lower_file = job_dir / "lower.obj"
        upper_file = job_dir / "upper.obj"
        
        try:
            # Notify hub that job started
            await self.ws.send(json.dumps({
                "type": "job_started",
                "job_id": job_id,
                "worker_id": self.worker_id
            }))
            
            logger.info(f"[WORKER] Processing job {job_id}")
            
            # Call your existing FastAPI /segment endpoint
            async with httpx.AsyncClient(timeout=300.0) as client:
                with open(lower_file, "rb") as lf, open(upper_file, "rb") as uf:
                    files = {
                        "lower": ("lower.obj", lf, "application/octet-stream"),
                        "upper": ("upper.obj", uf, "application/octet-stream")
                    }
                    
                    logger.info(f"[WORKER] Calling {self.worker_url}/segment")
                    response = await client.post(
                        f"{self.worker_url}/segment",
                        files=files
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        logger.info(f"[WORKER] Job {job_id} completed successfully")
                        logger.info(f"[WORKER] Result: {result}")
                        
                        # Download output files from /outputs/{filename} endpoint
                        await self.download_outputs(job_id)
                        
                        # Notify hub of completion
                        await self.ws.send(json.dumps({
                            "type": "job_done",
                            "job_id": job_id,
                            "worker_id": self.worker_id
                        }))
                        
                        self.current_job = None
                    else:
                        raise Exception(f"Segmentation failed with status {response.status_code}: {response.text}")
        
        except Exception as e:
            logger.error(f"[WORKER] Job {job_id} failed: {e}")
            
            # Notify hub of failure
            await self.ws.send(json.dumps({
                "type": "job_failed",
                "job_id": job_id,
                "worker_id": self.worker_id,
                "error": str(e)
            }))
            
            self.current_job = None
    
    async def download_outputs(self, job_id: str):
        """
        Download output files from your FastAPI's /outputs/{filename} endpoint
        Your server.py outputs: input_lower.obj, input_upper.obj, input_lower.json, input_upper.json
        """
        job_dir = self.temp_dir / job_id
        
        # These match your server.py output filenames
        output_files = [
            "input_lower.obj",
            "input_upper.obj",
            "input_lower.json",
            "input_upper.json"
        ]
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for filename in output_files:
                try:
                    logger.info(f"[WORKER] Downloading {filename} from {self.worker_url}/outputs/{filename}")
                    response = await client.get(f"{self.worker_url}/outputs/{filename}")
                    
                    if response.status_code == 200:
                        output_path = job_dir / filename
                        with open(output_path, "wb") as f:
                            f.write(response.content)
                        logger.info(f"[WORKER] Downloaded {filename} ({len(response.content)} bytes)")
                    else:
                        logger.warning(f"[WORKER] Could not download {filename}: {response.status_code}")
                
                except Exception as e:
                    logger.error(f"[WORKER] Error downloading {filename}: {e}")
    
    async def handle_output_request(self, msg):
        """Send output files back to hub"""
        job_id = msg["job_id"]
        files = msg["files"]
        
        job_dir = self.temp_dir / job_id
        
        logger.info(f"[WORKER] Hub requested {len(files)} output files for job {job_id}")
        
        for filename in files:
            file_path = job_dir / filename
            
            if file_path.exists():
                await self.send_file_to_hub(job_id, file_path)
            else:
                logger.warning(f"[WORKER] Output file not found: {filename}")
    
    async def send_file_to_hub(self, job_id: str, file_path: Path, chunk_size: int = 1024 * 1024):
        """Send a file to hub in chunks"""
        filename = file_path.name
        
        with open(file_path, "rb") as f:
            data = f.read()
        
        total_size = len(data)
        total_chunks = (total_size + chunk_size - 1) // chunk_size
        
        logger.info(f"[WORKER] Sending {filename} ({total_size} bytes) in {total_chunks} chunks")
        
        for i in range(total_chunks):
            start = i * chunk_size
            end = min(start + chunk_size, total_size)
            chunk = data[start:end]
            
            chunk_b64 = base64.b64encode(chunk).decode('utf-8')
            
            await self.ws.send(json.dumps({
                "type": "file_chunk",
                "job_id": job_id,
                "filename": filename,
                "data": chunk_b64,
                "chunk_index": i,
                "total_chunks": total_chunks
            }))
            
            # Small delay to avoid overwhelming the connection
            if i % 10 == 0:
                await asyncio.sleep(0.01)
        
        logger.info(f"[WORKER] File {filename} sent successfully")
    
    async def run(self):
        """Main run loop"""
        while True:
            try:
                if await self.connect():
                    # Start heartbeat and message loops
                    await asyncio.gather(
                        self.heartbeat_loop(),
                        self.message_loop()
                    )
                
                logger.warning("[WORKER] Disconnected, reconnecting in 5 seconds...")
                await asyncio.sleep(5)
            
            except KeyboardInterrupt:
                logger.info("[WORKER] Shutting down...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"[WORKER] Error: {e}")
                await asyncio.sleep(5)

async def main():
    # Configuration - Set these via environment variables or command line
    HUB_URL = os.getenv("HUB_URL", "ws://localhost:8001/ws/worker")
    WORKER_URL = os.getenv("WORKER_URL", "http://localhost:8000")
    
    logger.info("=" * 60)
    logger.info("WORKER CLIENT STARTING")
    logger.info("=" * 60)
    logger.info(f"Hub URL: {HUB_URL}")
    logger.info(f"Worker FastAPI URL: {WORKER_URL}")
    logger.info("=" * 60)
    
    worker = WorkerClient(hub_url=HUB_URL, worker_url=WORKER_URL)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())