StrictFlow 🔒

A strict, thread-safe flow controller for Python concurrency, enforcing Read (P2) and Write (P1) priority.

💡 The Problem

In high-concurrency, multi-threaded applications (e.g., financial systems, industrial IoT, or server configurations), simultaneous reads and writes to a single piece of global state can lead to data corruption, inconsistent reads, or fatal race conditions. This is especially true for data that cannot fail or be read mid-update (e.g., blockchain transaction hashes, live configuration objects).

Standard Python locking mechanisms often ensure safety but lack priority and ordered execution.

✨ The StrictFlow Solution

StrictFlow implements a single-thread, priority-based task queue that provides exclusive access for critical operations, ensuring data integrity is never compromised.

It enforces two critical priority levels:

P1 Write (Critical): Tasks decorated with @write are the highest priority. When a P1 task is executed, all P2 Read tasks are paused and blocked until the write is complete and stable. This guarantees the writer has exclusive access.

P2 Read (High-Concurrency): Tasks decorated with @read can run highly concurrent logic internally (using asyncio), but they must wait synchronously for any active P1 Write task to finish.

This pattern ensures readers never see unstable or incomplete data.

📦 Installation

pip install strictflow


(Note: Once uploaded to PyPI, this command will work.)

🚀 Quick Start Example

This example demonstrates how a P1 Write task (updating config) blocks P2 Read tasks to prevent them from reading the old version (V0) or an unstable version (V1 mid-write).

import threading
import time
import asyncio
import logging
from strictflow import StrictFlow, read, write, setup_logging
from strictflow.core import logger # Use the internal logger for application logs

# 1. The user MUST activate logging to see output
setup_logging(logging.DEBUG) 

GLOBAL_CONFIG = {"version": 0, "status": "stable"}

def main():
    flow = StrictFlow()
    loop_thread = threading.Thread(target=flow.run_loop, daemon=True, name="FlowLoop")
    loop_thread.start()
    
    @write(flow)
    def update_config(new_version: int, new_status: str):
        """P1 Write: Simulates a critical, blocking update."""
        global GLOBAL_CONFIG
        print(f"   [P1] Working: Simulating 1.5s write for V{new_version}...")
        time.sleep(1.5) 
        
        GLOBAL_CONFIG['version'] = new_version
        GLOBAL_CONFIG['status'] = new_status
        print(f"   [P1] Complete: Config updated to V{new_version}.")

    
    @read(flow)
    async def fetch_data_async(reader_id: str):
        """P2 Read: Can run concurrently but MUST wait for P1."""
        for i in range(3):
            await asyncio.sleep(0.3) 
            current_config = GLOBAL_CONFIG.copy()
            print(f"   [P2] Reader {reader_id} read | V{current_config['version']} | Iteration {i+1}")
            
    # --- SIMULATION ---
    fetch_data_async(reader_id="R-A") # P2 task submitted
    
    time.sleep(0.5) 
    
    update_config(new_version=1, new_status="deploying") # P1 task submitted (Blocks R-A)

    fetch_data_async(reader_id="R-B") # P2 task submitted (Will also wait for P1)

    time.sleep(7.5) # Wait for all tasks to finish
    flow.stop_loop()
    loop_thread.join() 

if __name__ == "__main__":
    main()


🛠️ Key Concepts

1. Thread-Safe Execution

All tasks (P1 and P2) are submitted to and executed sequentially by a single, dedicated worker thread (SF-LOOP). This single-threaded execution context eliminates concurrency issues at the data access level.

2. Priority Control (P1 vs P2)

While all tasks execute in the dedicated thread, the P1/P2 mechanism controls when P2 tasks are allowed to begin. P2 tasks actively check a status flag set by the P1 writer. If the flag is clear, they block the SF-LOOP thread until the P1 task is complete.

3. Asynchronous Reads

The @read decorator allows the wrapped function to be async. When the P2 task is executed by the SF-LOOP thread, it runs the async function using asyncio.run(), allowing the P2 task to achieve high internal concurrency (multiple fast reads) without requiring locks on the shared data outside the loop.