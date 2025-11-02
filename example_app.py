import time
import threading
import logging

# --- Assuming the package is installed or structured as follows: ---
from strictflow import StrictFlow, read, write, setup_logging, WRITE_PRIORITY, READ_PRIORITY

# --- 1. SETUP ---

# Configure StrictFlow's internal logger to see P1/P2 task executions
setup_logging(logging.DEBUG)

# Instantiate the central controller
FLOW = StrictFlow()

# The critical, configurable key that names the data.
# This KEY is what LogicLock would use to export/import the state between processes.
CONFIG_KEY = "SYSTEM_CONFIG_V1" 
LIST_KEY = "PENDING_JOBS_LIST"

# Start the dedicated control thread
FLOW.start_loop()

# --- 2. STATE INITIALIZATION (P1 Write to set initial values) ---

@write(FLOW, key=CONFIG_KEY)
def initialize_config(current_state):
    """Initializes the configuration dictionary."""
    if current_state is None:
        return {"retries": 5, "timeout_ms": 1000}
    return current_state # Already initialized, return as is

@write(FLOW, key=LIST_KEY)
def initialize_list(current_state):
    """Initializes the list of pending jobs."""
    if current_state is None:
        return []
    return current_state

# Run initializers (will be executed as P1 tasks)
initialize_config()
initialize_list()
time.sleep(0.1) # Wait for initial P1 writes to complete

# --- 3. CRITICAL WRITE FUNCTION (P1) ---

@write(FLOW, key=CONFIG_KEY)
def update_retries(current_config: dict, new_value: int) -> dict:
    """
    P1 Critical Write: Updates a single field in the isolated state copy.
    
    The function MUST return the new state (dict) for atomic commit.
    The integrity double-check will ensure 'None' is never committed.
    """
    
    # Simulate a critical logic step
    if new_value < 1:
        logging.getLogger('StrictFlow').critical("Integrity Error: Tried to set retries < 1. ABORT.")
        return None # 🛑 THIS TRIGGERS THE STRICTFLOW DOUBLE CHECK AND PREVENTS COMMIT
        
    current_config['retries'] = new_value
    current_config['last_update'] = time.time()
    
    # The new isolated object is returned and committed atomically by the flow loop
    return current_config

# --- 4. HIGH-CONCURRENCY READ FUNCTION (P2) ---

@read(FLOW, key=CONFIG_KEY)
def print_current_config(isolated_config: dict, thread_id: int):
    """
    P2 Read: Reads the isolated config copy.
    
    If a P1 (update_retries) is running, this function will WAIT for its commit/cleanup.
    The 'isolated_config' received is a safe, consistent snapshot.
    """
    retries = isolated_config.get('retries', 'N/A')
    timeout = isolated_config.get('timeout_ms', 'N/A')
    
    # Simulate work
    time.sleep(0.001) 
    
    logging.getLogger('StrictFlow').info(
        f"Thread {thread_id}: Read Config '{CONFIG_KEY}'. Retries: {retries}, Timeout: {timeout}"
    )

# --- 5. SIMULATE CONCURRENT EXECUTION ---

def worker_thread(thread_id: int):
    """Runs a sequence of reads and writes."""
    
    if thread_id % 3 == 0:
        # A subset of threads performs critical writes
        new_retry_value = 10 + thread_id
        update_retries(new_retry_value)
        
    for _ in range(5):
        # All threads perform high-concurrency reads
        print_current_config(thread_id)
        time.sleep(0.005)

# 1. Spawn multiple threads to simulate high load
threads = []
for i in range(1, 11):
    t = threading.Thread(target=worker_thread, args=(i,), name=f"Worker-{i}")
    threads.append(t)
    t.start()

# 2. Wait for all worker threads to finish their task submissions
for t in threads:
    t.join()

# 3. Wait for the StrictFlow loop to finish executing all queued tasks
FLOW._task_queue.join()
time.sleep(0.1) # Give a moment for final cleanup logs

# --- 6. FINAL STATE VERIFICATION ---

@read(FLOW, key=CONFIG_KEY)
def verify_final_state(isolated_config: dict):
    """A final P2 read to check the result after all operations."""
    final_retries = isolated_config.get('retries')
    logging.getLogger('StrictFlow').info(
        f"--- FINAL STATE CHECK --- Key: '{CONFIG_KEY}'. Final Retries Value: {final_retries}"
    )
    
verify_final_state()
FLOW._task_queue.join()
time.sleep(0.1)

# --- 7. CLEANUP ---
FLOW.stop_loop()
print("\nStrictFlow Example Finished. Check logs for P1/P2 sequence and final consistency.")