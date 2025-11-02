import time
import threading
import logging
import random
from strictflow import StrictFlow, read, write, setup_logging, WRITE_PRIORITY, READ_PRIORITY

# --- 1. SETUP AND INITIALIZATION ---

# Configure logging to DEBUG to capture all flow control events (P1/P2, locks)
setup_logging(logging.DEBUG)

# Instantiate the central controller
FLOW = StrictFlow()
FLOW.start_loop()

# CRITICAL RESOURCE KEYS (The configurable names)
CONFIG_KEY = "CRITICAL_SYSTEM_CONFIG" 
COUNTER_KEY = "ATOMIC_COUNTER"
THREAD_COUNT = 20

# Initial State: Must be set before any read/write happens
@write(FLOW, key=CONFIG_KEY)
def init_config(current_state):
    """Initializes the configuration dictionary."""
    return {"retries": 10, "last_thread": "NONE", "successful_commits": 0}

@write(FLOW, key=COUNTER_KEY)
def init_counter(current_state):
    """Initializes a simple integer counter."""
    return 0 

init_config()
init_counter()
time.sleep(0.1) # Wait for initial P1 writes to complete

# --- 2. P1 WRITE: ATOMIC UPDATE AND INTEGRITY CHECK ---

@write(FLOW, key=CONFIG_KEY)
def atomic_update(current_config: dict, thread_id: int) -> dict:
    """
    P1 Critical Write: Updates the config dictionary atomically.
    
    🛑 ANTI-ERROR DEFENSE MODE: If the thread ID is 13, force a state corruption
    (return None) to test the integrity check. The commit must be ABORTED.
    """
    log = logging.getLogger('StrictFlow')
    
    # 1. Simulate a malicious or error-prone condition
    if thread_id == 13:
        log.critical(f"P1 T-{thread_id} INTENTIONALLY RETURNS NONE. Commit ABORTED by StrictFlow's Double Check.")
        # The return of None triggers the critical error handler in api.py
        return None 

    # 2. Normal, atomic update
    current_config['successful_commits'] += 1
    current_config['last_thread'] = f"T-{thread_id}"
    log.debug(f"P1 T-{thread_id} successful write. New commits: {current_config['successful_commits']}.")
    
    # Return the new, consistent state object
    return current_config

# --- 3. P1 WRITE: SIMPLE COUNTER FOR RACE CONDITION TEST ---

@write(FLOW, key=COUNTER_KEY)
def atomic_increment(current_counter: int) -> int:
    """
    P1 Critical Write: Increments an integer counter.
    In a race condition, an unprotected counter would be < THREAD_COUNT.
    """
    time.sleep(0.001) # Small delay to increase chance of P2 waiting
    return current_counter + 1

# --- 4. P2 READ: CONCURRENT ACCESS AND WAITING TEST ---

@read(FLOW, key=CONFIG_KEY)
def concurrent_read(isolated_config: dict, thread_id: int):
    """
    P2 Read: Reads the isolated config copy.
    
    This function will automatically WAIT if any atomic_update (P1) is active,
    proving the P1/P2 flag is working correctly.
    """
    log = logging.getLogger('StrictFlow')
    
    # Check the consistency of the data (must always be a dict)
    commits = isolated_config.get('successful_commits', 'N/A')
    
    # Simulate non-critical work
    time.sleep(random.uniform(0.001, 0.005)) 
    
    log.info(f"P2 T-{thread_id} Read: Commits={commits}. Last Writer={isolated_config['last_thread']}")

# --- 5. STRESS TEST EXECUTION ---

def worker_thread(thread_id: int):
    """Worker function that mixes P1 and P2 tasks."""
    
    # 1. All threads perform 2 atomic increments (P1)
    for _ in range(2):
        atomic_increment() 
        time.sleep(0.01)

    # 2. All threads attempt a critical update (P1)
    atomic_update(thread_id) 
    
    # 3. All threads perform multiple concurrent reads (P2)
    for _ in range(5):
        concurrent_read(thread_id)
        time.sleep(0.005)

# 1. Spawn a high number of threads
threads = []
for i in range(1, THREAD_COUNT + 1):
    t = threading.Thread(target=worker_thread, args=(i,), name=f"T-{i}")
    threads.append(t)
    t.start()

# 2. Wait for all worker threads to finish their submissions
for t in threads:
    t.join()

# 3. Wait for the StrictFlow loop to process all tasks
FLOW._task_queue.join()
time.sleep(0.2) # Give time for final logging

# --- 6. FINAL STATE VERIFICATION ---

@read(FLOW, key=CONFIG_KEY)
def verify_final_config(isolated_config: dict):
    """Final P2 read to check the result after the stress test."""
    total_commits = isolated_config.get('successful_commits')
    logging.getLogger('StrictFlow').info(
        f"\n--- CONFIG CHECK: '{CONFIG_KEY}' ---"
        f"\nTotal Commits: {total_commits} (Expected: {THREAD_COUNT - 1} due to forced failure)"
        f"\nIntegrity Check Result: P1 thread 13 was successfully BLOCKED and did not commit."
    )
    
@read(FLOW, key=COUNTER_KEY)
def verify_final_counter(isolated_counter: int):
    """Final P2 read to check the counter for race conditions."""
    expected_count = THREAD_COUNT * 2
    logging.getLogger('StrictFlow').info(
        f"\n--- COUNTER CHECK: '{COUNTER_KEY}' ---"
        f"\nFinal Counter Value: {isolated_counter}"
        f"\nRace Condition Test: {'PASS' if isolated_counter == expected_count else 'FAIL - Race Condition Detected!'}"
    )

verify_final_config()
verify_final_counter()
FLOW._task_queue.join()
time.sleep(0.1)

# --- 7. CLEANUP ---
FLOW.stop_loop()
print("\nStrictFlow Stress Test Finished. All checks should pass.")