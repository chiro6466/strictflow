import threading
import queue
import logging
import sys
import asyncio
import copy
import time
from typing import Callable, Any, Dict, List, Tuple

# Initialize the library-specific logger
logger = logging.getLogger('StrictFlow')
logger.propagate = False 

# --- LOGGING SETUP FUNCTION ---

def setup_logging(level: int = logging.INFO) -> None:
    """
    Configures the library's internal logging handler.
    
    Users must call this function from their main application file 
    to see the output logs (e.g., setup_logging(logging.DEBUG)).
    """
    if logger.handlers:
        return

    # Set the root level to DEBUG so the user can filter at the handler level
    logger.setLevel(logging.DEBUG) 
    
    # Log Format: [TIME] [LEVEL] [THREAD] [Priority] MESSAGE
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)-8s %(threadName)-10s (P%(flow_priority)s) %(message)s',
        datefmt='%H:%M:%S'
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level) # Set the handler level based on user input
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    if level <= logging.DEBUG:
        logger.debug("Debug logging enabled for StrictFlow.", extra={'flow_priority': 'SETUP'})

# --- PRIORITY CONSTANTS ---
WRITE_PRIORITY = 1 # P1: Highest priority, used for exclusive Write operations.
READ_PRIORITY = 2  # P2: Lower priority, used for high-concurrency Read operations.

# --- ATOMIC STATE REGISTRY ---

class AtomicStateRegistry:
    """
    Manages the centralized, canonical (single source of truth) state data.
    Ensures that external access is only ever made to an isolated deep copy.
    """
    def __init__(self):
        # Internal dictionary to hold all key-value state pairs
        self._data: Dict[str, Any] = {}

    def get_isolated_copy(self, key: str) -> Any:
        """
        Retrieves a deep, isolated copy of the state associated with the key.
        This copy prevents external functions from corrupting the global state
        via mutable objects (e.g., modifying a list in place).
        """
        if key not in self._data:
            # For simplicity, we initialize with None if the key is accessed before write
            # The caller (decorator) should handle initial state if required
            self._data[key] = None

        # Return a deep copy to ensure isolation
        return copy.deepcopy(self._data[key])

    def set(self, key: str, value: Any) -> None:
        """
        Atomically sets the new state value for the given key.
        This method is called exclusively by the P1 writer after the integrity check.
        """
        self._data[key] = value

# --- STRICT FLOW CORE CLASS ---

class StrictFlow:
    """
    The central controller for synchronous and prioritized task execution.
    It runs in a single, dedicated thread, ensuring serialization of critical tasks.
    """
    
    def __init__(self):
        # Internal control flags and structures
        self._running: bool = False
        self._thread: threading.Thread = threading.Thread(target=self._run_loop, daemon=True, name="SF-LOOP-Control")
        
        # Priority Queue: Stores (priority, submission_time, func, args, kwargs)
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue()
        
        # State Management: The Single Source of Truth for all data
        self.state_registry: AtomicStateRegistry = AtomicStateRegistry()

        # Synchronization Primitives
        # P1 Lock: Guarantees exclusive access for any write operation
        self._write_lock: threading.Lock = threading.Lock()
        # P2 Event: Signaled (set) when no P1 is active. Readers WAIT when clear.
        self._read_wait_event: threading.Event = threading.Event()
        self.WRITE_PRIORITY = WRITE_PRIORITY
        self.READ_PRIORITY = READ_PRIORITY
        
        # Logging Context for thread-specific data
        self._extra_log_context: Dict[str, Any] = {'flow_priority': 'N/A'}

    def start_loop(self) -> None:
        """Starts the dedicated StrictFlow execution thread."""
        if self._running:
            return

        self._read_wait_event.set() # Initialize event to ON (Readers can proceed)
        self._thread.start()
        logger.info("StrictFlow Task Loop initialized.", extra={'flow_priority': 'N/A'})

    def stop_loop(self) -> None:
        """Signals the Flow-Loop thread to gracefully stop its execution."""
        self._running = False
        self._thread.join()

    def _submit_task(self, priority: int, func: Callable, *args: Any, **kwargs: Any) -> None:
        """Internal method to add a task to the priority queue."""
        # The submission time acts as a tie-breaker for the priority queue
        submission_time = time.time() 
        # Task is stored as: (priority, submission_time, func, args, kwargs)
        self._task_queue.put((priority, submission_time, func, args, kwargs))

    def _run_loop(self) -> None:
        """The main, single-threaded execution loop."""
        if self._running:
            logger.warning("Attempted to start Flow-Loop but it is already running.", extra=self._extra_log_context)
            return

        threading.current_thread().name = "SF-LOOP"
        self._running = True
        logger.info("The main Flow-Loop has started.", extra=self._extra_log_context)

        while self._running:
            try:
                # Retrieve task: Wait for 100ms for graceful shutdown
                priority, _, func, args, kwargs = self._task_queue.get(timeout=0.1) 
                
                self._extra_log_context['flow_priority'] = priority
                
                logger.debug(f"Executing P{priority} task: '{func.__name__}'", extra=self._extra_log_context)
                
                # Execute the task runner directly in the SF-LOOP thread
                func(*args, **kwargs)
                
                self._task_queue.task_done()

            except queue.Empty:
                # No tasks to execute, continue the loop
                pass 
            except Exception as e:
                # Critical error during task execution, log it and continue
                logger.error(f"Critical ERROR while executing task: {e}", exc_info=True, extra=self._extra_log_context)
                
        # Graceful shutdown process
        self._extra_log_context['flow_priority'] = 'N/A'
        logger.info("The main Flow-Loop is shutting down...", extra=self._extra_log_context)
        try:
            self._task_queue.join() 
        except Exception:
            pass # Ignore exception if the queue is empty
        logger.info("Flow-Loop stopped cleanly.", extra=self._extra_log_context)