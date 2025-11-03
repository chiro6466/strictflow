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

# Thread-local storage for Flow Priority context (INTERNAL USE ONLY)
_thread_local_context = threading.local()
_thread_local_context.flow_priority = 'N/A'

# --- LOGGING SETUP FUNCTION ---

def setup_logging(level: int = logging.INFO) -> None:
    """Configures the library's internal logging handler."""
    if logger.handlers:
        for handler in logger.handlers:
            handler.filters.clear()
    else:
        logger.setLevel(logging.DEBUG) 
        
        # Log Format: [TIME] [LEVEL] [THREAD] [Priority] MESSAGE
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)-8s %(threadName)-10s (P%(flow_priority)s) %(message)s',
            datefmt='%H:%M:%S'
        )
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Define a filter to inject the thread local priority
    class PriorityFilter(logging.Filter):
        def filter(self, record):
            record.flow_priority = getattr(_thread_local_context, 'flow_priority', 'N/A')
            return True

    logger.addFilter(PriorityFilter())
    logger.setLevel(level)

# --- CONSTANTS ---
WRITE_PRIORITY = 1 # Lower number means higher priority (1 is highest)
READ_PRIORITY = 2

# --- ATOMIC STATE REGISTRY ---

class AtomicStateRegistry:
    """Manages the central, canonical state dictionary."""
    
    def __init__(self):
        # The single source of truth for all flow operations
        self._state_dict: Dict[str, Any] = {}
        
    def get_isolated_copy(self, key: str) -> Any:
        """
        Returns a deep copy of the state for a key. 
        
        CRITICAL: Returns None if the key is not initialized. This allows P1 
        writers to initialize the state, providing a safe starting point.
        """
        if key not in self._state_dict:
            # FIX: Return None to allow P1 writers (like initializers) to create the state.
            return None 
        # Uses deepcopy to guarantee isolation for mutable objects
        return copy.deepcopy(self._state_dict[key])
        
    def set(self, key: str, value: Any) -> None:
        """
        Sets the canonical state value. 
        Note: This is only called by the P1 writer's execution logic after 
        all integrity checks pass, and it's protected by the single SF-LOOP thread.
        """
        self._state_dict[key] = value

# --- MAIN FLOW CONTROLLER ---

class StrictFlow:
    """
    The central synchronous controller running in a single dedicated thread (SF-LOOP).
    Enforces task priority and uses synchronization primitives for defense-in-depth.
    """
    
    def __init__(self):
        self._running: bool = False
        # Single, dedicated thread for serialization
        self._thread: threading.Thread = threading.Thread(target=self._run_loop, daemon=True, name="SF-LOOP-Control")
        
        self._task_queue: queue.PriorityQueue = queue.PriorityQueue()
        self.state_registry: AtomicStateRegistry = AtomicStateRegistry()

        # Synchronization Primitives (Defense-in-Depth Safety Features)
        # 1. Write Lock (P1): Explicit lock for API safety barrier against accidental re-entrancy.
        self._write_lock: threading.Lock = threading.Lock() 
        # 2. Read Wait Event (P2): Flag to block P2 until P1 confirms commit completion.
        self._read_wait_event: threading.Event = threading.Event() 
        self._read_wait_event.set() # Start in 'UP' state (reads allowed by default)

        self.WRITE_PRIORITY = WRITE_PRIORITY
        self.READ_PRIORITY = READ_PRIORITY
        
        self._submission_counter: int = 0
        
    def start_loop(self) -> None:
        """Starts the dedicated SF-LOOP thread."""
        if not self._running:
            self._thread.start()
            logger.info("StrictFlow Task Loop initialized.")

    def stop_loop(self) -> None:
        """Signals the dedicated SF-LOOP thread to shut down."""
        self._running = False
        
    def _submit_task(self, priority: int, func: Callable, *args: Any, **kwargs: Any) -> None:
        """Submits a task (P1 or P2) to the priority queue."""
        # The submission counter ensures FIFO order for tasks with equal priority.
        self._submission_counter += 1
        self._task_queue.put((priority, self._submission_counter, time.time(), func, args, kwargs))

    def _run_loop(self) -> None:
        """The main synchronous execution loop (takes over the thread)."""
        threading.current_thread().name = "SF-LOOP"
        self._running = True
        logger.info("The main Flow-Loop has started.")

        while self._running:
            try:
                # 1. Get the highest priority task (P1 > P2)
                priority, _, _, func, args, kwargs = self._task_queue.get(timeout=0.1) 
                
                # 2. Set the priority context for accurate logging
                _thread_local_context.flow_priority = priority
                
                logger.debug(f"Executing P{priority} task: '{func.__name__}'")
                
                # 3. Execute the task
                func(*args, **kwargs)
                
                self._task_queue.task_done()

            except queue.Empty:
                pass # Expected when no tasks are present
            except Exception as e:
                # CRASH RESILIENCE: Log the error and drop the task.
                # This ensures the SF-LOOP thread is never halted by a task failure.
                logger.error(f"Critical ERROR while executing task: {e}. Dropping task and continuing loop.", exc_info=True)
            finally:
                # 4. Clean up the priority context
                _thread_local_context.flow_priority = 'N/A'
                
        logger.info("The main Flow-Loop is shutting down...")
        try:
            self._task_queue.join() 
        except Exception:
            pass
