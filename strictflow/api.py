import asyncio
from functools import wraps
from typing import Callable, Any, TypeVar, ParamSpec, Optional, Dict 
# Import core components and constants
from .core import StrictFlow, WRITE_PRIORITY, READ_PRIORITY, logger 

P = ParamSpec('P')
R = TypeVar('R')

def write(flow_instance: StrictFlow, key: str) -> Callable[[Callable[[Any, P], R]], Callable[P, None]]:
    """
    Decorator for Write operations (P1). Enforces exclusivity, atomicity, 
    and controls the read/write synchronization barrier.
    """
    def decorator(func: Callable[[Any, P], R]) -> Callable[P, None]:
        @wraps(func)
        def task_runner(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
            """Execution logic wrapper for P1 task within the Flow-Loop."""
            
            # DEFENSE-IN-DEPTH: Acquire the write lock. 
            # This acts as an API safety barrier against re-entrancy bugs.
            if not flow_instance._write_lock.acquire(blocking=False):
                logger.critical(f"Write lock was already acquired. Execution error in '{func.__name__}'.") 
                return None 

            try:
                # 1. BLOCK READS: P1 sets the Flag DOWN. This blocks any subsequent P2 tasks.
                flow_instance._read_wait_event.clear()
                logger.debug(f"P1 Write: '{func.__name__}' [START]. Flag DOWN. Readers blocked.")
                
                # 2. Get isolated copy and execute the user's function
                current_state = flow_instance.state_registry.get_isolated_copy(key)
                new_state = func(current_state, *args, **kwargs)
                
                # 3. INTEGRITY CHECK: The core security check. If result is None, abort commit.
                if new_state is None:
                    error_msg = f"INTEGRITY CHECK FAILED! Write function '{func.__name__}' for key '{key}' returned None. Commit ABORTED."
                    logger.critical(error_msg)
                    # Use ValueError to trigger the 'except' block for consistent logging,
                    # but the 'finally' is guaranteed to run.
                    raise ValueError(error_msg) 

                # 4. ATOMIC COMMIT AND GLOBAL UPDATE
                flow_instance.state_registry.set(key, new_state)
                logger.debug(f"P1 Write: '{func.__name__}' [SUCCESS]. State '{key}' committed and global state updated.")
                
                return None
            except Exception as e:
                # Any exception prevents the commit (Zero tolerance for corrupt data).
                logger.error(f"FATAL ERROR or Integrity Check failure during P1 execution of '{func.__name__}'. Commit prevented.", exc_info=True)
                return None
            finally:
                # 5. RELEASE BARRIER (CRITICAL): Executes even if the task crashed.
                # This raises the flag, guaranteeing that P2 can proceed.
                flow_instance._read_wait_event.set()
                logger.debug("P1 Write: Flag UP. Readers released. P1 finished execution.")
                
                # Release the write lock.
                flow_instance._write_lock.release()
                
        @wraps(func)
        def task_submitter(*args: P.args, **kwargs: P.kwargs) -> None:
            """Submits the P1 task to the flow queue with high priority."""
            flow_instance._submit_task(WRITE_PRIORITY, task_runner, *args, **kwargs)
        
        return task_submitter
    return decorator


def read(flow_instance: StrictFlow, key: str) -> Callable[[Callable[[Any, P], R]], Callable[P, None]]:
    """
    Decorator for Read operations (P2). Enforces isolation and guarantees data freshness.
    """
    def decorator(func: Callable[[Any, P], R]) -> Callable[P, None]:
        @wraps(func)
        def task_runner(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
            """Execution logic wrapper for P2 task within the Flow-Loop."""

            task_name = func.__name__

            # CRITICAL SAFETY BLOCK (Intentional Blocking)
            if not flow_instance._read_wait_event.is_set():
                logger.debug(f"P2 Read: '{task_name}' detects P1 active. Entering WAIT...")
                # P2 MUST pause until P1 confirms its commit is finalized (Flag UP).
                # This guarantees that P2 never reads stale data.
                flow_instance._read_wait_event.wait() 
                logger.debug(f"P2 Read: '{task_name}' Flag received. Exiting WAIT.")

            # 2. Get safe isolated copy
            try:
                isolated_state = flow_instance.state_registry.get_isolated_copy(key)
            except Exception as e:
                logger.error(f"P2 Read: '{task_name}' failed during state isolation for key '{key}'.", exc_info=True)
                return None

            # 3. Execute the user's logic (supports both sync and async functions)
            logger.debug(f"P2 Read: '{task_name}' Starting execution.")
            try:
                if asyncio.iscoroutinefunction(func):
                    # Handle async: Creates a new loop within the synchronous thread.
                    loop = asyncio.new_event_loop()
                    try:
                        coro = func(isolated_state, *args, **kwargs)
                        result = loop.run_until_complete(coro)
                    finally:
                        loop.close()
                else:
                    # Simple synchronous P2 task
                    result = func(isolated_state, *args, **kwargs)

                logger.debug(f"P2 Read: '{task_name}' Finished.")
                return result
            except Exception as e:
                logger.error(f"Error during P2 execution of '{task_name}'.", exc_info=True)
                return None
            finally:
                pass # Log context cleanup is handled by core.py
        
        @wraps(func)
        def task_submitter(*args: P.args, **kwargs: P.kwargs) -> None:
            """Submits the P2 task to the flow queue with standard priority."""
            flow_instance._submit_task(READ_PRIORITY, task_runner, *args, **kwargs)
            
        return task_submitter
    return decorator
