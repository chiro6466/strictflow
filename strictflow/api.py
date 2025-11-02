import asyncio
from functools import wraps
from typing import Callable, Any, TypeVar, ParamSpec, Optional, Dict 
# Import core components and constants
from .core import StrictFlow, WRITE_PRIORITY, READ_PRIORITY, logger

P = ParamSpec('P')
R = TypeVar('R')

def write(flow_instance: StrictFlow, key: str) -> Callable[[Callable[[Any, P], R]], Callable[P, None]]:
    """
    Decorator for critical Write operations (Priority 1).
    
    Accepts: 
    - flow_instance: The StrictFlow controller instance.
    - key: The unique string identifier for the state data in the registry.
    
    The decorated function MUST accept the current isolated state (Any) as its 
    first argument and MUST RETURN the new state to be written atomically.
    """
    # The user function type requires the first argument (the state)
    def decorator(func: Callable[[Any, P], R]) -> Callable[P, None]:
        @wraps(func)
        def task_runner(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
            """Execution logic wrapper for P1 task within the Flow-Loop."""
            
            log_context = {'flow_priority': WRITE_PRIORITY}
            
            # The P1 lock should always be available inside the SF-LOOP (P1 execution is sequential)
            if not flow_instance._write_lock.acquire(blocking=False):
                # This should only happen if the developer is misusing the API outside the flow loop
                logger.critical(f"Write lock was already acquired. Execution error in '{func.__name__}'.", extra=log_context)
                return None 

            try:
                # 1. Clear the flag: Readers must WAIT
                flow_instance._read_wait_event.clear()
                logger.debug(f"P1 Write: '{func.__name__}' [START]. Flag DOWN. Readers blocked.", extra=log_context)
                
                # 2. STATE INJECTION: Get the isolated copy BEFORE execution
                current_state = flow_instance.state_registry.get_isolated_copy(key)

                # 3. Execute user function: Inject state as the first argument (current_state)
                new_state = func(current_state, *args, **kwargs)
                
                # 🛑 4. CRITICAL INTEGRITY DOUBLE CHECK (Fail-Safe Mechanism)
                # If the function returns None, the state is considered corrupt or the logic flawed.
                if new_state is None:
                    error_msg = f"CRITICAL STATE CORRUPTION ATTEMPT! Write function '{func.__name__}' for key '{key}' returned None. Commit ABORTED."
                    logger.critical(error_msg, extra=log_context)
                    # Force exception to skip to finally block WITHOUT committing
                    raise ValueError(error_msg)

                # 5. ATOMIC COMMIT: Update the global state in the registry
                flow_instance.state_registry.set(key, new_state)
                logger.debug(f"P1 Write: '{func.__name__}' [SUCCESS]. State '{key}' committed.", extra=log_context)
                
                return None
            except Exception as e:
                # Any exception (including the ValueError from the double check) prevents commit
                logger.error(f"FATAL ERROR or Integrity Check failure during P1 execution of '{func.__name__}'. Commit prevented.", exc_info=True, extra=log_context)
                return None
            finally:
                # 6. Release the lock and Set the flag: Readers can proceed
                flow_instance._write_lock.release()
                flow_instance._read_wait_event.set()
                logger.debug("P1 Write: Flag UP. Readers released.", extra=log_context)

        @wraps(func)
        def task_submitter(*args: P.args, **kwargs: P.kwargs) -> None:
            """Submits the wrapped runner function to the StrictFlow queue."""
            flow_instance._submit_task(WRITE_PRIORITY, task_runner, *args, **kwargs)
        
        return task_submitter
    return decorator


def read(flow_instance: StrictFlow, key: str) -> Callable[[Callable[[Any, P], R]], Callable[P, None]]:
    """
    Decorator for high-concurrency Read operations (Priority 2).
    
    Accepts: 
    - flow_instance: The StrictFlow controller instance.
    - key: The unique string identifier for the state data in the registry.
    
    The decorated function receives the isolated copy of the state (Any) as its
    first argument. It can be a synchronous or an asynchronous function.
    """
    def decorator(func: Callable[[Any, P], R]) -> Callable[P, None]:
        @wraps(func)
        def task_runner(*args: P.args, **kwargs: P.kwargs) -> Optional[R]:
            """Execution logic wrapper for P2 task within the Flow-Loop."""

            log_context = {'flow_priority': READ_PRIORITY}
            task_name = func.__name__

            # 1. Check the flag: If DOWN (False), a P1 (writer) is active.
            if not flow_instance._read_wait_event.is_set():
                logger.debug(f"P2 Read: '{task_name}' detects P1 active. Entering WAIT...", extra=log_context)
                
                # Blocking wait until P1 sets the flag (atomicity guarantee)
                flow_instance._read_wait_event.wait() 
                
                logger.debug(f"P2 Read: '{task_name}' Flag received. Exiting WAIT.", extra=log_context)

            # 2. STATE INJECTION: Get the isolated copy
            try:
                # This guarantees that the read operation is performed on a consistent snapshot.
                isolated_state = flow_instance.state_registry.get_isolated_copy(key)
            except Exception as e:
                logger.error(f"P2 Read: '{task_name}' failed during state isolation for key '{key}'.", exc_info=True, extra=log_context)
                return None

            # 3. Execute the encapsulated logic, injecting the state
            logger.debug(f"P2 Read: '{task_name}' Starting execution.", extra=log_context)
            try:
                # The user's function is called with the state + original arguments.
                if asyncio.iscoroutinefunction(func):
                    # For asynchronous P2 tasks, run them in the current loop (SF-LOOP thread)
                    result = asyncio.run(func(isolated_state, *args, **kwargs))
                else:
                    # For synchronous P2 tasks
                    result = func(isolated_state, *args, **kwargs)

                logger.debug(f"P2 Read: '{task_name}' Finished.", extra=log_context)
                return result
            except Exception as e:
                logger.error(f"Error during P2 execution of '{task_name}'.", exc_info=True, extra=log_context)
                return None
        
        @wraps(func)
        def task_submitter(*args: P.args, **kwargs: P.kwargs) -> None:
            """Submits the wrapped runner function to the StrictFlow queue."""
            flow_instance._submit_task(READ_PRIORITY, task_runner, *args, **kwargs)
            
        return task_submitter
    return decorator