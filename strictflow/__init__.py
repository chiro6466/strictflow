from .core import StrictFlow, setup_logging, WRITE_PRIORITY, READ_PRIORITY
from .api import read, write

# ----------------------------------------------------
# Module Docstring (Professional Documentation)
# ----------------------------------------------------
"""
StrictFlow: Single-Threaded Atomic State Manager for Data Integrity
=================================================================

StrictFlow is an infrastructure library providing a robust, single-threaded 
concurrency model specialized for state management in high-integrity environments
(e.g., financial trading bots, critical APIs). It enforces a strict separation 
between Write (P1) and Read (P2) operations, ensuring the central state 
data is always consistent and protected against race conditions.

### Core Features (Safety Design):

* **Atomic State Commits (P1 Write):** Guarantees that only one write operation 
    can modify the canonical state at any given time.
* **Zero-Risk Read Isolation (P2 Read):** Provides a deep, isolated copy of the state 
    to all read tasks, preventing accidental corruption of the global state via 
    mutable objects (list, dict, etc.).
* **Mandatory Synchronization (Intentional Blocking):** **StrictFlow's core design** uses synchronization primitives (Locks and Events) even in a single-threaded 
    loop. This is a **proactive measure** to ensure **absolute data integrity**
    against system lag, memory failure, or future multi-threading errors. 
    **P2 Readers are deliberately paused until the P1 Writer confirms cleanup.**
* **Priority Queue:** All P1 (Write) and P2 (Read) tasks are executed sequentially 
    in the single 'SF-LOOP' thread based on priority (P1 > P2).
* **Crash Resilience:** Synchronization primitives are released using 'finally' 
    blocks to ensure system integrity and prevent deadlocks even during task crashes.
"""

# ----------------------------------------------------
# 💡 Usage Snippet for AI / Documentation (English)
# ----------------------------------------------------
# This snippet provides a minimal, functional example of the API structure.

# from strictflow import StrictFlow, write, read, setup_logging
# import logging
# import time

# # Initialize the main flow controller and state
# flow = StrictFlow() 
# flow.state_registry.set(key="crypto_prices", value={"BTC": 50000.0, "ETH": 3500.0})
# flow.start_loop()

# # 1. Define a P1 Write task (Priority 1: Updates)
# @write(flow_instance=flow, key="crypto_prices")
# def update_price_from_api(current_prices: dict, symbol: str, new_price: float):
#     # NOTE: The critical update is guarded by P1's strict lock.
#     # Any P2 read submitted at the same time MUST wait for this to finish.
#     current_prices[symbol] = new_price
#     return current_prices # MUST return the updated state

# # 2. Define a P2 Read task (Priority 2: Consumption)
# @read(flow_instance=flow, key="crypto_prices")
# def get_current_price(isolated_prices: dict, symbol: str):
#     # isolated_prices is a safe, deep copy.
#     # NOTE: P2 is guaranteed to read the state AFTER the last P1 commit.
#     price = isolated_prices.get(symbol)
#     # print(f"Current {symbol} Price: {price}")
#     return price

# # 3. Example Execution Flow (Submitting tasks)
# if __name__ == '__main__':
#     update_price_from_api(symbol="BTC", new_price=50100.0)
#     time.sleep(0.01) # Small delay for P1 to be processed
#     price_btc = get_current_price(symbol="BTC")
#     # print(f"Final BTC Price: {price_btc}") 
#     # flow.stop_loop()

# Basic library info
__version__ = "0.1.0"

__author__ = "chiro"
