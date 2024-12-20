import pytest
import asyncio
from tradebot.core.entity import TaskManager
from tradebot.core.nautilius_core import MessageBus, LiveClock
from tradebot.core.registry import OrderRegistry
from nautilus_trader.model.identifiers import TraderId

"""
Creates one fixture for the entire test run
Most efficient but least isolated
Example: Database connection that can be reused across all tests
"""


@pytest.fixture(scope="session")
def event_loop_policy():
    import asyncio

    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture
def task_manager(event_loop_policy):
    loop = event_loop_policy.new_event_loop()
    return TaskManager(loop, enable_signal_handlers=False)


@pytest.fixture
def message_bus():
    return MessageBus(trader_id=TraderId("TEST-001"), clock=LiveClock())


@pytest.fixture
def order_registry():
    return OrderRegistry()
