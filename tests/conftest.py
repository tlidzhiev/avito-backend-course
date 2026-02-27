from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture(autouse=True)
def _mock_db(monkeypatch):
    mock_pool = MagicMock()

    async def mock_init():
        return mock_pool

    async def mock_close(pool):
        pass

    monkeypatch.setattr('src.main.init_db_pool', mock_init)
    monkeypatch.setattr('src.main.close_db_pool', mock_close)


@pytest.fixture
def client():
    with TestClient(app) as client:
        yield client
