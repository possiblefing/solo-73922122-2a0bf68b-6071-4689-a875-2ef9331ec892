"""Per-device independence and read-model tests."""
import pytest

from tests.conftest import submit, uniform_csv, wait_status


@pytest.mark.asyncio
async def test_devices_have_independent_baselines_and_windows(client):
    ac = client
    # Device X: three baselines then a drift.
    for i in range(1, 4):
        await submit(ac, f"X{i}", "EQ-X", uniform_csv(f"x{i}", 5, 500))
        await wait_status(ac, f"X{i}", "RELEASED")
    await submit(ac, "XQ", "EQ-X", uniform_csv("xq", 5, 600))
    xq = await wait_status(ac, "XQ", "QUARANTINED")
    assert xq["reference_batch_nos"] == ["X1", "X2", "X3"]

    # Device Y starts fresh: its first three are baselines even though X
    # already has history, and Y's own scale (800) must not be judged against X.
    for i in range(1, 4):
        await submit(ac, f"Y{i}", "EQ-Y", uniform_csv(f"y{i}", 5, 800))
        yi = await wait_status(ac, f"Y{i}", "RELEASED")
        assert yi["reference_batch_nos"] == []
    # Y's fourth batch at 806: drift relative to Y's own 800 mean -> quarantine.
    await submit(ac, "Y4", "EQ-Y", uniform_csv("y4", 5, 806))
    y4 = await wait_status(ac, "Y4", "QUARANTINED")
    assert y4["reference_batch_nos"] == ["Y1", "Y2", "Y3"]
    assert y4["reference_mean_nm"] == pytest.approx(800.0)


@pytest.mark.asyncio
async def test_device_listing_and_404(client):
    ac = client
    await submit(ac, "P1", "EQ-P", uniform_csv("p1", 3, 500))
    await wait_status(ac, "P1", "RELEASED")
    r = await ac.get("/devices/EQ-P/batches")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1 and body["batches"][0]["batch_no"] == "P1"

    r = await ac.get("/batches/DOES-NOT-EXIST")
    assert r.status_code == 404

    r = await ac.get("/health")
    assert r.status_code == 200 and r.json()["database"] is True


@pytest.mark.asyncio
async def test_pagination_and_filtering_of_released_data(client):
    ac = client
    for i in range(1, 5):
        await submit(ac, f"PG{i}", "EQ-PG", uniform_csv(f"pg{i}", 2, 500))
        await wait_status(ac, f"PG{i}", "RELEASED")
    page1 = await ac.get("/data/released", params={"limit": 2, "offset": 0})
    page2 = await ac.get("/data/released", params={"limit": 2, "offset": 2})
    p1, p2 = page1.json(), page2.json()
    assert p1["total_released"] == 4 and len(p1["items"]) == 2
    assert len(p2["items"]) == 2
    ids1 = {i["batch_no"] for i in p1["items"]}
    ids2 = {i["batch_no"] for i in p2["items"]}
    assert ids1.isdisjoint(ids2)
    # Bad pagination params are rejected by FastAPI validation.
    r = await ac.get("/data/released", params={"limit": 0})
    assert r.status_code == 422
    r = await ac.get("/data/released", params={"offset": -1})
    assert r.status_code == 422
