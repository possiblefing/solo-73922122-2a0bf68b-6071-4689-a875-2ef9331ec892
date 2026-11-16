"""End-to-end decision-rule tests through the FastAPI app + real Postgres."""
import pytest

from tests.conftest import make_csv, submit, uniform_csv, wait_status


@pytest.mark.asyncio
async def test_first_three_always_released(client):
    ac = client
    for i, thick in enumerate([100, 200, 300], start=1):
        r = await submit(ac, f"B{i}", "EQ-A", uniform_csv(f"b{i}", 5, thick))
        assert r.status_code == 202, r.text
        d = await wait_status(ac, f"B{i}", "RELEASED")
        assert d["accept_seq"] == i
        assert d["reference_batch_nos"] == []  # baseline has no reference set


@pytest.mark.asyncio
async def test_drift_quarantine_and_released_within_tolerance(client):
    ac = client
    # Three baselines at ~500 (medians 500, 500, 500 -> mean 500).
    for i in range(1, 4):
        await submit(ac, f"BASE{i}", "EQ-D", uniform_csv(f"base{i}", 5, 500))
        await wait_status(ac, f"BASE{i}", "RELEASED")

    # Batch 4: median 506 -> deviation 6 > 5 -> QUARANTINED.
    await submit(ac, "DRIFT", "EQ-D", uniform_csv("drift", 5, 506))
    drift = await wait_status(ac, "DRIFT", "QUARANTINED")
    assert drift["deviation_nm"] == pytest.approx(6.0)
    assert drift["reference_mean_nm"] == pytest.approx(500.0)
    assert drift["reference_batch_nos"] == ["BASE1", "BASE2", "BASE3"]

    # Batch 5: median 505 -> deviation exactly 5 -> RELEASED (rule: > 5 only).
    await submit(ac, "EDGE", "EQ-D", uniform_csv("edge", 5, 505))
    edge = await wait_status(ac, "EDGE", "RELEASED")
    assert edge["deviation_nm"] == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_quarantined_batch_never_enters_reference_window(client):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"R{i}", "EQ-W", uniform_csv(f"r{i}", 5, 500))
        await wait_status(ac, f"R{i}", "RELEASED")
    # Quarantined outlier at 900.
    await submit(ac, "Q1", "EQ-W", uniform_csv("q1", 5, 900))
    await wait_status(ac, "Q1", "QUARANTINED")
    # Next batch is still compared against R1,R2,R3 (mean 500), not Q1.
    await submit(ac, "R4", "EQ-W", uniform_csv("r4", 5, 504))
    r4 = await wait_status(ac, "R4", "RELEASED")
    assert r4["reference_batch_nos"] == ["R1", "R2", "R3"]
    assert r4["deviation_nm"] == pytest.approx(4.0)
    # Rolling window slides: R4 replaces R1 for the following batch.
    await submit(ac, "R5", "EQ-W", uniform_csv("r5", 5, 503))
    r5 = await wait_status(ac, "R5", "RELEASED")
    assert r5["reference_batch_nos"] == ["R2", "R3", "R4"]


@pytest.mark.asyncio
async def test_even_wafer_count_median_is_average_of_middles(client):
    ac = client
    await submit(ac, "M1", "EQ-M", make_csv([("a", 100), ("b", 200),
                                             ("c", 300), ("d", 400)]))
    d = await wait_status(ac, "M1", "RELEASED")
    assert d["median_nm"] == pytest.approx(250.0)


@pytest.mark.asyncio
async def test_validation_errors_return_400(client):
    ac = client
    r = await submit(ac, "BAD1", "EQ-Z", b"wafer_no,thickness_nm\nW1,0\n")
    assert r.status_code == 400
    r = await submit(ac, "BAD2", "EQ-Z", b"wafer_no,thickness_nm\nW1,5\nW1,6\n")
    assert r.status_code == 400
    r = await submit(ac, "", "EQ-Z", uniform_csv("x", 2, 10))
    assert r.status_code in (400, 422)


@pytest.mark.asyncio
async def test_retry_identical_reused_conflicting_rejected(client):
    ac = client
    csv1 = uniform_csv("a", 3, 500)
    r1 = await submit(ac, "DUP", "EQ-C", csv1)
    assert r1.status_code == 202
    first_id = r1.json()["batch_id"]
    await wait_status(ac, "DUP", "RELEASED")

    # Identical bytes -> reuse, same id, flagged.
    r2 = await submit(ac, "DUP", "EQ-C", csv1)
    assert r2.status_code == 202
    assert r2.json()["reused_retry"] is True
    assert r2.json()["batch_id"] == first_id

    # Same batch number, different content -> 409.
    r3 = await submit(ac, "DUP", "EQ-C", uniform_csv("b", 3, 501))
    assert r3.status_code == 409

    # Same batch number, different device -> 409.
    r4 = await submit(ac, "DUP", "EQ-OTHER", csv1)
    assert r4.status_code == 409


@pytest.mark.asyncio
async def test_data_endpoint_serves_only_released(client):
    ac = client
    for i in range(1, 4):
        await submit(ac, f"L{i}", "EQ-L", uniform_csv(f"l{i}", 4, 500))
        await wait_status(ac, f"L{i}", "RELEASED")
    await submit(ac, "LQ", "EQ-L", uniform_csv("lq", 4, 900))
    await wait_status(ac, "LQ", "QUARANTINED")

    r = await ac.get("/data/released")
    assert r.status_code == 200
    body = r.json()
    assert body["total_released"] == 3
    nos = {item["batch_no"] for item in body["items"]}
    assert "LQ" not in nos
    # Released detail rows carry measurements.
    item = next(i for i in body["items"] if i["batch_no"] == "L1")
    assert len(item["wafers"]) == 4
    assert item["wafers"][0]["thickness_nm"] == 500

    # Filtering works and the quarantined batch is invisible even explicitly.
    r = await ac.get("/data/released", params={"batch_no": "LQ"})
    assert r.json()["total_released"] == 0
    r = await ac.get("/data/released", params={"device_id": "EQ-L"})
    assert r.json()["total_released"] == 3
