"""
Smoke test for the outbox contract.

Verifies:
  1. ingest._write_chunk() produces JSON files in pending/ with the right shape
  2. main._list_outbox_chunks() reads them back in order
  3. main._push_one_outbox_chunk() reads, validates, and moves to processed/
  4. main.process_outbox_to_taxii() drives the full cycle with a mocked TAXII
  5. Failure case: chunk with invalid JSON is moved to processed/ (skipped)
  6. Failure case: chunk that fails to push stays in pending/ for retry

This is a disk-only test — no OTX, no TAXII, no Redis, no network.
"""

import json
import os
import sys
import tempfile
import shutil
import logging

# Ensure imports resolve.
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)  # otx2taxii/
PARENT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(PARENT, "venv", "lib", "python3.11", "site-packages"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("smoke")


# ---------------------------------------------------------------------------
# Test 1: ingest._write_chunk produces well-formed JSON.
# ---------------------------------------------------------------------------
def test_write_chunk_produces_well_formed_json():
    """Verify the writer side of the contract."""
    from ingest import _write_chunk, _safe_chunk_filename

    # Filename helper
    name = _safe_chunk_filename("pulse-abc-123", 1, 3, 200)
    assert name == "pulse-abc-123__1__3__200.json", f"unexpected filename: {name}"
    log.info("PASS  test_write_chunk_produces_well_formed_json: filename helper OK")

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    os.makedirs(pending)

    bundle_dict = {
        "type": "bundle",
        "id": "bundle--00000000-0000-0000-0000-000000000001",
        "objects": [
            {"type": "identity", "id": "identity--otx"},
            {"type": "grouping", "id": "grouping--g1", "name": "Test - 1/3"},
        ],
    }
    path = _write_chunk(
        pending_dir=pending,
        pulse_id="pulse-abc-123",
        pulse_name="Test Pulse",
        chunk_idx=1,
        chunk_total=3,
        bundle_dict=bundle_dict,
    )
    assert os.path.isfile(path), f"file not written: {path}"
    with open(path) as f:
        payload = json.load(f)

    # Verify payload structure
    assert payload["pulse_id"] == "pulse-abc-123"
    assert payload["pulse_name"] == "Test Pulse"
    assert payload["chunk_idx"] == 1
    assert payload["chunk_total"] == 3
    assert payload["indicator_count"] == 2
    assert "created_at" in payload
    assert payload["stix_bundle"] == bundle_dict
    log.info(f"PASS  test_write_chunk_produces_well_formed_json: payload OK at {path}")

    # Cleanup
    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Test 2: main._list_outbox_chunks reads in order.
# ---------------------------------------------------------------------------
def test_list_outbox_chunks_reads_in_order():
    """Verify the reader side orders files deterministically."""
    from ingest import _write_chunk
    from main import _list_outbox_chunks

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    os.makedirs(pending)

    # Write chunks in NON-sorted order to verify sort logic.
    chunks_to_write = [
        ("pulse-X", 2, 3, 100, "X - 2/3"),
        ("pulse-X", 1, 3, 200, "X - 1/3"),
        ("pulse-A", 1, 1, 50, "A"),
        ("pulse-X", 3, 3, 47, "X - 3/3"),
        (".tmp", 0, 0, 0, None),  # Should be filtered out
    ]
    for pulse_id, idx, total, count, _ in chunks_to_write:
        if pulse_id == ".tmp":
            # Write a stray .tmp file that should be ignored
            with open(os.path.join(pending, "stray.tmp"), "w") as f:
                f.write("ignore me")
            continue
        # filename's indicator_count == len(bundle.objects). Compute it to
        # match what _write_chunk actually emits.
        n_objects = (count // 50) + 1
        _write_chunk(
            pending_dir=pending,
            pulse_id=pulse_id,
            pulse_name=pulse_id,
            chunk_idx=idx,
            chunk_total=total,
            bundle_dict={
                "type": "bundle",
                "id": f"bundle--{pulse_id}-{idx}",
                "objects": [{"type": "identity", "id": "identity--otx"}] * n_objects,
            },
        )

    listed = _list_outbox_chunks(pending)
    basenames = [os.path.basename(p) for p in listed]

    # Expected sort: alphabetical by filename.
    # indicator_count == (count // 50) + 1:
    #   pulse-A count=50  -> 2 objects  -> pulse-A__1__1__2.json
    #   pulse-X count=200 -> 5 objects  -> pulse-X__1__3__5.json
    #   pulse-X count=100 -> 3 objects  -> pulse-X__2__3__3.json
    #   pulse-X count=47  -> 1 object   -> pulse-X__3__3__1.json
    expected = [
        "pulse-A__1__1__2.json",
        "pulse-X__1__3__5.json",
        "pulse-X__2__3__3.json",
        "pulse-X__3__3__1.json",
    ]
    assert basenames == expected, f"got {basenames}, expected {expected}"
    # No .tmp files in the list
    assert not any(".tmp" in b for b in basenames)
    log.info(f"PASS  test_list_outbox_chunks_reads_in_order: {basenames}")

    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Test 3: _push_one_outbox_chunk moves file on success.
# ---------------------------------------------------------------------------
def test_push_chunk_moves_to_processed_on_success():
    """Verify the pusher moves a successfully-pushed chunk to processed/."""
    from ingest import _write_chunk
    from main import _push_one_outbox_chunk

    # Mock Config
    class FakeConfig:
        ENABLE_CACHE_PREVALIDATION = False

    # Mock TAXII client that always succeeds
    class FakeTaxii:
        def add_stix_bundle(self, bundle_dict):
            return True

    # Mock OTX client (no-op cache_stix_uuid when pre-validation off)
    class FakeOtx:
        def cache_stix_uuid(self, *a, **k):
            pass

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    processed = os.path.join(tmp, "processed")
    os.makedirs(pending)
    os.makedirs(processed)

    _write_chunk(
        pending_dir=pending,
        pulse_id="p1",
        pulse_name="Test 1",
        chunk_idx=1,
        chunk_total=1,
        bundle_dict={"type": "bundle", "id": "bundle--x", "objects": []},
    )
    chunk_path = _list_first(pending)

    push_ok, proc_ok, pid, cidx, ctot = _push_one_outbox_chunk(
        config=FakeConfig(),
        taxii_client=FakeTaxii(),
        otx_client=FakeOtx(),
        pending_dir=pending,
        processed_dir=processed,
        chunk_path=chunk_path,
    )
    assert push_ok is True, "push should have succeeded"
    assert proc_ok is True
    assert pid == "p1"
    assert not os.path.exists(chunk_path), "file should have moved out of pending/"
    moved_path = os.path.join(processed, os.path.basename(chunk_path))
    assert os.path.exists(moved_path), "file should be in processed/"
    log.info("PASS  test_push_chunk_moves_to_processed_on_success")

    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Test 4: _push_one_outbox_chunk keeps file on failure (for retry).
# ---------------------------------------------------------------------------
def test_push_chunk_stays_in_pending_on_failure():
    """Verify that a failed push leaves the file in pending/ for next cycle."""
    from ingest import _write_chunk
    from main import _push_one_outbox_chunk

    class FakeConfig:
        ENABLE_CACHE_PREVALIDATION = False

    class FakeTaxiiFailing:
        def add_stix_bundle(self, bundle_dict):
            return False  # push fails

    class FakeOtx:
        def cache_stix_uuid(self, *a, **k):
            pass

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    processed = os.path.join(tmp, "processed")
    os.makedirs(pending)
    os.makedirs(processed)

    _write_chunk(
        pending_dir=pending,
        pulse_id="p2",
        pulse_name="Test 2",
        chunk_idx=1,
        chunk_total=1,
        bundle_dict={"type": "bundle", "id": "bundle--x", "objects": []},
    )
    chunk_path = _list_first(pending)

    push_ok, proc_ok, pid, cidx, ctot = _push_one_outbox_chunk(
        config=FakeConfig(),
        taxii_client=FakeTaxiiFailing(),
        otx_client=FakeOtx(),
        pending_dir=pending,
        processed_dir=processed,
        chunk_path=chunk_path,
    )
    assert push_ok is False, "push should have failed"
    assert proc_ok is True
    assert os.path.exists(chunk_path), "file should stay in pending/ for retry"
    assert not os.listdir(processed), "nothing in processed/"
    log.info("PASS  test_push_chunk_stays_in_pending_on_failure")

    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Test 5: _push_one_outbox_chunk skips corrupt JSON (moves to processed/).
# ---------------------------------------------------------------------------
def test_push_chunk_skips_corrupt_json():
    """Verify a corrupt JSON file is moved to processed/ to skip forever."""
    from main import _push_one_outbox_chunk

    class FakeConfig:
        ENABLE_CACHE_PREVALIDATION = False

    class FakeTaxii:
        def add_stix_bundle(self, bundle_dict):
            return True

    class FakeOtx:
        def cache_stix_uuid(self, *a, **k):
            pass

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    processed = os.path.join(tmp, "processed")
    os.makedirs(pending)
    os.makedirs(processed)

    # Write garbage JSON
    corrupt_path = os.path.join(pending, "corrupt__1__1__1.json")
    with open(corrupt_path, "w") as f:
        f.write("{this is not valid JSON")

    push_ok, proc_ok, pid, cidx, ctot = _push_one_outbox_chunk(
        config=FakeConfig(),
        taxii_client=FakeTaxii(),
        otx_client=FakeOtx(),
        pending_dir=pending,
        processed_dir=processed,
        chunk_path=corrupt_path,
    )
    assert push_ok is False
    assert proc_ok is False
    assert not os.path.exists(corrupt_path), "corrupt file should be moved out"
    moved = os.path.join(processed, "corrupt__1__1__1.json")
    assert os.path.exists(moved), "corrupt file should be in processed/ to skip"
    log.info("PASS  test_push_chunk_skips_corrupt_json")

    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Test 6: process_outbox_to_taxii drives the full cycle.
# ---------------------------------------------------------------------------
def test_process_outbox_to_taxii_full_cycle():
    """End-to-end: write 3 chunks, run process_outbox_to_taxii, verify all moved."""
    from ingest import _write_chunk
    from main import process_outbox_to_taxii

    class FakeConfig:
        STIX_OUTBOX_DIR = ""
        ENABLE_CACHE_PREVALIDATION = False
        MAX_WORKERS = 1
        MAX_BUNDLES_TO_PUSH = None
        OUTBOX_RETENTION_DAYS = 0

    pushed_bundles = []

    class FakeTaxii:
        def add_stix_bundle(self, bundle_dict):
            pushed_bundles.append(bundle_dict["id"])
            return True

    class FakeOtx:
        def cache_stix_uuid(self, *a, **k):
            pass

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    processed = os.path.join(tmp, "processed")
    os.makedirs(pending)
    os.makedirs(processed)
    FakeConfig.STIX_OUTBOX_DIR = tmp

    # Write 3 chunks
    for i in range(1, 4):
        _write_chunk(
            pending_dir=pending,
            pulse_id=f"p{i}",
            pulse_name=f"Pulse {i}",
            chunk_idx=i,
            chunk_total=3,
            bundle_dict={
                "type": "bundle",
                "id": f"bundle--p{i}-{i}",
                "objects": [{"type": "identity", "id": "identity--otx"}],
            },
        )

    assert len(os.listdir(pending)) == 3

    # Run the full cycle
    process_outbox_to_taxii(FakeConfig(), FakeTaxii(), FakeOtx())

    # Verify all pushed
    assert len(pushed_bundles) == 3, f"expected 3 pushes, got {len(pushed_bundles)}"
    assert sorted(pushed_bundles) == sorted(
        ["bundle--p1-1", "bundle--p2-2", "bundle--p3-3"]
    )

    # Verify all moved to processed
    assert len(os.listdir(pending)) == 0, "pending should be empty after success"
    assert len(os.listdir(processed)) == 3, "processed should have 3 files"
    log.info(f"PASS  test_process_outbox_to_taxii_full_cycle: pushed {pushed_bundles}")

    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Test 7: empty pending dir is a no-op.
# ---------------------------------------------------------------------------
def test_process_outbox_handles_empty_pending():
    """No chunks pending should not error and should not push anything."""
    from main import process_outbox_to_taxii

    class FakeConfig:
        STIX_OUTBOX_DIR = ""
        ENABLE_CACHE_PREVALIDATION = False
        MAX_WORKERS = 1
        MAX_BUNDLES_TO_PUSH = None
        OUTBOX_RETENTION_DAYS = 0

    pushed = []

    class FakeTaxii:
        def add_stix_bundle(self, bd):
            pushed.append(bd)
            return True

    class FakeOtx:
        pass

    tmp = tempfile.mkdtemp(prefix="outbox_test_")
    pending = os.path.join(tmp, "pending")
    processed = os.path.join(tmp, "processed")
    os.makedirs(pending)
    os.makedirs(processed)
    FakeConfig.STIX_OUTBOX_DIR = tmp

    process_outbox_to_taxii(FakeConfig(), FakeTaxii(), FakeOtx())
    assert len(pushed) == 0
    log.info("PASS  test_process_outbox_handles_empty_pending")

    shutil.rmtree(tmp)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _list_first(pending_dir):
    files = sorted(os.listdir(pending_dir))
    assert len(files) >= 1, f"expected at least one file in {pending_dir}"
    return os.path.join(pending_dir, files[0])


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    tests = [
        test_write_chunk_produces_well_formed_json,
        test_list_outbox_chunks_reads_in_order,
        test_push_chunk_moves_to_processed_on_success,
        test_push_chunk_stays_in_pending_on_failure,
        test_push_chunk_skips_corrupt_json,
        test_process_outbox_to_taxii_full_cycle,
        test_process_outbox_handles_empty_pending,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except Exception as e:
            log.error(f"FAIL  {t.__name__}: {e}", exc_info=True)
            failed += 1
    print()
    print(f"=== {passed} passed, {failed} failed, {len(tests)} total ===")
    sys.exit(1 if failed else 0)
