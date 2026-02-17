from __future__ import annotations

from pathlib import Path

from uk_gp_practices.index import PracticeIndex


def test_epraccur_ingest_and_search(tmp_path: Path) -> None:
    db_file = tmp_path / "practices.sqlite3"
    csv_file = Path(__file__).parent / "fixtures" / "epraccur_sample.csv"

    idx = PracticeIndex(db_file=db_file)
    idx.load_csv(csv_file)  # this must ingest the headerless CSV correctly

    # get() by code
    p = idx.get("W96001")
    assert p is not None
    assert p.name == "CASTLE MEDICAL PRACTICE"
    assert p.town.lower().startswith("swansea")
    assert p.postcode.replace(" ", "") == "SA11AA"

    # search() by name
    res = idx.search(name="castle", status=None, limit=10)
    assert any(r.organisation_code == "W96001" for r in res)

    # status filtering should work when requested
    active = idx.search(name="practice", status="ACTIVE", limit=50)
    assert all((r.status or "").upper() == "ACTIVE" for r in active)
