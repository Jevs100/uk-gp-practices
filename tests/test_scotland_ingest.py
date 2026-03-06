from __future__ import annotations

from pathlib import Path

from uk_gp_practices.index import PracticeIndex
from uk_gp_practices.sources.scotland import ScotlandSource


def test_scotland_ingest_and_search(tmp_path: Path) -> None:
    db_file = tmp_path / "practices.sqlite3"
    csv_file = Path(__file__).parent / "fixtures" / "scotland_sample.csv"

    source = ScotlandSource()
    idx = PracticeIndex(db_file=db_file)
    idx.load_source(csv_file, source=source)

    # get() by code
    p = idx.get("S10001")
    assert p is not None
    assert p.name == "MARYHILL HEALTH CENTRE"
    assert p.nation == "scotland"

    # town falls back from AddressLine3 (empty) to nothing for S10005 — AddressLine1 used
    woodside = idx.get("S10005")
    assert woodside is not None
    assert woodside.town == "BARR STREET"

    # AddressLine3 present → used as town
    assert p.town == "GLASGOW"

    # postcode normalised
    if p.postcode:
        assert p.postcode.replace(" ", "") == "G209DR"

    # status is None for all Scotland records
    assert p.status is None

    # search() by name
    results = idx.search(name="health centre", limit=10)
    codes = {r.organisation_code for r in results}
    assert "S10001" in codes
    assert "S10002" in codes

    # nation filter
    scotland_results = idx.search(nation="scotland", limit=10)
    assert all(r.nation == "scotland" for r in scotland_results)
    assert len(scotland_results) == 5
