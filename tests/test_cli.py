from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from uk_gp_practices.cli import app
from uk_gp_practices.index import PracticeIndex

runner = CliRunner()


def _make_index(tmp_path: Path) -> PracticeIndex:
    """Create a PracticeIndex with sample data loaded."""
    db_file = tmp_path / "practices.sqlite3"
    csv_file = Path(__file__).parent / "fixtures" / "epraccur_sample.csv"
    idx = PracticeIndex(db_file=db_file)
    idx.load_csv(csv_file)
    return idx


class TestGetCommand:
    def test_found(self, tmp_path: Path):
        idx = _make_index(tmp_path)
        with patch.object(PracticeIndex, "auto_update", return_value=idx):
            result = runner.invoke(app, ["get", "W96001"])
        assert result.exit_code == 0
        assert "CASTLE MEDICAL PRACTICE" in result.output

    def test_not_found(self, tmp_path: Path):
        idx = _make_index(tmp_path)
        with patch.object(PracticeIndex, "auto_update", return_value=idx):
            result = runner.invoke(app, ["get", "ZZZZZ"])
        assert result.exit_code == 1
        assert "Practice not found" in result.output


class TestSearchCommand:
    def test_search_by_name(self, tmp_path: Path):
        idx = _make_index(tmp_path)
        with patch.object(PracticeIndex, "auto_update", return_value=idx):
            result = runner.invoke(app, ["search", "--name", "castle"])
        assert result.exit_code == 0
        assert "CASTLE MEDICAL PRACTICE" in result.output

    def test_search_no_results(self, tmp_path: Path):
        idx = _make_index(tmp_path)
        with patch.object(PracticeIndex, "auto_update", return_value=idx):
            result = runner.invoke(app, ["search", "--name", "nonexistent999"])
        assert result.exit_code == 0
        assert result.output.strip() == "[]"
