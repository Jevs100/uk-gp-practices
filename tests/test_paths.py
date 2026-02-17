from __future__ import annotations

from uk_gp_practices.paths import csv_path


class TestCsvPathSanitization:
    def test_normal_report(self):
        p = csv_path("epraccur")
        assert p.name == "epraccur.csv"

    def test_path_traversal_slashes(self):
        p = csv_path("../../etc/passwd")
        assert "/" not in p.name.replace(".csv", "")
        assert "\\" not in p.name.replace(".csv", "")

    def test_dotdot(self):
        p = csv_path("../secret")
        assert ".." not in p.name
