from __future__ import annotations

from pathlib import Path
from unittest.mock import patch, MagicMock

import httpx
import pytest

from uk_gp_practices.download import download_report


def _mock_response(content: bytes = b"code,name\nA1,Test", status_code: int = 200):
    resp = MagicMock(spec=httpx.Response)
    resp.content = content
    resp.status_code = status_code
    resp.url = "https://example.com/api/getReport?report=epraccur"
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


class TestDownloadReport:
    def test_success(self, tmp_path: Path):
        dest = tmp_path / "report.csv"
        mock_resp = _mock_response()

        with patch("uk_gp_practices.download.httpx.Client") as mock_client_cls:
            client = MagicMock()
            client.get.return_value = mock_resp
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

            result = download_report(report="epraccur", dest=dest)

        assert result.report == "epraccur"
        assert result.path == dest
        assert result.bytes_written > 0
        assert dest.exists()

    def test_http_error_raises(self, tmp_path: Path):
        dest = tmp_path / "report.csv"
        mock_resp = _mock_response(status_code=500)

        with patch("uk_gp_practices.download.httpx.Client") as mock_client_cls:
            client = MagicMock()
            client.get.return_value = mock_resp
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

            with pytest.raises(RuntimeError, match="Failed to download"):
                download_report(report="epraccur", dest=dest)

    def test_all_retries_exhausted(self, tmp_path: Path):
        dest = tmp_path / "report.csv"

        with patch("uk_gp_practices.download.httpx.Client") as mock_client_cls:
            client = MagicMock()
            client.get.side_effect = httpx.ConnectError("refused")
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

            with patch("uk_gp_practices.download.time.sleep"):
                with pytest.raises(RuntimeError, match="Failed to download"):
                    download_report(report="epraccur", dest=dest, retries=1, backoff_seconds=0)

    def test_env_override(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        dest = tmp_path / "report.csv"
        monkeypatch.setenv("UK_GP_PRACTICES_DSE_URL", "https://custom.example.com/api")
        mock_resp = _mock_response()

        with patch("uk_gp_practices.download.httpx.Client") as mock_client_cls:
            client = MagicMock()
            client.get.return_value = mock_resp
            mock_client_cls.return_value.__enter__ = MagicMock(return_value=client)
            mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

            result = download_report(report="epraccur", dest=dest)

        client.get.assert_called_once_with("https://custom.example.com/api", params={"report": "epraccur"})
        assert result.bytes_written > 0
