from __future__ import annotations


from uk_gp_practices.normalise import normalize_name, normalize_postcode


class TestNormalizePostcode:
    def test_basic(self):
        assert normalize_postcode("SW1A 1AA") == "SW1A1AA"

    def test_extra_whitespace(self):
        assert normalize_postcode("  sw1a  1aa ") == "SW1A1AA"

    def test_lowercase(self):
        assert normalize_postcode("sw1a1aa") == "SW1A1AA"

    def test_none(self):
        assert normalize_postcode(None) is None

    def test_empty_string(self):
        assert normalize_postcode("") is None

    def test_whitespace_only(self):
        assert normalize_postcode("   ") is None


class TestNormalizeName:
    def test_basic(self):
        assert normalize_name("Castle Medical") == "castle medical"

    def test_extra_whitespace(self):
        assert normalize_name("  Castle   Medical  ") == "castle medical"

    def test_none(self):
        assert normalize_name(None) is None

    def test_empty_string(self):
        assert normalize_name("") is None

    def test_whitespace_only(self):
        assert normalize_name("   ") is None

    def test_already_lowercase(self):
        assert normalize_name("castle medical") == "castle medical"
