# uk-gp-practices

Query UK GP practices ("surgeries") locally using NHS ODS Data Search & Export (DSE) CSV reports.

This package downloads a predefined ODS report (default: `epraccur`), stores it in a local SQLite database, and provides a simple Python API + CLI to query it quickly.

> **Note:** On first use the package will automatically download the latest report from the NHS ODS endpoint. Subsequent queries use the local cache (refreshed daily by default).

## Install

```bash
pip install uk-gp-practices
```

For fuzzy name matching (optional):

```bash
pip install uk-gp-practices[fuzzy]
```

## Python API

```python
from uk_gp_practices import PracticeIndex

# Auto-download the latest data (cached for 24 h)
idx = PracticeIndex.auto_update()

# Look up a single practice by ODS code
practice = idx.get("A81001")
print(practice.name, practice.postcode)

# Search by name / postcode / town / status
results = idx.search(name="castle", status="ACTIVE", limit=10)
for r in results:
    print(r.organisation_code, r.name, r.postcode)

# Context-manager usage
with PracticeIndex.auto_update() as idx:
    print(idx.get("A81001"))
```

## CLI

```bash
# Update the local database
uk-gp update

# Get a single practice (JSON output)
uk-gp get A81001

# Search practices
uk-gp search --name "castle" --status ACTIVE --limit 5
uk-gp search --postcode "SW1A 1AA"
uk-gp search --town "Swansea"
```

## License

MIT
