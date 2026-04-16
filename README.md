# uk-gp-practices

Query UK GP practices ("surgeries") locally using public NHS data sources.

This package downloads GP practice data, stores it in a local SQLite database, and provides a simple Python API + CLI to query it quickly.

Currently supported sources:

- England and Wales: NHS ODS Data Search & Export `epraccur` report
- Scotland: Public Health Scotland / NHS Scotland Open Data GP practice contact details

> **Note:** On first use the package will automatically download the latest data from the configured sources. Subsequent queries use the local cache, refreshed daily by default.

## Install

```bash
pip install uk-gp-practices
```

## Python API

```python
from uk_gp_practices import PracticeIndex

# Auto-download the latest data for all supported sources (cached for 24 h)
idx = PracticeIndex.auto_update()

# Look up a single practice by ODS code
practice = idx.get("A81001")
if practice:
    print(practice.name, practice.postcode, practice.nation)

# Search by name / postcode / town / status / nation
results = idx.search(name="castle", status="ACTIVE", nation="england", limit=10)
for r in results:
    print(r.organisation_code, r.name, r.postcode, r.nation)

# Context-manager usage
with PracticeIndex.auto_update() as idx:
    print(idx.get("A81001"))
```

### Loading Specific Sources

```python
from datetime import timedelta

from uk_gp_practices import PracticeIndex
from uk_gp_practices.sources import EnglandSource, ScotlandSource

# Only download/update Scotland.
idx = PracticeIndex.auto_update(sources=[ScotlandSource()])

# Use a different cache freshness window.
idx = PracticeIndex.auto_update(max_age=timedelta(hours=6))

# Load a local source CSV into a chosen database file.
idx = PracticeIndex(db_file="practices.sqlite3")
idx.load_source("scotland.csv", source=ScotlandSource())

# Backward-compatible helper for local epraccur CSV files.
idx.load_csv("epraccur.csv")
```

## CLI

```bash
# Update the local database for all supported sources
uk-gp update

# Get a single practice (JSON output)
uk-gp get A81001

# Search practices
uk-gp search --name "castle" --status ACTIVE --limit 5
uk-gp search --postcode "SW1A 1AA"
uk-gp search --town "Swansea"
uk-gp search --nation scotland --name "health centre"
```

## Cache And Configuration

Data is cached in the platform user cache directory, for example `~/.cache/uk-gp-practices/` on Linux.

The default SQLite database is stored as `practices.sqlite3` in that cache directory. Downloaded source CSV files are cached alongside it.

Environment variables:

- `UK_GP_PRACTICES_DSE_URL`: override the NHS ODS Data Search & Export endpoint used for the `epraccur` report.
- `UK_GP_PRACTICES_SCOTLAND_URL`: override the Scotland CSV URL. When unset, the package discovers the latest CSV from the NHS Scotland Open Data CKAN API.

## License

MIT
