# webdb

`webdb` is an AWS CDK app that builds a domain-data pipeline on AWS using S3 Tables (Iceberg).

## Stacks

| Stack | Description |
| --- | --- |
| `WebdbStorage` | S3 buckets: `enrich`, `insert`, `archive`, `temporary` |
| `WebdbTransfer` | Scheduled Lambda that copies source data into the enrich bucket |
| `WebdbEnrich` | Event-driven Lambda that enriches domain records with GeoIP data |
| `WebdbGithub` | OIDC role for GitHub Actions deployments |

## Table Schema

`webdb.domains` Iceberg table (namespace `webdb`, bucket `webdb`):

| Column | Type |
| --- | --- |
| `dns` | string |
| `ns` | string |
| `ip` | string |
| `co` | string |
| `web` | string |
| `eml` | string |
| `hold` | string |
| `tel` | long |
| `rank` | long |
| `ts` | string |
| `id` | string |
| `sld` | string |
| `tld` | string |
| `asn` | long |

## Prerequisites

- Python 3.12+
- AWS CDK v2
- AWS credentials configured with a `db` profile

```bash
pip install -r requirements.txt
```

## Deploy

```bash
cdk deploy --profile db --all
```

```bash
cdk diff --profile db --all
```

## Repository Layout

- [app.py](app.py) — CDK app entry point
- [webdb/](webdb/) — CDK stack definitions
- [enrich/](enrich/) — enrichment Lambda handler
- [transfer/](transfer/) — transfer Lambda handler
