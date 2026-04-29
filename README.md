# webdb

`webdb` is an AWS CDK app that builds a domain-data pipeline on AWS using S3 Tables (Iceberg).

## Stacks

| Stack | Description |
|---|---|
| `WebdbStorage` | S3 buckets: `enrich`, `insert`, `archive`, `temporary` |
| `WebdbTable` | S3 Tables bucket, namespace, and Iceberg table (`webdb.domains`) |
| `WebdbTransfer` | Scheduled Lambda that copies source data into the enrich bucket |
| `WebdbEnrich` | Event-driven Lambda that enriches domain records with GeoIP data |
| `WebdbInsert` | Event-driven Lambda that ingests JSONL into the Iceberg table |
| `WebdbGithub` | OIDC role for GitHub Actions deployments |

## Insert Pipeline

JSONL files uploaded to `webdb-us-east-2-insert` are processed by an SQS-triggered Lambda:

1. Starts an AWS Glue Spark job for each created object
2. Glue job reads JSONL from the insert bucket and writes into `webdb.domains`
3. Lambda waits for job completion (so SQS retries still work on failures)
4. On success, Lambda gzips and archives the source file to `webdb-us-east-2-archive/{YYYY/MM/DD/}`
5. Lambda deletes the original JSONL object from the insert bucket

## Table Schema

`webdb.domains` Iceberg table (namespace `webdb`, bucket `webdb`):

| Column | Type |
|---|---|
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
- [insert/](insert/) — insert Lambda handler and Glue job script
- [enrich/](enrich/) — enrichment Lambda handler
- [transfer/](transfer/) — transfer Lambda handler
