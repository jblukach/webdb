# webdb

`webdb` is an AWS CDK app that builds a domain-data pipeline on AWS with S3, Lambda, Glue, and Athena.

## Stacks

| Stack | Description |
| --- | --- |
| `WebdbDatabase` | DynamoDB table infrastructure, including the shared `possibilities` table for cross-account access from lunker |
| `WebdbStorage` | S3 buckets, Glue Iceberg table, console-ready optimizer IAM role, and Athena workgroup/query resources |
| `WebdbTransfer` | Scheduled Lambda that copies source data into the enrich bucket |
| `WebdbEnrich` | Event-driven Lambda that enriches domain records with GeoIP data |
| `WebdbInsert` | S3/SQS-triggered Python Lambda that starts a Glue Spark job to append into Iceberg, then archives gzip JSONL |
| `WebdbSearch` | Lambda invoked by WebMonitor that expands permutations from shared DynamoDB and runs Athena UNLOAD of unique domain matches |
| `WebdbOutput` | S3/SQS-triggered Lambda that ingests gzip output files and batch-writes discovered domains into DynamoDB |
| `WebdbGithub` | OIDC role for GitHub Actions deployments |

## Table Schema

`webdb.domains` Glue Iceberg v2 table (with `year`,`month`,`day` date columns).
The Glue ingest job enforces Iceberg partition fields on these columns.

| Column | Type |
| --- | --- |
| `dns` | string |
| `ns` | array(string) |
| `ip` | string |
| `co` | string |
| `web` | string |
| `eml` | string |
| `hold` | string |
| `tel` | bigint |
| `rank` | bigint |
| `ts` | string |
| `id` | string |
| `sld` | string |
| `tld` | string |
| `asn` | bigint |
| `year` | int |
| `month` | int |
| `day` | int |

## DynamoDB

`WebdbDatabase` creates the `possibilities` DynamoDB table in `us-east-2` with the following shape:

| Attribute | Purpose |
| --- | --- |
| `pk` | partition key |
| `sk` | sort key |

`WebdbInsert` creates the `processed-objects` DynamoDB table in the current region to track ingest idempotency:

| Attribute | Purpose |
| --- | --- |
| `pk` | `<bucket>#<key>` composite key |
| `processed_at` | Unix timestamp of processing |
| `ttl` | Auto-expiration (30 days) |

`WebdbOutput` writes items with this layout:

| Attribute | Value |
| --- | --- |
| `pk` | `LUNKER#` |
| `sk` | `LUNKER#<search>#<domain>#` |
| `domain` | fully qualified domain name |
| `search` | first folder from the S3 object key |
| `sld` | second-level domain |
| `tbl` | `possibilities` |
| `tld` | top-level domain |
| `ttl` | Unix epoch expiration set to 30 days |

The table uses on-demand billing, point-in-time recovery, and deletion protection. A DynamoDB resource policy grants the lunker account access to `DescribeTable`, `GetItem`, and `Query`.

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

Note: CDK deploy provisions the Iceberg optimization IAM role, but compaction/retention/orphan-file optimization settings are configured in the AWS Glue console.

## Athena Performance

- Always filter by partitions (`year`, `month`, `day`) to reduce scanned data.
- Prefer selective columns over `SELECT *`.
- Use date-pruned queries for interactive searches.

Example:

```sql
SELECT dns, ip, rank, ts, asn
FROM webdb.domains
WHERE year = 2026
AND month = 4
AND day = 30
ORDER BY ts DESC
LIMIT 100;
```

## Iceberg Optimization Role

`WebdbStorage` creates one IAM role for AWS Glue Iceberg table optimization in the AWS console.

- Output `webdb-iceberg-optimizer-role-arn`: full role ARN
- Output `webdb-iceberg-optimizer-role-name`: IAM role name
- Trust principal: `glue.amazonaws.com`
- Included access: S3 read/write for `webdb-<region>-database`, Glue catalog/database/table metadata permissions for `webdb.domains`, and Lake Formation `GetDataAccess`.

Console setup:

1. Open the `WebdbStorage` stack in CloudFormation and copy either output.
2. In Glue, open table `webdb.domains` and go to table optimizations.
3. Configure compaction/snapshot retention/orphan file cleanup and provide this role.

Use the ARN when the console/API asks for Role ARN. Use the role name when the UI asks you to select by name.

## Insert Pipeline Behavior

`WebdbInsert` ingests `.jsonl` objects from the insert bucket with built-in idempotency and resilience:

1. **Idempotency check** — Queries `processed-objects` DynamoDB table to skip already-processed files (prevents duplicate rows on SQS redelivery or manual reruns).
2. **Glue ingest** — Starts a Glue Spark job that normalizes JSONL rows and appends them into the `webdb.domains` Iceberg table.
   - **First-run resilience** — If the table does not exist, the Glue job creates it with Iceberg v2 formatting and year/month/day partitioning, then inserts the batch.
   - **Partition field enforcement** — On subsequent runs, adds partition fields if missing (idempotent via exception handling).
3. **Archive & cleanup** — After Glue succeeds, writes the original payload as gzip JSONL to the archive bucket and deletes the source object.
4. **Idempotency record** — Records the file as processed in DynamoDB with a 30-day TTL for cleanup.

Partition date resolution order:

1. Parse `YYYYMMDD`, `YYYY-MM-DD`, or `YYYY_MM_DD` from the beginning of the source filename.
2. Fallback to the first record `ts` field (`YYYY-MM-DD`).
3. Fallback to current UTC date.

Current object key layout:

- Archive gzip JSONL: `year=YYYY/month=MM/day=DD/<source-filename>.gz`

Idempotency guarantees:

- Duplicate files processed within a 30-day window are silently skipped (no Glue job triggered, no duplicates inserted).
- Processed file records auto-expire after 30 days, allowing the same file to be re-ingested if needed.
- If `PROCESSED_OBJECTS_TABLE` environment variable is not set, idempotency is disabled (graceful degradation).

## Splitting Large Source Files

If a source export is too large to handle comfortably as a single object, split it into smaller chunks before loading it into the webdb data lake. This makes uploads and downstream processing easier to manage.

Example: split a large CSV into 1,000,000-line chunks with a date-stamped prefix:

```bash
split -l 1000000 domains-detailed.csv 2026-05-01-domains-detailed-
```

This produces files such as `2026-05-01-domains-detailed-aa`, `2026-05-01-domains-detailed-ab`, and so on, which can then be uploaded or processed in smaller batches.

## Search Pipeline Behavior

`WebdbSearch` is invoked by WebMonitor with a payload containing `Item` (the SLD).

Lookup behavior:

1. Reads permutations from DynamoDB table `permutation` in the lunker account.
2. Requires `DYNAMODB_TABLE` to be set (recommended: full table ARN for cross-account access).
3. Uses key pattern `pk = LUNKER#` and `sk = LUNKER#<sld>#`.
4. Reads the `perm` attribute and normalizes/de-duplicates values.

Query behavior:

1. Builds a term list from the SLD plus all permutations.
2. For SLDs shorter than 5 characters, queries Athena with `lower(sld) IN (...)` using the SLD plus all permutations.
3. For longer SLDs, expands terms into Athena `LIKE` clauses joined by `OR`, using `lower(dns) LIKE '%term%' ESCAPE '#'`.
4. Runs Athena `UNLOAD` of distinct `dns` values.

Output behavior:

1. Writes compressed text output to the output bucket.
2. Prefix format is timestamped to avoid target directory collisions: `<sld>/YYYY-MM-DD-HH-MM-SS/`.

## Output Pipeline Behavior

`WebdbOutput` ingests `.gz` objects from the output bucket and performs these actions:

1. S3 `OBJECT_CREATED` events are delivered to an SQS queue.
2. The main queue uses a dead-letter queue after 5 failed receives.
3. The output Lambda downloads each gzip file to `/tmp`.
4. The Lambda decompresses the file and processes one domain per line.
5. Domains are batch-written into DynamoDB table `possibilities`.
6. The local `/tmp` file is deleted after processing.

Current runtime configuration:

- Lambda memory: `1024 MB`
- Lambda ephemeral storage: `1 GiB`
- SQS batch size: `10`
- DynamoDB batch write size: `25`

## Repository Layout

- [app.py](app.py) — CDK app entry point
- [webdb/](webdb/) — CDK stack definitions
- [enrich/](enrich/) — enrichment Lambda handler
- [insert/](insert/) — Python Lambda handler and Glue Spark ingest job for Iceberg appends
- [output/](output/) — Lambda for gzip output ingestion into DynamoDB
- [transfer/](transfer/) — transfer Lambda handler
