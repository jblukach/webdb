# webdb

`webdb` is an AWS CDK app that builds a domain-data pipeline on AWS with S3, Lambda, Glue, and Athena.

## Stacks

| Stack | Description |
| --- | --- |
| `WebdbDatabase` | DynamoDB table infrastructure, including the shared `possibilities` table for cross-account access from lunker |
| `WebdbStorage` | S3 buckets, Glue Iceberg table, console-ready optimizer IAM role, and Athena workgroup/query resources |
| `WebdbTransfer` | Scheduled Lambda that copies source data into the enrich bucket |
| `WebdbEnrich` | Event-driven Lambda that enriches domain records with GeoIP data |
| `WebdbInsert` | S3/SQS-triggered Python Lambda that starts Glue ingest asynchronously and records execution state for monitor handling |
| `WebdbSearch` | Lambda invoked by WebMonitor that expands permutations and launches Athena UNLOAD asynchronously |
| `WebdbMonitor` | EventBridge-driven Lambda that tracks Athena/Glue completion, performs post-success actions, and sends SNS failure alerts |
| `WebdbSchedule` | EventBridge-scheduled Lambda that scans cross-account permutation SLDs and seeds missing `LUNKER#` entries into `state` and `run` |
| `WebdbCheck` | EventBridge-scheduled Lambda that processes exactly one eligible `state` SLD, counts permutation matches in `possibilities`, and writes totals to `metrics` |
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

`WebdbMonitor` creates the `webdb-<region>-executions` DynamoDB table to track asynchronous Athena and Glue execution status (`PENDING`, `SUCCEEDED`, `FAILED`) with context and 7-day TTL auto-cleanup.

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

`WebdbInsert` ingests `.jsonl` objects from the insert bucket with built-in idempotency and asynchronous completion handling:

1. **Idempotency check** — Queries `processed-objects` DynamoDB table to skip already-processed files (prevents duplicate rows on SQS redelivery or manual reruns).
2. **Glue ingest launch** — Starts a Glue Spark job (up to 60-minute timeout) that normalizes JSONL rows and appends them into the `webdb.domains` Iceberg table.
   - **First-run resilience** — If the table does not exist, the Glue job creates it with Iceberg v2 formatting and year/month/day partitioning, then inserts the batch.
   - **Partition field enforcement** — On subsequent runs, adds partition fields if missing (idempotent via exception handling).
3. **Execution tracking** — Stores Glue `JobRunId` plus source bucket/key and partition date in `webdb-<region>-executions` with `PENDING` status and 7-day TTL. Lambda returns immediately; the original `.jsonl` stays in the insert bucket until Glue completes.
4. **Async archive on success** — `WebdbMonitor` receives the Glue `SUCCEEDED` EventBridge event, re-reads the `.jsonl` from the insert bucket, gzip-compresses it into the archive bucket under `year=YYYY/month=MM/day=DD/<filename>.gz`, deletes the source from the insert bucket, and marks the processed-objects record.
5. **Failure alerting** — On terminal failure states (`FAILED`, `STOPPED`, `TIMEOUT`, `ERROR`, `EXPIRED`), `WebdbMonitor` marks status `FAILED` and emails `hello@lukach.io` via SNS. The original `.jsonl` is left in the insert bucket for inspection and retry.

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

`WebdbSearch` is invoked to process one pending SLD from the local `run` table.

Lookup behavior:

1. Queries one pending SLD from local DynamoDB table `run` using `pk = LUNKER#` and `Limit=1`.
2. Uses that SLD to query permutations from DynamoDB table `permutation` in the lunker account.
3. Requires `DYNAMODB_TABLE` to be set (recommended: full table ARN for cross-account access).
4. Uses key pattern `pk = LUNKER#` and `sk = LUNKER#<sld>#`.
5. Reads the `perm` attribute and normalizes/de-duplicates values.

Query behavior:

1. Builds a term list from the SLD plus all permutations.
2. For SLDs shorter than 5 characters, uses the SLD plus all permutations and matches on the Athena `sld` column with `lower(sld) IN (...)`.
3. For SLDs with length 5 or greater, matches only the SLD against Athena `dns` with contains syntax `lower(dns) LIKE '%<sld>%'`.
4. Runs Athena `UNLOAD` of distinct `dns` values.
5. Stores query execution context in `webdb-<region>-executions` and returns immediately.

Output behavior:

1. Writes compressed text output to the output bucket.
2. Prefix format is timestamped to avoid target directory collisions: `<sld>/YYYY-MM-DD-HH-MM-SS/`.
3. `WebdbMonitor` handles Athena completion events: deletes the processed `run` item on `SUCCEEDED`, writes `000-empty.gz` when a successful query has zero rows, and sends SNS alerts on failure states.

## Monitor Pipeline Behavior

`WebdbMonitor` is invoked by EventBridge state-change rules for both Athena and Glue.

Athena behavior (`SUCCEEDED`, `FAILED`, `CANCELLED` for workgroup `webdb`):

1. Looks up the execution record by `queryExecutionId` in `webdb-<region>-executions`.
2. On `SUCCEEDED`: writes a `000-empty.gz` marker to the output prefix if Athena produced zero result files, then deletes the processed SLD entry from the `run` table.
3. On `FAILED` or `CANCELLED`: marks the record `FAILED` and emails `hello@lukach.io` via SNS with the query ID, state, and reason.

Glue behavior (`SUCCEEDED`, `FAILED`, `STOPPED`, `TIMEOUT`, `ERROR`, `EXPIRED` for job `webdb-<region>-insert-iceberg`):

1. Looks up the execution record by `jobRunId` in `webdb-<region>-executions`.
2. On `SUCCEEDED`: reads the source object from the insert bucket, gzip-archives it to the archive bucket under `year=YYYY/month=MM/day=DD/<filename>.gz`, deletes the source, and marks the `processed-objects` record.
3. On any failure state: marks the record `FAILED` and emails `hello@lukach.io` via SNS with the job name, run ID, state, and message.

## Schedule Pipeline Behavior

`WebdbSchedule` runs every 5 minutes on EventBridge using a cron expression (`*/5 * * * ? *`) and backfills missing search seeds.

Behavior:

1. Queries the lunker-account `permutation` table (via cross-account table ARN) for unique SLD keys using `pk=LUNKER#` and `sk` pattern `LUNKER#<sld>#`.
2. Checks which of those keys already exist in `state` using DynamoDB `BatchGetItem`.
3. For missing SLDs only, writes matching records into both `state` and `run` with:
   - `pk = LUNKER#`
   - `sk = LUNKER#<sld>#`
   - `lastday = yyyy-mm-dd` (UTC)
   - `ttl = now + 365 days`
4. Leaves existing state entries untouched.

## Output Pipeline Behavior

`WebdbOutput` ingests `.gz` objects from the output bucket and performs these actions:

1. S3 `OBJECT_CREATED` events are delivered to an SQS queue.
2. The main queue uses a dead-letter queue after 5 failed receives.
3. The output Lambda downloads each gzip file to `/tmp`.
4. The Lambda decompresses the file and processes one domain per line.
5. Domains are batch-written into DynamoDB table `possibilities`.
6. The local `/tmp` file is deleted after processing.

Current runtime configuration:

- Lambda memory: `2048 MB`
- Lambda ephemeral storage: `1 GiB`
- SQS batch size: `10`
- DynamoDB batch write size: `25`

## Check Pipeline Behavior

`WebdbCheck` runs every 5 minutes on EventBridge and processes exactly one eligible SLD per invocation.

Eligibility and processing flow (query/get only, no scans):

1. Queries `state` with `pk = LUNKER#` and iterates `sk = LUNKER#<sld>#` candidates.
2. Skips rows if `run` already has `pk = LUNKER#`, `sk = LUNKER#<sld>#`.
3. Reads `check` for the same key (`pk = LUNKER#`, `sk = LUNKER#<sld>#`).
4. If the `check` row exists and its `lastday` hour is at least `02` UTC (`yyyy-mm-dd-hh`), the SLD is skipped.
5. If the `check` row does not exist, the SLD remains eligible.
6. For the first SLD that passes all checks, performs a cross-account query to `permutation` with exact key `pk = LUNKER#`, `sk = LUNKER#<sld>#` and reads `perm`.
7. Queries `possibilities` with `pk = LUNKER#` and `begins_with(sk, LUNKER#<sld>#)` to collect domains.
8. Counts occurrences of each permutation across the returned domains.
9. Writes one row per permutation to `metrics` with `pk = LUNKER#`, `sk = LUNKER#<sld>#<perm>#`, and numeric `total`.
10. Writes a completion marker to `check` for `pk = LUNKER#`, `sk = LUNKER#<sld>#`.
11. `check` and `metrics` writes use a 30-day TTL.

### Example Eligibility

For a candidate key `pk = LUNKER#`, `sk = LUNKER#apple#`:

1. If `run` has the same key, it is skipped.
2. If `run` does not have the key and `check` has no row for that key, it is eligible.
3. If `run` does not have the key and `check.lastday` is `2026-05-07-01`, it is eligible (`HH = 01 < 02`).
4. If `run` does not have the key and `check.lastday` is `2026-05-07-02` (or higher hour), it is skipped.

## Lambda Sizing Guidance

All pipeline Lambdas are configured with a `900` second timeout by design to tolerate slow upstream/downstream dependencies (S3, Glue, Athena) without premature retries.

Current stack runtime sizing:

| Lambda | Memory | Ephemeral Storage | Notes |
| --- | --- | --- | --- |
| `transfer` | `512 MB` | `1 GiB` | Downloads/uploads one CSV object via `/tmp` |
| `unzip` | `2048 MB` | `1 GiB` | Reads gzip fully in memory, then decompresses fully in memory |
| `enrich` | `2048 MB` | `1 GiB` | Reads source file into `/tmp`, writes JSONL output, GeoIP lookups |
| `insert` | `2048 MB` | `1 GiB` | Reads JSONL into memory, starts Glue, records execution ID — returns immediately |
| `search` | `512 MB` | `1 GiB` | Builds query, starts Athena UNLOAD, records execution ID — returns immediately |
| `check` | `1024 MB` | `1 GiB` | Processes one eligible state SLD, counts permutation hits, writes metrics/check markers |
| `monitor` | `1024 MB` | `1 GiB` | Handles Athena/Glue completion events: archives source, writes empty markers, sends alerts |
| `output` | `2048 MB` | `1 GiB` | Downloads gzip to `/tmp`, decompresses and batch-writes to DynamoDB |

Recommended right-size baseline (no timeout changes):

1. Keep `enrich`, `output`, `search`, and `monitor` as-is unless CloudWatch metrics show sustained low utilization.
2. Reduce `transfer` ephemeral storage from `1 GiB` to `512 MiB` if source CSV files fit comfortably.
3. Treat `unzip` memory as the primary risk knob (it currently holds both compressed and decompressed payloads in memory at once). If large files are common, increase memory before increasing ephemeral storage.
4. `insert` and `search` no longer hold large payloads in memory beyond job startup — memory and storage are sized for overhead only.

Validation metrics to watch after any sizing change:

- `Max Memory Used` (CloudWatch Lambda Insights or REPORT logs)
- `Duration` and `Timeouts`
- SQS `ApproximateAgeOfOldestMessage` and DLQ message count
- Error rate/retries during high-volume ingest windows

## Repository Layout

- [app.py](app.py) — CDK app entry point
- [webdb/](webdb/) — CDK stack definitions
- [enrich/](enrich/) — enrichment Lambda handler
- [insert/](insert/) — Lambda that launches Glue ingest and Glue Spark job script for Iceberg appends
- [check/](check/) — Lambda that selects one eligible SLD and writes permutation metrics
- [monitor/](monitor/) — Lambda that handles Athena/Glue completion events and SNS failure alerts
- [output/](output/) — Lambda for gzip output ingestion into DynamoDB
- [search/](search/) — Lambda that launches Athena UNLOAD searches
- [transfer/](transfer/) — transfer Lambda handler
- [unzip/](unzip/) — Lambda that decompresses source archives
