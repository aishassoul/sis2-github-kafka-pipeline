# Pipeline verification summary

| Stage | Records |
|---|---:|
| collected api records | 600 |
| published to kafka | 600 |
| consumed from kafka | 600 |
| test records added | 4 |
| validated total | 604 |
| valid records | 600 |
| invalid records | 4 |
| records after cleaning | 510 |
| records stored in sqlite | 510 |

## Explanation of differences
* Collected = published = consumed: no message was lost in Kafka.
* Validated = consumed (600) + 4 deliberately invalid test records = 604.
* Invalid = 4 (4 test records + 0 real records); they were quarantined in invalid_records.csv.
* Valid (600) -> after cleaning (510): 90 rows were the same repository returned by several overlapping topic queries and were merged into one row (topics kept in search_topics / repository_topics).
* After cleaning = stored in SQLite: True (510 vs 510).
* Idempotency: second load inserted 0 new rows, table checksum unchanged = True, duplicate ids in DB = 0.

## Automatic checks
| Check | Passed |
|---|---|
| at least 200 collected | yes |
| at least 200 published | yes |
| at least 200 consumed | yes |
| published equals consumed | yes |
| all test records detected | yes |
| valid plus invalid equals total | yes |
| cleaned equals stored | yes |
| idempotent reload | yes |
| no duplicates in db | yes |

**All checks passed: True**
