REQ-1:
Database size 2 GB

REQ-2:
200-400 message per minute means 420 / 60 = 7 message per second

To accommodate load spikes instead of uniform 7 message per second
100 message per second x 200 KB = 20,000 KB/s = 20 MB/s

REQ-3:
100-200 KBs per message
200 KB per message

Throughput calculation:
15 Mins = 900 seconds x 20 MB = 18000 MB = 20 GB
Considering the indexing and other storages, disk required 50 GB

Standard vs Premium disks (To be tested)

### Controlling IOPS and throughput (Azure)

Use a distinct StorageClass per performance tier (Premium SSD v2). Set your PVC's `storageClassName` to the desired class. See `deployments/azure-storage-class-iops.yaml` for examples: `managed-csi-premiumv2-2k-200`, `managed-csi-premiumv2-5k-400`. Add more StorageClasses for other IOPS/throughput combinations. Min: 2 IOPS/GiB, 0.032 MB/s per GiB.

nmon -f -s1 -c120

python3 /scripts/system_monitor.py --no-disk
python3 /scripts/system_monitor.py --no-cpu --no-mem --no-net

iostat -mdx 1 | awk '
$1=="Device" {
  printf "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", "Device", "r/s", "rMB/s", "r_await", "w/s", "wMB/s", "w_await", "%util"
  next
}
$1 ~ /^(sda|sdb)/ {
  printf "%-10s %-10f %-10f %-10f %-10f %-10f %-10f %-10f\n", $1, $2, $3, $6, $8, $9, $12, $23
}'

TAG=$(~/source/az-cli/.venv/bin/az acr repository show-tags --name devgwrxacr --repository bikash/postgres --orderby time_desc --top 1 --output tsv | awk -F. '{printf "%s.%s.%d", $1, $2, $3+1}')
docker build -t devgwrxacr.azurecr.io/bikash/postgres:${TAG} -f Dockerfile.postgres .
docker push devgwrxacr.azurecr.io/bikash/postgres:${TAG}
sed -i '' "s|devgwrxacr.azurecr.io/bikash/postgres:.*|devgwrxacr.azurecr.io/bikash/postgres:${TAG}|" deployments/postgres.yaml
k apply -f deployments/postgres.yaml

top -b -p 1 -d 1 | grep clickhouse-serv
python main.py --database clickhouse --duration 21600 --batch-size 2 --batch-wait-sec 1.0 --process 4 --workers 5 --rows-per-second 200 --queries-per-record 2

python main.py --database postgres   --duration 21600 --batch-size 2 --batch-wait-sec 1.0 --process 4 --workers 5 --rows-per-second 200 --queries-per-record 2

./loadrunner --database postgres   --duration 21600 --batch-size 2 --batch-wait-sec 1.0 --workers 20 --rows-per-second 200 --queries-per-record 2


top -b -d 1 | grep "[c]lickhouse-keeper"

TAG=$(~/source/az-cli/.venv/bin/az acr repository show-tags --name devgwrxacr --repository bikash/db-benchmarking --orderby time_desc --top 1 --output tsv | awk -F. '{printf "%s.%s.%d", $1, $2, $3+1}')
docker build -f Dockerfile.k8s -t devgwrxacr.azurecr.io/bikash/db-benchmarking:$TAG .
docker push devgwrxacr.azurecr.io/bikash/db-benchmarking:$TAG
sed -i '' "s|devgwrxacr.azurecr.io/bikash/db-benchmarking:.*|devgwrxacr.azurecr.io/bikash/db-benchmarking:${TAG}|" deployments/load-runner.yaml
k apply -f deployments/load-runner.yaml

TAG=$(~/source/az-cli/.venv/bin/az acr repository show-tags --name devgwrxacr --repository bikash/db-benchmarking-go --orderby time_desc --top 1 --output tsv | awk -F. '{printf "%s.%s.%d", $1, $2, $3+1}')
docker build -f Dockerfile.k8s.go -t devgwrxacr.azurecr.io/bikash/db-benchmarking-go:$TAG .
docker push devgwrxacr.azurecr.io/bikash/db-benchmarking-go:$TAG
sed -i '' "s|devgwrxacr.azurecr.io/bikash/db-benchmarking-go:.*|devgwrxacr.azurecr.io/bikash/db-benchmarking-go:${TAG}|" deployments/load-runner-go.yaml
k apply -f deployments/load-runner-go.yaml

-- drop table hl7_messages on cluster default sync
-- drop table hl7_messages_local sync
-- select * from default.hl7_messages final where MEDICAL_RECORD_NUMBER = 'MRN-0000000599';
-- select FHIR_ID from hl7_messages_local order by FHIR_ID
-- select count(*) from hl7_messages
-- select count(*) from hl7_messages_local
-- select * from system.zookeeper settings allow_unrestricted_reads_from_keeper = 'true'
-- select path from system.zookeeper group by path order by path settings allow_unrestricted_reads_from_keeper = 'true'
-- select * from system.zookeeper where path = '/clickhouse/tables/01/hl7_messages_local/replicas'
-- system drop replica 'chi-clickhouse-dev-cluster-0-0' from zkpath '/clickhouse/tables/01/hl7_messages_local'
-- system drop replica 'chi-clickhouse-dev-cluster-0-1' from zkpath '/clickhouse/tables/01/hl7_messages_local'
-- select * from system.replicas
-- show tables
-- show tables from system
-- select * from system.distributed_ddl_queue settings query_cache_system_table_handling = 'ignore';
-- select * from system.distribution_queue settings query_cache_system_table_handling = 'ignore';
-- select * from system.replication_queue settings query_cache_system_table_handling = 'ignore';
-- select * from system.database_engines settings query_cache_system_table_handling = 'ignore';
-- select * from system.table_engines order by name settings query_cache_system_table_handling = 'ignore';
-- select * from system.background_schedule_pool settings query_cache_system_table_handling = 'ignore';
-- select * from system.settings order by name settings query_cache_system_table_handling = 'ignore';
-- select * from system.settings_profiles settings query_cache_system_table_handling = 'ignore';
-- select * from system.settings_profile_elements settings query_cache_system_table_handling = 'ignore';
-- select * from system.merge_tree_settings order by name settings query_cache_system_table_handling = 'ignore';
-- select * from system.replicated_merge_tree_settings order by name settings query_cache_system_table_handling = 'ignore';
-- select * from system.server_settings order by name settings query_cache_system_table_handling = 'ignore';

-- SELECT * FROM pg_compression;
-- SHOW default_toast_compression;
-- SELECT reltoastrelid::regclass FROM pg_class WHERE relname = 'hl7_messages';
-- SELECT pg_size_pretty(pg_total_relation_size('pg_toast.pg_toast_16389'));
-- SELECT length(source) AS logical_size, pg_column_size(source) AS physical_size FROM hl7_messages LIMIT 10;
-- SELECT sum(length(source)) AS total_logical, sum(pg_column_size(source)) AS total_physical, round(sum(pg_column_size(source))::numeric / sum(length(source)) * 100, 2) AS percent_of_original FROM hl7_messages;
-- SELECT attstorage, attcompression FROM pg_attribute WHERE attrelid = 'hl7_messages'::regclass AND attname = 'source';
-- select version();
-- select * from hl7_messages limit 10;

--   SELECT a.attname,
--          CASE a.attstorage
--            WHEN 'p' THEN 'PLAIN'
--            WHEN 'e' THEN 'EXTERNAL'
--            WHEN 'm' THEN 'MAIN'
--            WHEN 'x' THEN 'EXTENDED'
--          END AS storage,
--          a.attcompression
--   FROM pg_attribute a
--   JOIN pg_class c ON c.oid = a.attrelid
--   JOIN pg_namespace n ON n.oid = c.relnamespace
--   WHERE c.relname = 'hl7_messages'
--     AND n.nspname = 'public'
--     AND a.attname = 'source'
--     AND NOT a.attisdropped;

--   SELECT pg_column_compression(source) AS compression_used
--   FROM hl7_messages
--   LIMIT 1;

--   SELECT pg_column_toast_chunk_id(source) AS toast_chunk_id
--   FROM hl7_messages
--   LIMIT 1;

--update hl7_messages set source = source;
