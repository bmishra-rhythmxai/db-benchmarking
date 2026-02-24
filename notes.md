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

DSTAT_PID=1 dstat -tcm --custom-pid-cpu-mem
DSTAT_FREESPACE_MOUNTS=/var/lib/postgresql:root,/var/lib/postgresql/data:data dstat -trd -Dtotal,sda,sdh --custom-freespace
DSTAT_FREESPACE_MOUNTS=/var/lib/postgresql:root,/var/lib/postgresql/data:data dstat -tcmrd -Dtotal,sda,sdh --custom-freespace

iostat -mdx 1 | awk '
$1=="Device" {
  printf "%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", "Device", "r/s", "rMB/s", "r_await", "w/s", "wMB/s", "w_await", "%util"
  next
}
$1 ~ /^(sda|sdb)/ {
  printf "%-10s %-10f %-10f %-10f %-10f %-10f %-10f %-10f\n", $1, $2, $3, $6, $8, $9, $12, $23
}'

export TAG=v0.0.10
docker build -t devgwrxacr.azurecr.io/bikash/postgres:${TAG} -f Dockerfile.postgres .
docker push devgwrxacr.azurecr.io/bikash/postgres:${TAG}
sed -i '' "s|devgwrxacr.azurecr.io/bikash/postgres:.*|devgwrxacr.azurecr.io/bikash/postgres:${TAG}|" deployments/postgres.yaml
k apply -f deployments/postgres.yaml

top -b -p 1 -d 1 | grep clickhouse-serv
python main.py --database clickhouse --duration 1800 --batch-size 2 --batch-wait-sec 1.0 --workers 20 --rows-per-second 100 --queries-per-record 2

python main.py --database postgres   --duration 1800 --batch-size 2 --batch-wait-sec 1.0 --workers 20 --rows-per-second 100 --queries-per-record 2

top -b -d 1 | grep "[c]lickhouse-keeper"

export TAG=v0.0.13
docker build -f Dockerfile.k8s -t devgwrxacr.azurecr.io/bikash/db-benchmarking:$TAG .
docker push devgwrxacr.azurecr.io/bikash/db-benchmarking:$TAG
sed -i '' "s|devgwrxacr.azurecr.io/bikash/db-benchmarking:.*|devgwrxacr.azurecr.io/bikash/db-benchmarking:${TAG}|" deployments/load-runner.yaml
k apply -f deployments/load-runner.yaml

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

## Linux perf and nmon on Postgres pods

Perf and nmon are installed via an init container (Debian bookworm-slim; nmon is not in Alpine repos) and available under `/opt/perf/`. The main containers have `CAP_SYS_ADMIN` and `CAP_SYS_PTRACE` so perf can run.

**perf (primary or replica):**
```bash
k exec -it deploy/postgres-primary -n clickhouse -c postgres -- /opt/perf/perf record -F 99 -g -p 1 -- sleep 30
# or replica
k exec -it deploy/postgres-replica -n clickhouse -c postgres -- /opt/perf/perf record -F 99 -g -p 1 -- sleep 30
```

Copy out the `perf.data` and run `perf report` locally, or run `perf report` in the pod. If you see kernel version mismatches, consider using the node’s perf via a hostPath volume (mount host’s `/usr/bin/perf` and optionally `/lib/modules`).

**nmon (interactive):**
```bash
k exec -it deploy/postgres-primary -n clickhouse -c postgres -- /opt/perf/nmon
```
Press single-letter keys for views (c=cpu, m=mem, d=disk, n=network, etc.); `q` to quit.
