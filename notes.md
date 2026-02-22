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


top -b -p 1 -d 1 | grep clickhouse-serv
python main.py --database clickhouse --duration 10 --batch-size 2 --workers 20 --rows-per-second 10 --queries-per-record 2 --query-delay 100

python main.py --database postgres --duration 60 --batch-size 2 --workers 20 --rows-per-second 100 --queries-per-record 2

top -b -d 1 | grep "[c]lickhouse-keeper"

export TAG=v0.0.11
docker build -f Dockerfile.k8s -t devgwrxacr.azurecr.io/bikash/db-benchmarking:$TAG .
docker push devgwrxacr.azurecr.io/bikash/db-benchmarking:$TAG

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
