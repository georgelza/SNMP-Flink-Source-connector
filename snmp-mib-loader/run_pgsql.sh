
python3 mib_ingester.py \
    --mib-directory mibs \
    --db-type postgresql \
    --db-host localhost \
    --db-port 5433 \
    --db-user dbadmin \
    --db-password dbpassword \
    --db-name snmp \
    --db-schema public \
    --tbl-name snmp_oid_metadata
