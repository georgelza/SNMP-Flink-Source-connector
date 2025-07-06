
python3 mib_ingester.py \
    mibs/SNMPv2-MIB.mib \
    --db-type mysql \
    --db-host localhost \ 
    --db-port 3306 \
    --db-user dbadmin \
    --db-password dbpassword \
    --db-name snmp \
    --tbl-name snmp_oid_metadata
