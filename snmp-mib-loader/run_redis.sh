. ./.pws

python3 mib_ingester.py \
    mibs/SNMPv2-MIB.mib \
    --db-type redis \
    --db-host localhost \
    --db-port 6379 \
    --db-name 0\
    --redis-key-prefix oid
