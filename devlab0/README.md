# Environment configuration and deployment

## in Window #1

In root Directory

1. ```mvn clean package```

2. copy snmp-source/target/snmp-source-1.0-SNAPSHOT-shaded.jar => devlab0/conf/flink/lib/flink

## in Window #2

1. cd <root>/devlab0

2. make run

## in Window #3

1. cd <root>/devlab0

2. make fsql

3. copy paste <root>/devlab0/creFlinkflows/1.0.creCat.sql

4. Edit root>/devlab0/creFlinkflows/2.1.snmp_poll_data_get.sql
   
   Replace target ip's in create statements to point to snmp agent ip.

5. copy paste  <root>/devlab0/creFlinkflows/2.1.snmp_poll_data_get.sql

   This needs to be done table by table, can't be a copy/paste of all.

6. select * from snmp_poll_data1;

Should at this point be getting data from a single snmp agent every 5 seconds for a single oid

6. select * from snmp_poll_data2;

Should at this point be getting data from a single snmp agent every 5 seconds for a multiple oid

7. select * from snmp_poll_data3;

Should at this point be getting data from a two snmp agents every 5 seconds for a multiple oid

## in Window #4

docker-compose logs -f