# Environment configuration and deployment

## in Window #1

In root Directory

1. ```mvn clean package```

2. copy `./snmp-source/target/snmp-source-1.0-SNAPSHOT-shaded.jar` => `./devlab0/conf/flink/lib/flink`

## in Window #2

1. `cd ./devlab0`

2. `make run`

## in Window #3

1. cd `./devlab0`

2. `make fsql`

3. copy paste `./devlab0/creFlinkflows/1.0.creCat.sql`

4. Edit `./devlab0/creFlinkflows/2.1.snmp_poll_data_get.sql`
   
   Replace target ip's in create statements to point to snmp agent ip.

5. In fsql terminal
   
   `use default_catalog.snmp;`

6. In fsql terminal
   
   copy paste  `./devlab0/creFlinkflows/2.1.snmp_poll_data_get.sql`

   **This needs to be done table by table, can't be a copy/paste of all.**

7. In fsql terminal
   
   `select * from snmp_poll_data1;`

   This should at this point be getting data from a single snmp agent every 5 seconds for a single oid

8. In fsql terminal
   
   `select * from snmp_poll_data2;`

   Should at this point be getting data from a single snmp agent every 5 seconds for a multiple oid

9. In fsql terminal
    
   `select * from snmp_poll_data3;`

   Should at this point be getting data from a two snmp agents every 5 seconds for a multiple oid


## in Window #4

`docker-compose logs -f `

## in Window #5

`docker-compose logs -f |grep SnmpSourceReader`


## Notes

- Allot of the code from `SnmpsourceReader.java` has been decorated with `System.out.println` commands that contains `SnmpSourceReader`.

- Some of the other files similarly... file name is what can be grepped for.

- The entire process is suppose to start from `snmp-source/src/main/java/com/snmp/source/SnmpDynamicTableSourceFactory`. This defines the source connector, it's made discoverable via the `snmp-source/src/main/resource/META-INF/services/org.apache.flink.table.factories.Factory` file and it's contents: `com.snmp.source.SnmpDynamicTableSourceFactory`. Apache Flink goes looking for this as a plugin, internal bits...

- When a `select * from snmp_poll_data#;` statement is run it is kicks off as a Apache Flink job by executing `snmp-source/src/main/java/com/snmp/job/SnmpPollingJob` which initiates `SnmpSourceReader`. The reader exposes the collected data as a dynamic stream into our `snmp_poll_data#` table.
