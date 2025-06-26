# MVP: SNMP Flink Source connector (scraper)

NOTE: This is still early days and allot is changing between all git sync's

Dont even try and use it yet.


## Overview

Idea is to build a SNMP source connector that can scrape agents based on IP:port:Method and OIDs

Executed as either a Get or Walk based on OIDs or Root OID to Walk from.

The MIB files will be loaded into either a MySql. PostgreSQL or REDIS in memory DB for joining/enriching the data.


ToDo: Need to come up with solution how to handle inbound SNMP Traps.


### EXecuting Polling Job

This can be done by either running the below SQL statement against the created table or by submitting the below job in the job manager.

```SQL
Select * from hive_catalog.snmp.snmp_poll_data;
```

OR
NOTE SURE about the next as just pulling data means nothing, and well to use it we need to select from it, which instantiates the job anyhow.
The table parameters define the source agent to fetch data from. **MOST LIKELY TO REMOVE THE BELOW.**

```SHELL
/opt/flink/bin/flink run \
  --detached \
  -c com.snmp.job.SnmpPollingJob \
  /opt/flink/lib/flink/snmp-job-1.0-SNAPSHOT.jar \
  hive_catalog \
  snmp \
  snmp_poll_data
```


## Example commands for Testing

### SNMPWALK

snmpwalk -c passsword 172.16.10.2

### SNMPGET

snmpget -v1 -c password 172.16.10.2 sysDescr.0

snmpget -c passsword 172.16.10.24 HOST-RESOURCES-MIB::hrSystemUptime.0
`HOST-RESOURCES-MIB::hrSystemUptime.0 = Timeticks: (41519049) 4 days, 19:19:50.49`


snmpget -v1 -c passsword 172.16.10.24 sysDescr.0

`SNMPv2-MIB::sysDescr.0 = STRING: TrueNAS-25.04.1. Hardware: x86_64 Intel(R) Core(TM) i5-7400 CPU @ 3.00GHz. Software: Linux 6.12.15-production+truenas (revision #1 SMP PREEMPT_DYNAMIC Mon May 26 13:44:31 UTC 2025)`


### References

- [Gettin Started with snmp](https://www.easysnmp.com/tutorial/getting-snmp-data/)



### Credits:



### By:

George

[georgelza@gmail.com](georgelza@gmail.com)

[George on Linkedin](https://www.linkedin.com/in/george-leonard-945b502/)

[George on Medium](https://medium.com/@georgelza)



mvn install:install-file \
  -Dfile=snmp4j-3.7.0.jar \
  -DgroupId=org.snmp4j \
  -DartifactId=snmp4j \
  -Dversion=3.7.0 \
  -Dpackaging=jar