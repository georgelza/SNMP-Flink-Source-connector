# MVP: Apache Flink SNMP Source connector (scraper)

## Overview

Idea is to build a SNMP source connector that can scrape targets/agents based on IP:port and OIDs using a method.

Executed as either a GET based on OIDs or WALK starting from a root OID.


ToDo: Need to come up with solution how to handle inbound SNMP Traps.


### Executing Polling Job

This can be done by either running the below SQL statement against the created table or by submitting the below job in the job manager.

```SQL
Select * from hive.snmp.snmp_poll_data#;
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


### By:

George

[georgelza@gmail.com](georgelza@gmail.com)

[George on Linkedin](https://www.linkedin.com/in/george-leonard-945b502/)

[George on Medium](https://medium.com/@georgelza)
