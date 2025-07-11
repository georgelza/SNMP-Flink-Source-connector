# MVP: Apache Flink SNMP Source connector (scraper)

## Overview

The basic idea is to build a **Apache Flink SNMP source connector** that can scrape/poll SNMP agents (defined as targets using **Apache Flink SQL** syntax as a Create table statement) 

The statement will define the "static" structure, combined with the various variables defining what where and how to poll.

The **main source connector** is located under `snmp-source`, while the **MIB loader** is located under `snmp-mib-loader`.
`snmp-mib-source` is based on java where as the `snmp-mib-loader` is a small Python program.


NOTE: Full disclosure, this is a project to show whats possible, it's definitely not complete...


This source connector currently allows the following scenarios.

- TableExamples/snmp_poll_data_get.sql
  
1. Single agent, single OID using the GET method.

2. Single agent, multiple OID's using the GET method.

3. Multiple agents, single OID. using the GET method.

4. Multiple agents, multiple OID's using the GET method.

- TableExamples/snmp_poll_data_walk.sql

5. Single agent, single root OID using the WALK method.

6. Multiple agents, single root OID using the WALK method.

All of the above is using **SNMPv1** and **SNMPv2c** protocol using a common **snmp.community-string** per defined agent as specified via the table create.

- TableExamples/snmp_poll_data_auth.sql

7. Examples as per above but for **SNMPv3** with associated additional information/parameters.


[Apache Flink SNMP Source Connector](https://github.com/georgelza/SNMP-Flink-Source-connector.git)    


**NOTE**: this Connector's output is to be joined with snmp oid based information extracted from mib files.
I've written a python program to do that, it was originally part of this project but have decided to rather extract it into it's own GIT Repo:
[SNMP OID Mib Parser](https://github.com/georgelza/SNMP-MIB-Parser.git)



### SNMPGET Example

snmpget -v1 -c password 172.16.10.2 sysDescr.0

snmpget -c passsword 172.16.10.24 HOST-RESOURCES-MIB::hrSystemUptime.0
`HOST-RESOURCES-MIB::hrSystemUptime.0 = Timeticks: (41519049) 4 days, 19:19:50.49`


snmpget -v1 -c passsword 172.16.10.24 sysDescr.0

`SNMPv2-MIB::sysDescr.0 = STRING: TrueNAS-25.04.1. Hardware: x86_64 Intel(R) Core(TM) i5-7400 CPU @ 3.00GHz. Software: Linux 6.12.15-production+truenas (revision #1 SMP PREEMPT_DYNAMIC Mon May 26 13:44:31 UTC 2025)`


### SNMPWALK Example

snmpwalk -v1 -c passsword <Agent IP> 1.3.6.1.2.1


### ToDo

1. Complete the stand alone MIB Loader (See below)

    Well this will be first up. This will be a standalone Java package, as mentioned above, used to load MIB files into a table structure to be joined with during select statements... Guess I will tackle this first... ;) 
    

2. Complete the SNMPv3 Auth code.

    Not much to figure out, just need a **SNMPv3** compliant end point and then work through all the red lines in the code.
    
    As per Cisco => [SNMPv3](https://www.cisco.com/c/en/us/td/docs/ios-xml/ios/snmp/configuration/xe-3se/3850/snmp-xe-3se-3850-book/nm-snmp-snmpv3.pdf)


3. Need to come up with solution how to handle inbound SNMP Traps.
    
    Not sure... thinking some form of API end point.

    See: [SNMP-Traps](https://www.logicmonitor.com/blog/snmp-traps)


4. Code Instrumentation...

    A project like this does need code instrumentqtion, think Prometheus.

    
### W.R.T. => The SNMP MIB Loader

As if we did not have enough to do... All this data is awesome, but we need to make it usefull, and that means making the various oid values more user friendlu, sensible. Thats done by associating the oid values with nice english descriptions. This is done using [MIB](https://www.solarwinds.com/resources/it-glossary/mib) data. 


MIB files reference a [OID](https://www.paessler.com/it-explained/snmp-mibs-and-oids-an-overview#:~:text=SNMP%20OID,objects%20for%20their%20own%20products.) value and it's associated "english" description.


**SNMP OID**

As per Paessler: OIDs stands for Object Identifiers. OIDs uniquely identify managed objects in a MIB hierarchy. This can be depicted as a tree, the levels of which are assigned by different organizations. Top level MIB object IDs (OIDs) belong to different standard organizations.
Vendors define private branches including managed objects for their own products.


And here we are, back at the Rabbit Hole... So lets create an utility that can read a MIB file and insert it's relevant information into a designated Apache Flink table.

See earlier note, this utility has been moved into it's own GIT Repo.


### References

- [SNMP Protocol](https://en.wikipedia.org/wiki/Simple_Network_Management_Protocol)

- [Structure of Management Information (SMI) Numbers (MIB Module Registrations)](https://www.iana.org/assignments/smi-numbers/smi-numbers.xhtml)

- [Getting Started with SNMP](https://www.easysnmp.com/tutorial/getting-snmp-data/)

- [SNMP by Techtarget](https://www.techtarget.com/searchnetworking/definition/SNMP)

- [Good resource to download missing/dependency MIB files](https://mibbrowser.online/mibdb_search.php)

- [Lisa's Home Page: SNMP Simulator](https://www.rushworth.us/lisa/?p=11032)
 
- [Public SNMP Agent Simulator](http://snmplabs.com/snmpsim/public-snmp-agent-simulator.html)

- [Pysnmp](https://github.com/pysnmp)  & [PySNMP v7](https://docs.lextudio.com/snmp/) & [LexStudio GIT Repo](https://github.com/lextudio/mibs.pysnmp.com)

  - The above (`PySNMP v7`) includes some good history w.r.t SNMP.


### By:

George

[georgelza@gmail.com](georgelza@gmail.com)

[George on Linkedin](https://www.linkedin.com/in/george-leonard-945b502/)

[George on Medium](https://medium.com/@georgelza)
