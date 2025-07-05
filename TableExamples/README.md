
# Table Examples

Below are the various tables that at a basic level would/coud/should be considered to create a complete solution.

1. snmp_poll_data_get.sql

    - Various tables to be used for the snmpget method, 
      - single table single oid,
      - single table multiple oid's
      - multiple tables single oid
      - multiple table multiple oids
  
2. snmp_poll_data_walk.sql

    - Various tables to be used for the snmpwalk method
      - single table single root oid.
      - multiple tables with a common single root oid

3. snmp_poll_data_auth.sql

    - Some of the above, but with SNMPv3 as snmp.version specified, which means we require:
      - snmp.username
      - snmp.password
      - snmp.auth-protocol
      - snmp.priv-protocol

4. snmp_device_info_*.sql  

    - a Table listing the variosu device Id's and what they are, where they ar etc.

5. snmp_oid_metadata_*.sql

    - Table that will contain the output of the snmp-mib-loader package
      - We will show how to output to:
        - REDIS
        - PostgreSQL
        - MySQL

6. snmp_trap_data.sql
   
   - ToDo... need to figure out how we will do this... Thinking is a API endpoint to which the SNMP agent will send the trap, that will push the value into a table to be exposed via this table...