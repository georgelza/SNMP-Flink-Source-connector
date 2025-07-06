#  MIB Loader

The below is a program to load MIB information into a simple table structure to enable the user to join the MIB detail with the values extracted via the SNMP Source connector from the SNMP Agents.

As can be seen, it's a Python program basedon [pysnmp](https://github.com/pysnmp/pysnmp) python module.


## Prepare Python environment

1. python3 -m venv ./venv

2. source venv/bin/activate

3. pip install --upgrade pip    

4. cd snmp-mib-loader

5. pip install -r requirements

6. run one of the run*.sh scripts of choice, see mib_ingester.py header for example