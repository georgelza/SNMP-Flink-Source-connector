
#######################################################################################################################
#
#
#  	Project     	: 	SNMP MIB Loader/Ingester.
#
#   File            :   mib_ingester.py
#
#   Description     :   Load the oid name/description data from MIB files directly into designated tables that cna be 
#                   :   exposed into Apache Flink

#                   :   Database engines currently supported: (Redis, PostgreSQL or MySql).
#  
#	By              :   George Leonard ( georgelza@gmail.com )
#
#   Created     	:   05 Jul 2025
#
#   Example         :   python mib_ingester.py \
#                           my-test-mib.mib \
#                           --db-type postgresql \
#                           --db-host localhost \
#                           --db-port 5432 \
#                           --db-name snmp \
#                           --db-schema public \
#                           --db-user dbadmin \
#                           --db-password dbpassword \
#                           --tbl-name snmp_oid_data
#
#   The script now uses argparse to accept command-line arguments for:
#
#       mib_file (required):    Path to the MIB file.
#           --mib-dirs:         Optional list of directories for dependent MIBs.
#           --db-type           (required): postgresql, mysql, or redis.
#           --db-host           (required): Database hostname.
#           --db-port           (required): Database port.
#           --db-name:          Database name (for SQL) or DB index (for Redis).
#           --db-schema:        Schema name (for SQL).
#           --db-user:          Username (for SQL).
#           --db-password:      Password (for all).
#           --tbl-name:         Target table to load data int
#           --redis-key-prefix: Custom key prefix for Redis (defaults to oid:).
# 
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "0.0.1"
__copyright__   = "Copyright 2025, George Leonard"


import os
import json
import argparse
from pysnmp.smi import builder, view, error


# Conditional imports for database connectors
# These will need to be installed:
# pip install pysnmp psycopg2-binary mysql-connector-python redis
try:
    import psycopg2
    
except ImportError:
    psycopg2 = None
    print("Warning: psycopg2 not found. PostgreSQL database option will not be available.")

# end try

try:
    import mysql.connector

except ImportError:
    mysql = None
    print("Warning: mysql-connector-python not found. MySQL database option will not be available.")

# end try

try:
    import redis
    
except ImportError:
    redis = None
    print("Warning: redis not found. Redis database option will not be available.")

# end try


def parse_mib_file(mib_file_path, mib_dirs=None):
    
    """
    Parses a given MIB file and extracts OID metadata.

    Args:
        mib_file_path (str): The path to the MIB file to parse.
        mib_dirs (list, optional): A list of directories where dependent MIBs
                                   might be located. Defaults to None.

    Returns:
        list: A list of dictionaries, each representing an OID's metadata.
              Returns an empty list if parsing fails or no OIDs are found.
    """
    
    mib_data = []

    # Create MIB loader/builder
    mibBuilder = builder.MibBuilder()

    # Add standard MIB sources (e.g., for SNMPv2-SMI, RFC1213-MIB)
    # These are usually shipped with pysnmp or can be downloaded.
    # Ensure these paths are correct for your environment.
    # We assume a 'mibs' folder exists next to the script for standard MIBs.
    mibBuilder.addMibSources(
        builder.DirMibSource(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mibs'))
    )
    
    if mib_dirs:
        for mib_dir in mib_dirs:
            if os.path.isdir(mib_dir):
                mibBuilder.addMibSources(builder.DirMibSource(mib_dir))
                
            else:
                print(f"Warning: MIB directory '{mib_dir}' does not exist or is not a directory.")
                
            #end if
        # end for
    # end if
    
    
    # Load the specified MIB module
    mib_module_name = os.path.basename(mib_file_path).replace('.mib', '').replace('.txt', '')
    try:
        # Load the MIB module by its name
        mibBuilder.loadModules(mib_module_name)
        print(f"Successfully loaded MIB module: {mib_module_name}")
        
    except error.MibLoadError as e:
        print(f"Error loading MIB module {mib_module_name}: {e}")
        return []
    
    except Exception as e:
        print(f"An unexpected error occurred while loading MIB module {mib_module_name}: {e}")
        return []

    #end try
    
    # Get a MIB view to iterate over objects (though not strictly used for iteration here)
    mibView = view.MibViewController(mibBuilder)


    # Iterate over all loaded MIB objects
    # We'll try to find all managed objects (OBJECT-TYPE, NOTIFICATION-TYPE, etc.)
    # The getObjects() method provides a way to iterate through all defined objects.
    for mibVar in mibBuilder.getObjects():
        try:
            # Check if it's a relevant MIB object (e.g., OBJECT-TYPE, NOTIFICATION-TYPE)
            # pysnmp represents these as instances of specific classes.
            if hasattr(mibVar, 'getName') and hasattr(mibVar, 'getOid'):
                
                oid_name    = str(mibVar.getName())
                oid_string  = str(mibVar.getOid())
                description = ""
                syntax_type = ""
                oid_type    = ""
                unit        = ""

                # For OBJECT-TYPEs and similar managed objects
                if hasattr(mibVar, 'getSyntax'):
                    # getSyntax() returns a pysnmp.smi.rfc1902.ObjectSyntax object
                    # prettyPrint() gives a readable string representation of the syntax
                    syntax_type = str(mibVar.getSyntax().prettyPrint())

                # end if
                
                if hasattr(mibVar, 'getDescription'):
                    description = str(mibVar.getDescription())
                    
                    # Simple attempt to extract unit from description
                    # This is a basic heuristic and might need more advanced NLP/regex
                    # For example, look for patterns like "in seconds", "bytes", "kilobytes"
                    desc_lower = description.lower()
                    if "seconds" in desc_lower:
                        unit = "seconds"
                        
                    elif "bytes" in desc_lower:
                        unit = "bytes"
                    
                    elif "kilobytes" in desc_lower:
                        unit = "kilobytes"
                    
                    # end if
                    
                    # Add more unit extraction logic as needed

                # end if
                
                # Determine oid_type based on MIB object class or properties
                # pysnmp objects have methods like isScalar, isTable, etc.
                if hasattr(mibVar, 'isScalar') and mibVar.isScalar():
                    oid_type = "scalar"
                    
                elif hasattr(mibVar, 'isTable') and mibVar.isTable():
                    oid_type = "table"
                
                elif hasattr(mibVar, 'isColumn') and mibVar.isColumn():
                    oid_type = "column"
                
                elif hasattr(mibVar, 'isNotification') and mibVar.isNotification():
                    oid_type = "notification"
                
                elif hasattr(mibVar, 'isModuleIdentity') and mibVar.isModuleIdentity():
                    oid_type = "module-identity"
                
                elif hasattr(mibVar, 'isObjectGroup') and mibVar.isObjectGroup():
                    oid_type = "object-group"
                
                else:
                    oid_type = "other" # Default for unclassified types
                
                #end if
                
                mib_data.append({
                    "oid_string":   oid_string,
                    "oid_name":     oid_name,
                    "description":  description,
                    "syntax_type":  syntax_type,
                    "unit":         unit,
                    "oid_type":     oid_type,
                })

            # end if
        except Exception as e:
            print(f"Warning: Could not process MIB object {mibVar} ({type(mibVar)}): {e}")
            continue

        # end try
    # end for
    
    return mib_data
# parse_mib_file


class DatabaseManager:
    
    """
    Manages database connections and data insertion for various database types.
    """
    def __init__(self, db_type, host, port, user=None, password=None, dbname=None, schema=None, tbl_name="snmp_oid_metadata", key_prefix="oid:"):

        self.db_type    = db_type.lower()
        self.host       = host
        self.port       = port
        self.user       = user
        self.password   = password
        self.dbname     = dbname
        self.schema     = schema        # Schema for PostgreSQL
        self.tblName    = tbl_name      # Table name for SQL databases
        self.key_prefix = key_prefix
        self.connection = None
        self.cursor     = None          # For SQL databases
    
    # end def
    
    def connect(self):
        """Establishes a connection to the specified database."""
        try:
            if self.db_type == 'postgresql':
                if not psycopg2:
                    raise ImportError("psycopg2 is not installed. Cannot connect to PostgreSQL.")
                
                self.connection = psycopg2.connect(
                    host    = self.host,
                    port    = self.port,
                    user    = self.user,
                    password= self.password,
                    dbname  = self.dbname
                    # Schema is typically set in the SQL query or via SET search_path
                )
                self.connection.autocommit  = True # Auto-commit changes
                self.cursor                 = self.connection.cursor()
                
                print(f"Connected to PostgreSQL database: {self.dbname} on {self.host}:{self.port}")
                
                if self.schema:
                    # Set the search path for the session to ensure correct schema usage
                    self.cursor.execute(f"SET search_path TO {self.schema}, public;")
                    print(f"PostgreSQL search_path set to: {self.schema}, public")
                
            elif self.db_type == 'mysql':
                if not mysql:
                    raise ImportError("mysql-connector-python is not installed. Cannot connect to MySQL.")

                # end if

                self.connection = mysql.connector.connect(
                    host        = self.host,
                    port        = self.port,
                    user        = self.user,
                    password    = self.password,
                    database    = self.dbname
                )
                
                self.cursor = self.connection.cursor()
                print(f"Connected to MySQL database: {self.dbname} on {self.host}:{self.port}")
                
            elif self.db_type == 'redis':
                if not redis:
                    raise ImportError("redis is not installed. Cannot connect to Redis.")
                
                # end if / Redis
                
                self.connection = redis.Redis(
                    host     = self.host,
                    port     = self.port,
                    db       = self.dbname, # In Redis, 'database' is an integer index
                    password = self.password
                )
                
                self.connection.ping() # Test connection
                print(f"Connected to Redis database: {self.dbname} on {self.host}:{self.port}")
                
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            # end if
            
        except Exception as e:
            print(f"Error connecting to {self.db_type} database: {e}")
            self.connection = None
            self.cursor     = None
            raise
        
        # end Try
    # end def
            
    def insert_oid_metadata(self, oid_data_list):
        
        """
        Inserts a list of OID metadata dictionaries into the connected database.
        Uses UPSERT logic for SQL databases.
        """
        if not self.connection:
            print("No active database connection. Please connect first.")
            return

        # end if
        
        
        if self.db_type == 'postgresql':
            
            # Construct the table name with schema if provided
            table_full_name = f"{self.schema}.{self.tbl_name}" if self.schema else self.tbl_name
            
            sql = f"""
                INSERT INTO {table_full_name} (oid_string, oid_name, description, syntax_type, unit, oid_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (oid_string) DO UPDATE SET
                    oid_name    = EXCLUDED.oid_name,
                    description = EXCLUDED.description,
                    syntax_type = EXCLUDED.syntax_type,
                    unit        = EXCLUDED.unit,
                    oid_type    = EXCLUDED.oid_type;
            """
            
            try:
                for oid in oid_data_list:
                    self.cursor.execute(sql, (
                        oid['oid_string'], 
                        oid['oid_name'], 
                        oid['description'],
                        oid['syntax_type'], 
                        oid['unit'], 
                        oid['oid_type']
                    ))
                print(f"Successfully inserted/updated {len(oid_data_list)} OIDs into PostgreSQL table '{table_full_name}'.")
                
            except Exception as e:
                print(f"Error inserting into PostgreSQL table '{table_full_name}': {e}")
                # self.connection.rollback() # Not needed with autocommit=True

            # end try

        elif self.db_type == 'mysql':
            
            table_full_name = f"{self.schema}"

            sql = f"""
                INSERT INTO {table_full_name} (oid_string, oid_name, description, syntax_type, unit, oid_type)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    oid_name    = VALUES(oid_name),
                    description = VALUES(description),
                    syntax_type = VALUES(syntax_type),
                    unit        = VALUES(unit),
                    oid_type    = VALUES(oid_type);
            """
            try:
                for oid in oid_data_list:
                    self.cursor.execute(sql, (
                        oid['oid_string'], 
                        oid['oid_name'], 
                        oid['description'],
                        oid['syntax_type'], 
                        oid['unit'], 
                        oid['oid_type']
                    ))
                # end for
                
                self.connection.commit() # MySQL connector might require explicit commit
                print(f"Successfully inserted/updated {len(oid_data_list)} OIDs into MySQL:"+ self.tblName)
            
            except Exception as e:
                print(f"Error inserting into MySQL: {e}")
                self.connection.rollback()
            
            # end try
            
        elif self.db_type == 'redis':
            try:
                pipe = self.connection.pipeline()
                for oid in oid_data_list:
                    redis_key = f"{self.key_prefix}{oid['oid_string']}"
                    # Store the entire OID dictionary as a JSON string
                    # The Flink example implies storing the whole payload as JSON
                    pipe.set(redis_key, json.dumps(oid))
                
                # end for
                pipe.execute()
                print(f"Successfully inserted/updated {len(oid_data_list)} OIDs into Redis.")
                
            except Exception as e:
                print(f"Error inserting into Redis: {e}")
            
            #end try
        else:
            print(f"Insertion not supported for database type: {self.db_type}")

        # end if
    # end insert_oid_metadata
    
    
    def close(self):
        """Closes the database connection."""
        if self.connection:
            if self.db_type in ['postgresql', 'mysql'] and self.cursor:
                self.cursor.close()
            
            # end if
            
            self.connection.close()
            print(f"Closed {self.db_type} database connection.")
            
        else:
            print("No active connection to close.")

        # end if
    # end fef close
# end class DatabaseManager


# Create a dummy MIB file for testing if it doesn't exist
def createFakeMIB(args):
    
    # Create a dummy MIB file for testing if it doesn't exist
    dummy_mib_path = args.mib_file
    
    if not os.path.exists(dummy_mib_path):
    
        dummy_mib_content = """
        MY-TEST-MIB DEFINITIONS ::= BEGIN

        IMPORTS
            MODULE-IDENTITY, OBJECT-TYPE, NOTIFICATION-TYPE,
            Integer32, Gauge32, DisplayString
                FROM SNMPv2-SMI;

        myTestModule MODULE-IDENTITY
            LAST-UPDATED "202310270000Z"
            ORGANIZATION "My Organization"
            CONTACT-INFO "support@example.com"
            DESCRIPTION
                "A test MIB module."
            REVISION "202310270000Z"
            DESCRIPTION
                "Initial version."
            ::= { iso(1) org(3) dod(6) internet(1) private(4) enterprises(1) 99999 }

        myScalarObject OBJECT-TYPE
            SYNTAX      DisplayString (SIZE (0..255))
            MAX-ACCESS  read-only
            STATUS      current
            DESCRIPTION
                "A simple scalar object, typically a string."
            ::= { myTestModule 1 }

        myIntegerScalar OBJECT-TYPE
            SYNTAX      Integer32
            MAX-ACCESS  read-only
            STATUS      current
            DESCRIPTION
                "A simple integer scalar object, representing a count in seconds."
            ::= { myTestModule 2 }

        myTable OBJECT-TYPE
            SYNTAX      SEQUENCE OF MyEntry
            MAX-ACCESS  not-accessible
            STATUS      current
            DESCRIPTION
                "A test table."
            ::= { myTestModule 3 }

        MyEntry ::=
            SEQUENCE {
                myIndex     Integer32,
                myColumn1   DisplayString,
                myColumn2   Gauge32
            }

        myIndex OBJECT-TYPE
            SYNTAX      Integer32
            MAX-ACCESS  not-accessible
            STATUS      current
            DESCRIPTION
                "Index for myTable."
            ::= { myEntry 1 }

        myColumn1 OBJECT-TYPE
            SYNTAX      DisplayString
            MAX-ACCESS  read-only
            STATUS      current
            DESCRIPTION
                "First column of myTable."
            ::= { myEntry 2 }

        myColumn2 OBJECT-TYPE
            SYNTAX      Gauge32
            MAX-ACCESS  read-only
            STATUS      current
            DESCRIPTION
                "Second column of myTable, a gauge value."
            ::= { myEntry 3 }

        myNotification NOTIFICATION-TYPE
            OBJECTS { myScalarObject }
            STATUS      current
            DESCRIPTION
                "A test notification."
            ::= { myTestModule 4 }

        END
        """
        
        # Ensure a 'mibs' directory exists for pysnmp to find standard MIBs
        mibs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mibs')
        if not os.path.exists(mibs_dir):
            os.makedirs(mibs_dir)
            print(f"Created '{mibs_dir}' directory. You might need to place standard MIBs (e.g., SNMPv2-SMI.mib) here.")

        # end if
        
        with open(dummy_mib_path, "w") as f:
            f.write(dummy_mib_content)
          
        # end with  
        
        print(f"Created dummy MIB file for parsing: {dummy_mib_path}")

    #end if
    
    # Parse the MIB file
    parsed_oids = parse_mib_file(args.mib_file, args.mib_dirs)

    if not parsed_oids:
        print("No OID data extracted. Exiting.")
        # Clean up dummy MIB file if it was created for this run
        if os.path.exists(dummy_mib_path) and dummy_mib_path.startswith(os.path.dirname(os.path.abspath(__file__))):
            os.remove(dummy_mib_path)

        # end if
        exit()
    # end if
    
    print("\n--- Parsed OID Metadata ---")
    for oid in parsed_oids:
        print(f"OID String:     {oid['oid_string']}")
        print(f"OID Name:       {oid['oid_name']}")
        print(f"Description:    {oid['description']}")
        print(f"Syntax Type:    {oid['syntax_type']}")
        print(f"Unit:           {oid['unit']}")
        print(f"OID Type:       {oid['oid_type']}")
        print("-" * 20)
        
    # end for
    
    return parsed_oids, dummy_mib_path

# end createFakeMIB


def Parse_and_load(args):
    
    parsed_oids, dummy_mib_path = createFakeMIB(args)

    # Database Insertion
    db_manager = None
    try:
        db_manager = DatabaseManager(
            db_type     = args.db_type,
            host        = args.db_host,
            port        = args.db_port,
            user        = args.db_user,
            password    = args.db_password,
            dbname      = args.db_name,
            key_prefix  = args.redis_key_prefix
        )
        
        db_manager.connect()
        db_manager.insert_oid_metadata(parsed_oids)
        
    except Exception as e:
        print(f"An error occurred during database operations: {e}")
        
    finally:
        if db_manager:
            db_manager.close()

        # end if
    # end try
    
    # Clean up dummy MIB file if it was created for this run
    if os.path.exists(dummy_mib_path) and dummy_mib_path.startswith(os.path.dirname(os.path.abspath(__file__))):

        os.remove(dummy_mib_path)
        
        print(f"Cleaned up dummy MIB file: {dummy_mib_path}")

    # end if
# end load


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Parse SNMP MIB files and insert OID metadata into a database.")
    parser.add_argument("mib_file", help="Path to the SNMP MIB file to parse.")
    parser.add_argument("--mib-dirs", nargs='*', default=[],
                        help="Optional: List of directories where dependent MIBs are located.")
    parser.add_argument("--db-type", choices=['postgresql', 'mysql', 'redis'], required=True,
                        help="Type of database to connect to (postgresql, mysql, redis).")
    parser.add_argument("--db-host", required=True, help="Database hostname or IP address.")
    parser.add_argument("--db-port", type=int, required=True, help="Database port number.")
    parser.add_argument("--db-name", help="Database name (for PostgreSQL/MySQL) or DB index (for Redis).")
    parser.add_argument("--db-user", help="Database username (for PostgreSQL/MySQL).")
    parser.add_argument("--db-password", help="Database password (for PostgreSQL/MySQL/Redis).")    
    parser.add_argument("--db-schema", help="Database schema name (for PostgreSQL).")
    parser.add_argument("--tbl-name ", help="Table name (for PostgreSQL/MySQL).")
    parser.add_argument("--redis-key-prefix", default="oid:",
                        help="Prefix for Redis keys (e.g., 'oid:' for 'oid:1.3.6.1.2.1.1.3.0'). Only for Redis.")

    args = parser.parse_args()

    Parse_and_load(args)
    
# end main