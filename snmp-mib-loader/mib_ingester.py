#######################################################################################################################
#
#
#  	Project     	: 	SNMP MIB Loader/Ingester.
#
#   File            :   mib_ingester.py
#
#   Description     :   Load the oid name/description data from MIB files directly into designated tables that cna be
#                   :   exposed into Apache Flink
#
#                   :   Database engines currently supported: (Redis, PostgreSQL or MySql).
#
#	By              :   George Leonard ( georgelza@gmail.com )
#
#   Created     	:   05 Jul 2025
#
#   Example         :   python mib_ingester.py \
#                           --mib-file mibs/RFC1213-MIB.mib \
#                           --db-type postgresql \
#                           --db-host localhost \
#                           --db-port 5433 \
#                           --db-name snmp \
#                           --db-schema public \
#                           --db-user dbadmin \
#                           --db-password dbpassword \
#                           --tbl-name snmp_oid_data
# OR
#                       python3 mib_ingester.py \
#                           --mib-directory mibs \
#                           --db-type postgresql \
#                           --db-host localhost \
#                           --db-port 5433 \
#                           --db-name snmp \
#                           --db-schema public \
#                           --db-user dbadmin \
#                           --db-password dbpassword \
#                           --tbl-name snmp_oid_data
#
#   The script now uses argparse to accept command-line arguments for:
#
#       --mib-file (mutually exclusive with --mib-directory): Path to a single MIB file.
#       --mib-directory (mutually exclusive with --mib-file): Path to a directory containing MIB files.
#           --mib-dirs:         Optional list of directories where dependent MIBs are located.
#           --db-type           (required): postgresql, mysql, or redis.
#           --db-host           (required): Database hostname.
#           --db-port           (required): Database port.
#           --db-name:          Database name (for SQL) or DB index (for Redis).
#           --db-schema:        Schema name (for PostgreSQL).
#           --db-user:          Username (for SQL).
#           --db-password:      Password (for all).
#           --tbl-name:         Target table to load data into (for PostgreSQL/MySQL).
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
from datetime import datetime # For timestamp in logs
from utils import * # Import our custom logger from utils.py

# Initialize logger instance globally for this script
LOG_TAG             = 'snmp_mib_ingester'
LOG_FILE            = f'{LOG_TAG}.log'
CONSOLE_DEBUG_LEVEL = 1                                                 # INFO
FILE_DEBUG_LEVEL    = 0                                                 # DEBUG
CUSTOM_LOG_FORMAT   = f'{LOG_TAG}, %(asctime)s, %(message)s'            # Custom format: "{LOG_TAG}, {time}, message"
logger_instance     = logger(LOG_FILE, CONSOLE_DEBUG_LEVEL, FILE_DEBUG_LEVEL, CUSTOM_LOG_FORMAT)

# Conditional imports for database connectors
# These will need to be installed:
# pip install pysnmp psycopg2-binary mysql-connector-python redis
try:
    import psycopg2

except ImportError:
    psycopg2 = None
    logger_instance.warning("psycopg2 not found. PostgreSQL database option will not be available.")

# end try

try:
    import mysql.connector

except ImportError:
    mysql = None
    logger_instance.warning("mysql-connector-python not found. MySQL database option will not be available.")

# end try

try:
    import redis

except ImportError:
    redis = None
    logger_instance.warning("redis not found. Redis database option will not be available.")

# end try


def parse_mib_files(mib_file_path=None, mib_directory_path=None, mib_dirs=None, logger_instance=None):
    """
    Parses MIB files from a given path (file or directory) and extracts OID metadata.

    Args:
        mib_file_path (str, optional): The path to a single MIB file to parse.
        mib_directory_path (str, optional): The path to a directory containing MIB files.
        mib_dirs (list, optional): A list of directories where dependent MIBs
                                   might be located. Defaults to None.
        logger_instance (logging.Logger, optional): The logger instance to use.

    Returns:
        list: A list of dictionaries, each representing an OID's metadata.
              Returns an empty list if parsing fails or no OIDs are found.
    """
    if logger_instance is None:
        logger_instance = logging.getLogger(__name__) # Fallback if not provided

    # end if
    
    mib_data = []
    mibBuilder = builder.MibBuilder()

    # Add standard MIB sources (e.g., for SNMPv2-SMI, RFC1213-MIB)
    mibBuilder.add_mib_sources(
        builder.DirMibSource(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mibs'))
    )

    if mib_dirs:
        for mib_dir in mib_dirs:
            if os.path.isdir(mib_dir):
                mibBuilder.add_mib_sources(builder.DirMibSource(mib_dir))
            else:
                logger_instance.warning(f"MIB directory '{mib_dir}' does not exist or is not a directory.")

            # end if
        # end for
    # end if
    
    mib_files_to_load = []
    if mib_file_path:
        if os.path.isfile(mib_file_path):
            mib_files_to_load.append(mib_file_path)
            logger_instance.info(f"Loading single MIB file: {mib_file_path}")
            
        else:
            logger_instance.error(f"Invalid MIB file path: '{mib_file_path}'. File does not exist.")
            return []

        # enf if
    elif mib_directory_path:
        if os.path.isdir(mib_directory_path):
            logger_instance.info(f"Loading MIB files from directory: {mib_directory_path}")
            for filename in os.listdir(mib_directory_path):
                if filename.endswith(('.mib', '.txt')):
                    mib_files_to_load.append(os.path.join(mib_directory_path, filename))

                # end if
            # end fo
        else:
            logger_instance.error(f"Invalid MIB directory path: '{mib_directory_path}'. Directory does not exist.")
            return []
        
        # enf if
        
    else:
        logger_instance.error("No MIB file or directory specified. Please use --mib-file or --mib-directory.")
        return []

    # end if
    
    for mib_file_path in mib_files_to_load:
        mib_module_name = os.path.basename(mib_file_path).replace('.mib', '').replace('.txt', '')
        try:
            mibBuilder.load_modules(mib_module_name)
            logger_instance.info(f"Successfully loaded MIB module: {mib_module_name}")
            
        except error.MibLoadError as e:
            logger_instance.error(f"Error loading MIB module {mib_module_name} from {mib_file_path}: {e}")
            
        except Exception as e:
            logger_instance.error(f"An unexpected error occurred while loading MIB module {mib_module_name} from {mib_file_path}: {e}")

        # end try
    #end for
    
    # Iterate over all loaded MIB objects using mibBuilder.mibSymbols.values()
    for mibVar in mibBuilder.mibSymbols.values():
        try:
            if hasattr(mibVar, 'getName') and hasattr(mibVar, 'getOid'):
                oid_name    = str(mibVar.getName())
                oid_string  = str(mibVar.getOid())
                description = ""
                syntax_type = ""
                oid_type    = ""
                unit        = ""

                if hasattr(mibVar, 'getSyntax'):
                    syntax_type = str(mibVar.getSyntax().prettyPrint())

                # end if
                
                if hasattr(mibVar, 'getDescription'):
                    description = str(mibVar.getDescription())
                    desc_lower = description.lower()
                    if "seconds" in desc_lower:
                        unit = "seconds"

                    elif "bytes" in desc_lower:
                        unit = "bytes"

                    elif "kilobytes" in desc_lower:
                        unit = "kilobytes"

                    # end if
                # end if
                
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
                    oid_type = "other"

                # end if
                
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
            logger_instance.warning(f"Could not process MIB object {mibVar} ({type(mibVar)}): {e}")
            continue

        # end try
    # end for
    return mib_data
# parse_mib_files


class DatabaseManager:
    """
    Manages database connections and data insertion for various database types.
    """
    def __init__(self, db_type, host, port, user=None, password=None, dbname=None, schema=None, tbl_name="snmp_oid_metadata", key_prefix="oid:", logger_instance=None):
        self.db_type    = db_type.lower()
        self.host       = host
        self.port       = port
        self.user       = user
        self.password   = password
        self.dbname     = dbname
        self.schema     = schema        # Schema for PostgreSQL
        self.tbl_name   = tbl_name      # Table name for SQL databases
        self.key_prefix = key_prefix
        self.connection = None
        self.cursor     = None          # For SQL databases
        self.logger     = logger_instance if logger_instance else logging.getLogger(__name__)

    # end def

    def connect(self):
        """Establishes a connection to the specified database."""
        try:
            if self.db_type == 'postgresql':
                if not psycopg2:
                    raise ImportError("psycopg2 is not installed. Cannot connect to PostgreSQL.")

                # end if
                self.connection = psycopg2.connect(
                    host    = self.host,
                    port    = self.port,
                    user    = self.user,
                    password= self.password,
                    dbname  = self.dbname
                )
                
                self.connection.autocommit  = True # Auto-commit changes
                self.cursor                 = self.connection.cursor()

                self.logger.info(f"Connected to PostgreSQL database: {self.dbname} on {self.host}:{self.port}")

                if self.schema:
                    self.cursor.execute(f"SET search_path TO {self.schema}, public;")
                    self.logger.info(f"PostgreSQL search_path set to: {self.schema}, public")

                # end if
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
                self.logger.info(f"Connected to MySQL database: {self.dbname} on {self.host}:{self.port}")

            elif self.db_type == 'redis':
                if not redis:
                    raise ImportError("redis is not installed. Cannot connect to Redis.")

                #end if
                
                self.connection = redis.Redis(
                    host     = self.host,
                    port     = self.port,
                    db       = self.dbname, # In Redis, 'database' is an integer index
                    password = self.password
                )

                self.connection.ping() # Test connection
                self.logger.info(f"Connected to Redis database: {self.dbname} on {self.host}:{self.port}")

            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            #end if
        except Exception as e:
            self.logger.error(f"Error connecting to {self.db_type} database: {e}")
            self.connection = None
            self.cursor     = None
            raise
        
        #end try
    # end def

    def insert_oid_metadata(self, oid_data_list):
        """
        Inserts a list of OID metadata dictionaries into the connected database.
        Uses UPSERT logic for SQL databases.
        """
        if not self.connection:
            self.logger.error("No active database connection. Please connect first.")
            return

        # end if
        
        if self.db_type == 'postgresql':
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
                
                # end for
                self.logger.info(f"Successfully inserted/updated {len(oid_data_list)} OIDs into PostgreSQL table '{table_full_name}'.")

            except Exception as e:
                self.logger.error(f"Error inserting into PostgreSQL table '{table_full_name}': {e}")

            # end try
        elif self.db_type == 'mysql':
            # For MySQL, schema is part of the database connection, not table name directly in query
            sql = f"""
                INSERT INTO {self.tbl_name} (oid_string, oid_name, description, syntax_type, unit, oid_type)
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
                self.connection.commit()
                self.logger.info(f"Successfully inserted/updated {len(oid_data_list)} OIDs into MySQL table '{self.tbl_name}'.")

            except Exception as e:
                self.logger.error(f"Error inserting into MySQL table '{self.tbl_name}': {e}")
                self.connection.rollback()

            # enf try
        elif self.db_type == 'redis':
            try:
                pipe = self.connection.pipeline()
                for oid in oid_data_list:
                    redis_key = f"{self.key_prefix}{oid['oid_string']}"
                    pipe.set(redis_key, json.dumps(oid))

                # end fr
                pipe.execute()
                self.logger.info(f"Successfully inserted/updated {len(oid_data_list)} OIDs into Redis (keys prefixed with '{self.key_prefix}').")

            except Exception as e:
                self.logger.error(f"Error inserting into Redis: {e}")
            
            # end try
        else:
            self.logger.warning(f"Insertion not supported for database type: {self.db_type}")

        # end if
    # end insert_oid_metadata


    def close(self):
        """Closes the database connection."""
        if self.connection:
            if self.db_type in ['postgresql', 'mysql'] and self.cursor:
                self.cursor.close()

            # end if
            self.connection.close()
            self.logger.info(f"Closed {self.db_type} database connection.")

        else:
            self.logger.info("No active connection to close.")
            
        # end if
    # end def close
# end class DatabaseManager


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Parse SNMP MIB files and insert OID metadata into a database.")

    # Create a mutually exclusive group for mib_file and mib_directory
    mib_input_group = parser.add_mutually_exclusive_group(required=True)
    
    mib_input_group.add_argument("--mib-file",      help="Path to a single MIB file to parse.")
    mib_input_group.add_argument("--mib-directory", help="Path to a directory containing MIB files.")

    parser.add_argument("--mib-dirs",               nargs='*', default=[], help="Optional: List of directories where dependent MIBs are located.")
    parser.add_argument("--db-type",                choices=['postgresql', 'mysql', 'redis'], required=True, help="Type of database to connect to (postgresql, mysql, redis).")
    parser.add_argument("--db-host",                required=True, help="Database hostname or IP address.")
    parser.add_argument("--db-port",                type=int, required=True, help="Database port number.")
    parser.add_argument("--db-name",                help="Database name (for PostgreSQL/MySQL) or DB index (for Redis).")
    parser.add_argument("--db-user",                help="Database username (for PostgreSQL/MySQL).")
    parser.add_argument("--db-password",            help="Database password (for PostgreSQL/MySQL/Redis).")
    parser.add_argument("--db-schema",              help="Database schema name (for PostgreSQL).")
    parser.add_argument("--tbl-name",               default="snmp_oid_metadata", help="Table name for SQL databases (PostgreSQL/MySQL). Defaults to 'snmp_oid_metadata'.")
    parser.add_argument("--redis-key-prefix",       default="oid:", help="Prefix for Redis keys (e.g., 'oid:' for 'oid:1.3.6.1.2.1.1.3.0'). Only for Redis.")

    args = parser.parse_args()

    # Parse the MIB file(s) or directory
    # Pass the appropriate argument based on which one was provided
    parsed_oids = parse_mib_files(
        mib_file_path       = args.mib_file,
        mib_directory_path  = args.mib_directory,
        mib_dirs            = args.mib_dirs,
        logger_instance     = logger_instance
    )

    if not parsed_oids:
        logger_instance.info("No OID data extracted. Exiting.")
        exit()

    # end if
    
    logger_instance.info("\n--- Parsed OID Metadata Summary ---")
    # Changed to debug level for individual OID details to keep INFO logs cleaner
    for oid in parsed_oids:
        logger_instance.debug(f"OID String:     {oid['oid_string']}")
        logger_instance.debug(f"OID Name:       {oid['oid_name']}")
        logger_instance.debug(f"Description:    {oid['description']}")
        logger_instance.debug(f"Syntax Type:    {oid['syntax_type']}")
        logger_instance.debug(f"Unit:           {oid['unit']}")
        logger_instance.debug(f"OID Type:       {oid['oid_type']}")
        logger_instance.debug("-" * 20)
    
    # end for
    
    logger_instance.info(f"Total OIDs parsed: {len(parsed_oids)}")

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
            schema      = args.db_schema,
            tbl_name    = args.tbl_name,
            key_prefix  = args.redis_key_prefix,
            logger_instance = logger_instance # Pass the logger instance
        )

        db_manager.connect()
        db_manager.insert_oid_metadata(parsed_oids)

    except Exception as e:
        logger_instance.error(f"An error occurred during database operations: {e}")

    finally:
        if db_manager:
            db_manager.close()
        
        # end if
    # end try
# end main
