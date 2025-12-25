import os
import psycopg2
import subprocess
from psycopg2.extras import RealDictCursor, DictCursor
from dotenv import load_dotenv
from datetime import datetime
import json
import pandas as pd
import io
import shutil
from pathlib import Path
import csv
import logging

load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection_config = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'my_app_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        self.pg_dump_cmd = "pg_dump"
        self.pg_restore_cmd = "pg_restore"
        
        self.storage_dirs = self.initialize_storage_directories()
    
    def initialize_storage_directories(self):
        """Initialize storage directories for different file types"""
        directory_map = {
            'backups': Path("backups"),
            'exports': Path("exports"),
            'archives': Path("archives")
        }
        
        for dir_path in directory_map.values():
            dir_path.mkdir(exist_ok=True)
            logger.info(f"Directory initialized: {dir_path}")
        
        return directory_map
    
    def generate_timestamp_directory(self, base_directory):
        """Create timestamp-based directory structure"""
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        target_dir = base_directory / timestamp_str
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir
    
    def establish_connection(self, use_dict_cursor=True):
        """Establish database connection"""
        try:
            conn = psycopg2.connect(
                **self.connection_config,
                cursor_factory=RealDictCursor if use_dict_cursor else DictCursor
            )
            logger.info("Database connection established")
            return conn
        except Exception as conn_error:
            logger.error(f"Connection failed: {conn_error}")
            return None
    
    def run_database_query(self, query_string, query_params=None, fetch_results=True):
        """Execute database query with parameters"""
        db_conn = self.establish_connection()
        if not db_conn:
            return None
        
        try:
            with db_conn.cursor() as cursor:
                cursor.execute(query_string, query_params or ())
                if fetch_results and cursor.description:
                    query_result = cursor.fetchall()
                else:
                    query_result = None
                db_conn.commit()
                return query_result
        except Exception as query_error:
            db_conn.rollback()
            logger.error(f"Query execution error: {query_error}")
            return None
        finally:
            db_conn.close()
    
    def get_table_names(self):
        """Retrieve all table names from database"""
        table_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
        query_result = self.run_database_query(table_query)
        return [row['table_name'] for row in query_result] if query_result else []
    
    def get_table_structure(self, table_name):
        """Get column structure of specified table"""
        structure_query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        return self.run_database_query(structure_query, (table_name,))
    
    def get_table_constraints_info(self, table_name):
        """Retrieve table constraints information"""
        constraints_query = """
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_name = %s
            AND tc.constraint_type IN ('FOREIGN KEY', 'PRIMARY KEY')
            ORDER BY tc.constraint_type, kcu.ordinal_position;
        """
        return self.run_database_query(constraints_query, (table_name,))
    
    def get_referenced_tables(self, table_name, specific_column=None):
        """Find tables referencing the specified table"""
        reference_query = """
            SELECT
                tc.table_name as referencing_table,
                kcu.column_name as referencing_column,
                ccu.table_name as referenced_table,
                ccu.column_name as referenced_column
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND ccu.table_name = %s
            AND (%s IS NULL OR ccu.column_name = %s)
        """
        return self.run_database_query(
            reference_query, 
            (table_name, specific_column, specific_column)
        )
    
    def fetch_table_data(self, table_name, row_limit=None, row_offset=0):
        """Fetch data from specified table"""
        if row_limit:
            data_query = f"SELECT * FROM {table_name} LIMIT %s OFFSET %s"
            return self.run_database_query(data_query, (row_limit, row_offset))
        else:
            data_query = f"SELECT * FROM {table_name}"
            return self.run_database_query(data_query)
    
    def count_table_rows(self, table_name):
        """Count rows in specified table"""
        count_query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        query_result = self.run_database_query(count_query)
        return query_result[0]['row_count'] if query_result else 0
    
    def add_table_row(self, table_name, row_data):
        """Insert new row into table"""
        columns = ', '.join(row_data.keys())
        value_placeholders = ', '.join(['%s'] * len(row_data))
        insert_query = f"""
            INSERT INTO {table_name} ({columns}) 
            VALUES ({value_placeholders}) 
            RETURNING id
        """
        query_result = self.run_database_query(insert_query, tuple(row_data.values()))
        return query_result[0]['id'] if query_result else None
    
    def modify_table_data(self, table_name, update_data, filter_condition):
        """Update table data with cascade support"""
        try:
            db_conn = self.establish_connection()
            if not db_conn:
                return None
            
            cursor = db_conn.cursor()
            
            # Retrieve existing data
            fetch_query = f"SELECT * FROM {table_name} WHERE {filter_condition}"
            cursor.execute(fetch_query)
            existing_rows = cursor.fetchall()
            
            if not existing_rows:
                db_conn.close()
                return False
            
            update_results = []
            
            for existing_row in existing_rows:
                row_dict = dict(zip([desc[0] for desc in cursor.description], existing_row))
                
                updated_row = row_dict.copy()
                for key, value in update_data.items():
                    if value:
                        updated_row[key] = value
                
                # Handle foreign key updates
                fk_query = """
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage AS ccu
                        ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.table_name = %s
                    AND tc.constraint_type = 'FOREIGN KEY'
                """
                cursor.execute(fk_query, (table_name,))
                foreign_keys = cursor.fetchall()
                
                for fk_info in foreign_keys:
                    local_col = fk_info[0]
                    foreign_tbl = fk_info[1]
                    foreign_col = fk_info[2]
                    
                    if local_col in update_data and update_data[local_col]:
                        old_value = row_dict[local_col]
                        new_value = update_data[local_col]
                        
                        if old_value != new_value:
                            update_related = f"""
                                UPDATE {foreign_tbl} 
                                SET {foreign_col} = %s 
                                WHERE {foreign_col} = %s
                            """
                            cursor.execute(update_related, (new_value, old_value))
                
                # Execute main update
                set_clause = ', '.join([f"{k} = %s" for k in update_data.keys() if update_data[k]])
                update_values = [update_data[k] for k in update_data.keys() if update_data[k]]
                
                where_condition = " AND ".join([f"{k} = %s" for k in row_dict.keys()])
                where_values = list(row_dict.values())
                
                update_query = f"""
                    UPDATE {table_name} 
                    SET {set_clause} 
                    WHERE {where_condition}
                """
                
                cursor.execute(update_query, update_values + where_values)
                update_results.append(cursor.rowcount > 0)
            
            db_conn.commit()
            db_conn.close()
            
            return any(update_results)
            
        except Exception as update_error:
            logger.error(f"Data modification error: {update_error}")
            return None
    
    def remove_table_data(self, table_name, filter_condition):
        """Remove data from table with cascade deletion"""
        try:
            db_conn = self.establish_connection()
            if not db_conn:
                return None
            
            cursor = db_conn.cursor()
            
            # Handle referenced tables
            referenced_tables = self.get_referenced_tables(table_name)
            
            for ref_info in referenced_tables:
                ref_table = ref_info['referencing_table']
                ref_column = ref_info['referencing_column']
                
                delete_ref_query = f"""
                    DELETE FROM {ref_table} 
                    WHERE {ref_column} IN (
                        SELECT id FROM {table_name} WHERE {filter_condition}
                    )
                """
                cursor.execute(delete_ref_query)
            
            # Delete from main table
            delete_query = f"DELETE FROM {table_name} WHERE {filter_condition}"
            cursor.execute(delete_query)
            
            rows_affected = cursor.rowcount
            db_conn.commit()
            db_conn.close()
            
            return rows_affected > 0
            
        except Exception as delete_error:
            logger.error(f"Data removal error: {delete_error}")
            return None
    
    def safe_remove_table_data(self, table_name, filter_condition):
        """Safe data removal with dependency checking"""
        try:
            db_conn = self.establish_connection()
            if not db_conn:
                return None
            
            cursor = db_conn.cursor()
            
            referenced_tables = self.get_referenced_tables(table_name)
            
            dependency_exists = False
            dependency_list = []
            
            for ref_info in referenced_tables:
                ref_table = ref_info['referencing_table']
                ref_column = ref_info['referencing_column']
                
                dependency_check = f"""
                    SELECT COUNT(*) 
                    FROM {ref_table} 
                    WHERE {ref_column} IN (
                        SELECT id FROM {table_name} WHERE {filter_condition}
                    )
                """
                cursor.execute(dependency_check)
                dependency_count = cursor.fetchone()[0]
                
                if dependency_count > 0:
                    dependency_exists = True
                    dependency_list.append({
                        'table': ref_table,
                        'count': dependency_count
                    })
            
            if dependency_exists:
                db_conn.close()
                return {
                    'status': False,
                    'error_type': 'dependencies_exist',
                    'dependency_list': dependency_list
                }
            
            # Execute safe deletion
            delete_query = f"DELETE FROM {table_name} WHERE {filter_condition}"
            cursor.execute(delete_query)
            
            rows_affected = cursor.rowcount
            db_conn.commit()
            db_conn.close()
            
            return {
                'status': True,
                'rows_affected': rows_affected
            }
            
        except Exception as safe_delete_error:
            logger.error(f"Safe removal error: {safe_delete_error}")
            return {
                'status': False,
                'error_message': str(safe_delete_error)
            }
    
    def drop_database_table(self, table_name):
        """Remove table from database"""
        try:
            drop_query = f"DROP TABLE IF EXISTS {table_name} CASCADE"
            db_conn = self.establish_connection(use_dict_cursor=False)
            if not db_conn:
                return False
            
            try:
                with db_conn.cursor() as cursor:
                    cursor.execute(drop_query)
                    db_conn.commit()
                    logger.info(f"Table removed: {table_name}")
                    return True
            except Exception as drop_error:
                db_conn.rollback()
                logger.error(f"Table removal error for {table_name}: {drop_error}")
                return False
            finally:
                db_conn.close()
        except Exception as general_error:
            logger.error(f"Table removal exception for {table_name}: {general_error}")
            return False
    
    def export_table_to_spreadsheet(self, table_name):
        """Export table to Excel format"""
        try:
            table_data = self.fetch_table_data(table_name, limit=50000)
            if not table_data:
                return None, "No data available for export"
            
            export_dir = self.generate_timestamp_directory(self.storage_dirs['exports'])
            filename = f"{table_name}_{datetime.now().strftime('%H%M%S')}.xlsx"
            export_path = export_dir / filename
            
            data_frame = pd.DataFrame(table_data)
            data_frame.to_excel(str(export_path), index=False)
            
            logger.info(f"Table exported: {table_name} -> {export_path}")
            return str(export_path), filename
            
        except Exception as export_error:
            logger.error(f"Spreadsheet export error: {export_error}")
            return None, str(export_error)
    
    def export_table_to_json_format(self, table_name):
        """Export table to JSON format"""
        try:
            table_data = self.fetch_table_data(table_name, limit=50000)
            if not table_data:
                return None, "No data available for export"
            
            export_dir = self.generate_timestamp_directory(self.storage_dirs['exports'])
            filename = f"{table_name}_{datetime.now().strftime('%H%M%S')}.json"
            export_path = export_dir / filename
            
            json_serializable = []
            for row in table_data:
                json_row = {}
                for key, value in row.items():
                    if isinstance(value, (datetime, pd.Timestamp)):
                        json_row[key] = value.isoformat()
                    elif hasattr(value, '__dict__'):
                        json_row[key] = str(value)
                    else:
                        json_row[key] = value
                json_serializable.append(json_row)
            
            with open(export_path, 'w', encoding='utf-8') as json_file:
                json.dump(json_serializable, json_file, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"JSON export completed: {table_name} -> {export_path}")
            return str(export_path), filename
            
        except Exception as json_error:
            logger.error(f"JSON export error: {json_error}")
            return None, str(json_error)
    
    def export_query_to_spreadsheet(self, query_result):
        """Export query results to spreadsheet"""
        try:
            if not query_result:
                return None, "No data available for export"
            
            temp_table = f"temp_export_{datetime.now().strftime('%H%M%S')}"
            
            db_conn = self.establish_connection(use_dict_cursor=False)
            if not db_conn:
                return None, "Database connection failed"
            
            try:
                with db_conn.cursor() as cursor:
                    first_row = query_result[0]
                    columns = list(first_row.keys())
                    
                    column_definitions = []
                    for col in columns:
                        column_definitions.append(f'"{col}" TEXT')
                    
                    create_temp = f"""
                        CREATE TEMPORARY TABLE {temp_table} (
                            {', '.join(column_definitions)}
                        )
                    """
                    cursor.execute(create_temp)
                    
                    for row in query_result:
                        placeholders = ', '.join(['%s'] * len(columns))
                        insert_temp = f"""
                            INSERT INTO {temp_table} ({', '.join([f'"{c}"' for c in columns])})
                            VALUES ({placeholders})
                        """
                        values = [str(row.get(col, '')) for col in columns]
                        cursor.execute(insert_temp, values)
                    
                    db_conn.commit()
                    
                    export_dir = self.generate_timestamp_directory(self.storage_dirs['exports'])
                    filename = f"query_export_{datetime.now().strftime('%H%M%S')}.xlsx"
                    export_path = export_dir / filename
                    
                    cursor.execute(f'SELECT * FROM {temp_table}')
                    result_rows = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    
                    result_df = pd.DataFrame(result_rows, columns=column_names)
                    result_df.to_excel(str(export_path), index=False)
                    
                    cursor.execute(f'DROP TABLE IF EXISTS {temp_table}')
                    db_conn.commit()
                    
                    return str(export_path), filename
                    
            except Exception as temp_error:
                db_conn.rollback()
                return None, f"Export processing error: {str(temp_error)}"
            finally:
                db_conn.close()
                
        except Exception as export_error:
            logger.error(f"Query export error: {export_error}")
            return None, str(export_error)
    
    # ============ BACKUP MANAGEMENT ============
    
    def create_database_backup(self):
        """Create full database backup"""
        try:
            db_name = os.getenv('DB_NAME', 'my_app_db')
            db_user = os.getenv('DB_USER', 'postgres')
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = os.getenv('DB_PORT', '5432')
            
            backup_dir = self.generate_timestamp_directory(self.storage_dirs['backups'])
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = backup_dir / f"backup_{db_name}_{timestamp_str}.backup"
            
            backup_command = [
                self.pg_dump_cmd,
                '-h', db_host,
                '-U', db_user,
                '-p', db_port,
                '-d', db_name,
                '-F', 'c',
                '--no-unlogged-table-data',
                '-f', str(backup_file),
                '-v'
            ]
            
            logger.info(f"Backup command: {' '.join(backup_command)}")
            
            env_vars = os.environ.copy()
            env_vars['PGPASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')
            
            backup_process = subprocess.run(
                backup_command,
                env=env_vars,
                capture_output=True,
                text=True,
                shell=False
            )
            
            if backup_process.returncode == 0:
                logger.info(f"Backup successful: {backup_file}")
                return True, str(backup_file), None
            else:
                error_output = f"Backup failed:\n{backup_process.stderr}\n{backup_process.stdout}"
                logger.error(error_output)
                return False, None, error_output
                
        except Exception as backup_error:
            error_message = f"Backup exception: {str(backup_error)}"
            logger.error(error_message)
            return False, None, error_message
    
    def create_table_backup(self, table_name, target_directory):
        """Create backup of specific table"""
        try:
            db_user = os.getenv('DB_USER', 'postgres')
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'my_app_db')
            
            backup_file = target_directory / f"backup_{table_name}_{datetime.now().strftime('%H%M%S')}.backup"
            
            table_backup_command = [
                self.pg_dump_cmd,
                '-h', db_host,
                '-U', db_user,
                '-p', db_port,
                '-d', db_name,
                '-t', table_name,
                '-F', 'c',
                '--no-unlogged-table-data',
                '-f', str(backup_file),
                '-v'
            ]
            
            logger.info(f"Table backup command for {table_name}: {' '.join(table_backup_command)}")
            
            env_vars = os.environ.copy()
            env_vars['PGPASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')
            
            backup_process = subprocess.run(
                table_backup_command,
                env=env_vars,
                capture_output=True,
                text=True,
                shell=False
            )
            
            if backup_process.returncode == 0:
                logger.info(f"Table backup successful: {table_name} -> {backup_file}")
                return True, str(backup_file), None
            else:
                error_output = f"Table backup failed for {table_name}:\n{backup_process.stderr}\n{backup_process.stdout}"
                logger.error(error_output)
                return False, None, error_output
                
        except Exception as table_backup_error:
            error_message = f"Table backup exception for {table_name}: {str(table_backup_error)}"
            logger.error(error_message)
            return False, None, error_message
    
    def restore_database_backup(self, backup_file_path):
        """Restore database from backup file"""
        try:
            if not os.path.exists(backup_file_path):
                return False, f"Backup file not found: {backup_file_path}"
            
            db_user = os.getenv('DB_USER', 'postgres')
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'my_app_db')
            
            restore_command = [
                self.pg_restore_cmd,
                '-h', db_host,
                '-U', db_user,
                '-p', db_port,
                '-d', db_name,
                '-v',
                '--clean',
                '--if-exists',
                '--no-comments',
                str(backup_file_path)
            ]
            
            logger.info(f"Restore command: {' '.join(restore_command)}")
            
            env_vars = os.environ.copy()
            env_vars['PGPASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')
            
            restore_process = subprocess.run(
                restore_command,
                env=env_vars,
                capture_output=True,
                text=True,
                shell=False
            )
            
            process_output = restore_process.stdout + restore_process.stderr
            
            if "unrecognized configuration parameter \"transaction_timeout\"" in process_output:
                if ("pg_restore: error" not in process_output or 
                    process_output.count("pg_restore: error") == 1 and 
                    "transaction_timeout" in process_output):
                    
                    logger.warning(f"Restore completed with transaction_timeout warning")
                    
                    try:
                        db_conn = self.establish_connection()
                        if db_conn:
                            cursor = db_conn.cursor()
                            cursor.execute("""
                                SELECT COUNT(*) 
                                FROM information_schema.tables 
                                WHERE table_schema = 'public'
                            """)
                            table_count = cursor.fetchone()[0]
                            db_conn.close()
                            
                            if table_count > 0:
                                return True, "Database restored successfully (warnings ignored)"
                    except:
                        pass
                    
                    return True, "Restoration completed with warnings"
            
            if restore_process.returncode == 0:
                logger.info(f"Restore successful: {backup_file_path}")
                return True, "Database restored successfully"
            else:
                error_output = f"Restore failed:\n{restore_process.stderr}\n{restore_process.stdout}"
                logger.error(error_output)
                return False, error_output
                
        except Exception as restore_error:
            error_message = f"Restore exception: {str(restore_error)}"
            logger.error(error_message)
            return False, error_message
    
    # ============ EXPORT FUNCTIONS ============
    
    def export_multiple_tables(self, table_list, export_format='excel'):
        """Export multiple tables to specified format"""
        try:
            if not table_list:
                return None, "No tables selected for export"
            
            export_dir = self.generate_timestamp_directory(self.storage_dirs['exports'])
            
            if export_format == 'excel':
                filename = f"export_{datetime.now().strftime('%H%M%S')}.xlsx"
                export_path = export_dir / filename
                
                with pd.ExcelWriter(str(export_path), engine='openpyxl') as excel_writer:
                    for table in table_list:
                        table_data = self.fetch_table_data(table, limit=50000)
                        if table_data:
                            data_frame = pd.DataFrame(table_data)
                            sheet_name = table[:31]
                            data_frame.to_excel(excel_writer, sheet_name=sheet_name, index=False)
                
                return str(export_path), filename
                
            elif export_format == 'json':
                filename = f"export_{datetime.now().strftime('%H%M%S')}.json"
                export_path = export_dir / filename
                
                export_result = {}
                for table in table_list:
                    table_data = self.fetch_table_data(table, limit=50000)
                    if table_data:
                        json_data = []
                        for row in table_data:
                            json_row = {}
                            for key, value in row.items():
                                if isinstance(value, (datetime, pd.Timestamp)):
                                    json_row[key] = value.isoformat()
                                elif hasattr(value, '__dict__'):
                                    json_row[key] = str(value)
                                else:
                                    json_row[key] = value
                            json_data.append(json_row)
                        export_result[table] = json_data
                
                with open(export_path, 'w', encoding='utf-8') as json_file:
                    json.dump(export_result, json_file, ensure_ascii=False, indent=2, default=str)
                
                return str(export_path), filename
            else:
                return None, f"Unsupported export format: {export_format}"
                
        except Exception as export_error:
            logger.error(f"Multi-table export error: {export_error}")
            return None, str(export_error)
    
    def export_all_tables(self, export_format='excel'):
        """Export all database tables"""
        all_tables = self.get_table_names()
        return self.export_multiple_tables(all_tables, export_format)
    
    # ============ ARCHIVING FUNCTIONS ============
    
    def archive_database_tables(self, table_list):
        """Archive specified tables"""
        try:
            logger.info(f"Archiving tables: {table_list}")
            
            if not table_list:
                return False, "No tables selected for archiving"
            
            archive_dir = self.generate_timestamp_directory(self.storage_dirs['archives'])
            
            archive_results = []
            overall_success = True
            
            for table in table_list:
                try:
                    existing_tables = self.get_table_names()
                    if table not in existing_tables:
                        archive_results.append(f"Table '{table}' does not exist")
                        continue
                    
                    logger.info(f"Starting archive for table: {table}")
                    
                    # Create table backup
                    backup_success, backup_file, backup_error = self.create_table_backup(table, archive_dir)
                    
                    if not backup_success:
                        archive_results.append(f"Backup failed for '{table}': {backup_error}")
                        overall_success = False
                        continue
                    
                    # Export to Excel
                    excel_filename = f"{table}_{datetime.now().strftime('%H%M%S')}.xlsx"
                    excel_path = archive_dir / excel_filename
                    
                    table_data = self.fetch_table_data(table)
                    row_count = len(table_data) if table_data else 0
                    
                    if table_data:
                        data_frame = pd.DataFrame(table_data)
                        data_frame.to_excel(str(excel_path), index=False)
                    
                    # Export to JSON
                    json_filename = f"{table}_{datetime.now().strftime('%H%M%S')}.json"
                    json_path = archive_dir / json_filename
                    
                    json_data = []
                    if table_data:
                        for row in table_data:
                            json_row = {}
                            for key, value in row.items():
                                if isinstance(value, (datetime, pd.Timestamp)):
                                    json_row[key] = value.isoformat()
                                elif hasattr(value, '__dict__'):
                                    json_row[key] = str(value)
                                else:
                                    json_row[key] = value
                            json_data.append(json_row)
                    
                    with open(json_path, 'w', encoding='utf-8') as json_file:
                        json.dump(json_data, json_file, ensure_ascii=False, indent=2, default=str)
                    
                    # Drop table
                    drop_result = self.drop_database_table(table)
                    
                    if drop_result:
                        archive_results.append({
                            'table': table,
                            'backup_file': os.path.basename(backup_file),
                            'excel_file': excel_filename,
                            'json_file': json_filename,
                            'rows_archived': row_count,
                            'status': 'success'
                        })
                        logger.info(f"Table {table} archived successfully")
                    else:
                        archive_results.append(f"Failed to drop table '{table}'")
                        overall_success = False
                        logger.error(f"Error dropping table {table}")
                    
                except Exception as table_error:
                    import traceback
                    error_details = traceback.format_exc()
                    logger.error(f"Error archiving table {table}: {error_details}")
                    archive_results.append(f"Archiving error for '{table}': {str(table_error)}")
                    overall_success = False
            
            # Create archive information file
            info_filename = f"archive_info_{datetime.now().strftime('%H%M%S')}.json"
            info_path = archive_dir / info_filename
            
            successful_tables = [r for r in archive_results if isinstance(r, dict) and r.get('status') == 'success']
            
            archive_info = {
                'timestamp': datetime.now().isoformat(),
                'tables_archived': len(successful_tables),
                'total_tables': len(table_list),
                'results': archive_results
            }
            
            with open(info_path, 'w', encoding='utf-8') as info_file:
                json.dump(archive_info, info_file, ensure_ascii=False, indent=2, default=str)
            
            if overall_success:
                return True, {
                    'message': f"Archiving completed successfully",
                    'archive_dir': str(archive_dir),
                    'tables_archived': len(successful_tables),
                    'total_tables': len(table_list),
                    'details': archive_results
                }
            else:
                if successful_tables:
                    return True, {
                        'message': f"Partial archiving completed. Successful: {len(successful_tables)} of {len(table_list)}",
                        'archive_dir': str(archive_dir),
                        'tables_archived': len(successful_tables),
                        'total_tables': len(table_list),
                        'details': archive_results
                    }
                else:
                    return False, "No tables were successfully archived"
                
        except Exception as archive_error:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"Archive processing exception: {error_details}")
            return False, f"Archiving exception: {str(archive_error)}"
    
    def archive_all_database_tables(self):
        """Archive all database tables"""
        all_tables = self.get_table_names()
        return self.archive_database_tables(all_tables)
    
    # ============ FILE LISTING ============
    
    def list_backup_files(self):
        """List all backup files"""
        backup_files = []
        for backup_dir in self.storage_dirs['backups'].iterdir():
            if backup_dir.is_dir():
                for file in backup_dir.glob("*.backup"):
                    backup_files.append({
                        'path': str(file),
                        'name': file.name,
                        'folder': backup_dir.name,
                        'size': file.stat().st_size,
                        'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                    })
        return sorted(backup_files, key=lambda x: x['folder'], reverse=True)
    
    def list_export_files(self):
        """List all export files"""
        export_files = []
        for export_dir in self.storage_dirs['exports'].iterdir():
            if export_dir.is_dir():
                for file in export_dir.glob("*"):
                    if file.is_file():
                        export_files.append({
                            'path': str(file),
                            'name': file.name,
                            'folder': export_dir.name,
                            'size': file.stat().st_size,
                            'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                        })
        return sorted(export_files, key=lambda x: x['folder'], reverse=True)
    
    def list_archive_files(self):
        """List all archive files"""
        archive_files = []
        for archive_dir in self.storage_dirs['archives'].iterdir():
            if archive_dir.is_dir():
                for file in archive_dir.glob("*"):
                    if file.is_file():
                        archive_files.append({
                            'path': str(file),
                            'name': file.name,
                            'folder': archive_dir.name,
                            'size': file.stat().st_size,
                            'modified': datetime.fromtimestamp(file.stat().st_mtime).isoformat()
                        })
        return sorted(archive_files, key=lambda x: x['folder'], reverse=True)
    
    def run_sql_query(self, query_text, query_params=None):
        """Generic SQL query execution"""
        return self.run_database_query(query_text, query_params)

# Global instance
database_manager = DatabaseManager()