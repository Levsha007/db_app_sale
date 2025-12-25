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

load_dotenv()

class Database:
    def __init__(self):
        self.connection_params = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'database': os.getenv('DB_NAME', 'my_app_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres'),
            'port': os.getenv('DB_PORT', '5432')
        }
        # Путь к утилитам PostgreSQL
        self.pg_dump_path = "pg_dump"
        self.pg_restore_path = "pg_restore"
        
        # Создаем основные папки
        self.base_dirs = self.create_base_directories()
    
    def create_base_directories(self):
        """Создать базовые директории для хранения файлов"""
        base_dirs = {
            'backups': Path("backups"),
            'exports': Path("exports"),
            'archives': Path("archives")
        }
        
        for dir_path in base_dirs.values():
            dir_path.mkdir(exist_ok=True)
        
        return base_dirs
    
    def create_timestamp_dir(self, base_dir):
        """Создать папку с текущей датой и временем"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dir_path = base_dir / timestamp
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path
    
    def get_connection(self, dict_cursor=True):
        """Получить подключение к БД"""
        try:
            conn = psycopg2.connect(
                **self.connection_params,
                cursor_factory=RealDictCursor if dict_cursor else DictCursor
            )
            return conn
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            return None
    
    def execute_query(self, query, params=None, fetch=True):
        """Выполнить SQL запрос"""
        conn = self.get_connection()
        if not conn:
            return None
        
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params or ())
                if fetch and cursor.description:
                    result = cursor.fetchall()
                else:
                    result = None
                conn.commit()
                return result
        except Exception as e:
            conn.rollback()
            print(f"Ошибка выполнения запроса: {e}")
            return None
        finally:
            conn.close()
    
    def get_tables(self):
        """Получить список всех таблиц"""
        try:
            query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """
            result = self.execute_query(query)
            if result:
                return [row['table_name'] for row in result]
            return []
        except Exception as e:
            print(f"Ошибка при получении таблиц: {e}")
            return []
    
    def get_total_records_count(self):
        """Получить общее количество записей во всех таблицах"""
        try:
            tables = self.get_tables()
            total = 0
            for table in tables:
                count = self.get_table_count(table)
                total += count
            return total
        except Exception as e:
            print(f"Ошибка при подсчете всех записей: {e}")
            return 0
    
    def get_table_columns(self, table_name):
        """Получить колонки таблицы"""
        query = """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """
        result = self.execute_query(query, (table_name,))
        return result if result else []
    
    def get_table_constraints(self, table_name):
        """Получить информацию о внешних ключах таблицы"""
        query = """
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
        return self.execute_query(query, (table_name,)) or []
    
    def get_referencing_tables(self, table_name, column_name=None):
        """Получить таблицы, которые ссылаются на данную таблицу"""
        query = """
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
        return self.execute_query(query, (table_name, column_name, column_name)) or []
    
    def get_table_data(self, table_name, limit=None, offset=0):
        """Получить данные из таблицы"""
        try:
            # Используем кавычки для имен таблиц
            if limit:
                query = f'SELECT * FROM "{table_name}" LIMIT %s OFFSET %s'
                return self.execute_query(query, (limit, offset))
            else:
                query = f'SELECT * FROM "{table_name}"'
                return self.execute_query(query)
        except Exception as e:
            print(f"Ошибка при получении данных таблицы {table_name}: {e}")
            return []
    
    def table_exists(self, table_name):
        """Проверить существование таблицы"""
        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """
            result = self.execute_query(query, (table_name,))
            return result[0]['exists'] if result else False
        except Exception as e:
            print(f"Ошибка при проверке существования таблицы {table_name}: {e}")
            return False
    
    def get_table_count(self, table_name):
        """Получить количество записей в таблице"""
        try:
            query = f'SELECT COUNT(*) as count FROM "{table_name}"'
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
        except Exception as e:
            print(f"Ошибка при подсчете записей таблицы {table_name}: {e}")
            return 0
    
    def insert_data(self, table_name, data):
        """Вставить данные в таблицу"""
        try:
            columns = ', '.join([f'"{col}"' for col in data.keys()])
            placeholders = ', '.join(['%s'] * len(data))
            query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders}) RETURNING id'
            result = self.execute_query(query, tuple(data.values()))
            return result[0]['id'] if result else None
        except Exception as e:
            print(f"Ошибка при вставке данных в таблицу {table_name}: {e}")
            return None
    
    def update_data(self, table_name, data, condition):
        """Обновить данные в таблице с каскадным обновлением"""
        try:
            conn = self.get_connection()
            if not conn:
                return None
            
            cursor = conn.cursor()
            
            # Получаем старые значения для каскадного обновления
            get_old_values_query = f'SELECT * FROM "{table_name}" WHERE {condition}'
            cursor.execute(get_old_values_query)
            old_rows = cursor.fetchall()
            
            if not old_rows:
                conn.close()
                return False
            
            results = []
            
            for old_row in old_rows:
                old_row_dict = dict(zip([desc[0] for desc in cursor.description], old_row))
                
                # Создаем обновленный словарь данных
                updated_data = old_row_dict.copy()
                for key, value in data.items():
                    if value:  # Обновляем только если передано значение
                        updated_data[key] = value
                
                # Получаем внешние ключи для этой таблицы
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
                
                # Обновляем связанные таблицы если обновляются FK
                for fk in foreign_keys:
                    local_column = fk[0]
                    foreign_table = fk[1]
                    foreign_column = fk[2]
                    
                    # Если обновляется колонка с внешним ключом
                    if local_column in data and data[local_column]:
                        old_value = old_row_dict[local_column]
                        new_value = data[local_column]
                        
                        if old_value != new_value:
                            # Обновляем связанные записи
                            update_related_query = f'UPDATE "{foreign_table}" SET "{foreign_column}" = %s WHERE "{foreign_column}" = %s'
                            cursor.execute(update_related_query, (new_value, old_value))
                
                # Выполняем основное обновление
                set_clause = ', '.join([f'"{k}" = %s' for k in data.keys() if data[k]])
                values = [data[k] for k in data.keys() if data[k]]
                
                # Добавляем условие для конкретной строки
                where_condition = " AND ".join([f'"{k}" = %s' for k in old_row_dict.keys()])
                where_values = list(old_row_dict.values())
                
                update_query = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_condition}'
                
                cursor.execute(update_query, values + where_values)
                results.append(cursor.rowcount > 0)
            
            conn.commit()
            conn.close()
            
            return any(results)
            
        except Exception as e:
            print(f"Ошибка при обновлении данных: {e}")
            return None
    
    def delete_data(self, table_name, condition):
        """Удалить данные из таблицы с каскадным удалением"""
        try:
            conn = self.get_connection()
            if not conn:
                return None
            
            cursor = conn.cursor()
            
            # Получаем таблицы, которые ссылаются на эту таблицу
            referencing_tables = self.get_referencing_tables(table_name)
            
            # Сначала удаляем из дочерних таблиц
            for ref_table in referencing_tables:
                ref_table_name = ref_table['referencing_table']
                ref_column = ref_table['referencing_column']
                
                # Получаем условие для JOIN
                delete_ref_query = f'DELETE FROM "{ref_table_name}" WHERE "{ref_column}" IN (SELECT id FROM "{table_name}" WHERE {condition})'
                cursor.execute(delete_ref_query)
            
            # Теперь удаляем из основной таблиции
            delete_query = f'DELETE FROM "{table_name}" WHERE {condition}'
            cursor.execute(delete_query)
            
            affected_rows = cursor.rowcount
            conn.commit()
            conn.close()
            
            return affected_rows > 0
            
        except Exception as e:
            print(f"Ошибка при удалении данных: {e}")
            return None
    
    def delete_data_safe(self, table_name, condition):
        """Безопасное удаление данных (без каскада)"""
        try:
            # Проверяем наличие зависимых записей
            conn = self.get_connection()
            if not conn:
                return None
            
            cursor = conn.cursor()
            
            # Получаем таблицы, которые ссылаются на эту таблицу
            referencing_tables = self.get_referencing_tables(table_name)
            
            has_dependencies = False
            dependency_info = []
            
            for ref_table in referencing_tables:
                ref_table_name = ref_table['referencing_table']
                ref_column = ref_table['referencing_column']
                
                # Проверяем есть ли зависимые записи
                check_query = f'SELECT COUNT(*) FROM "{ref_table_name}" WHERE "{ref_column}" IN (SELECT id FROM "{table_name}" WHERE {condition})'
                cursor.execute(check_query)
                count = cursor.fetchone()[0]
                
                if count > 0:
                    has_dependencies = True
                    dependency_info.append({
                        'table': ref_table_name,
                        'count': count
                    })
            
            if has_dependencies:
                conn.close()
                return {
                    'success': False,
                    'error': 'Есть зависимые записи',
                    'dependencies': dependency_info
                }
            
            # Если зависимостей нет, удаляем
            delete_query = f'DELETE FROM "{table_name}" WHERE {condition}'
            cursor.execute(delete_query)
            
            affected_rows = cursor.rowcount
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'affected_rows': affected_rows
            }
            
        except Exception as e:
            print(f"Ошибка при безопасном удалении данных: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def drop_table(self, table_name):
        """Удалить таблицу"""
        try:
            query = f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
            conn = self.get_connection(dict_cursor=False)
            if not conn:
                return False
            
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    conn.commit()
                    return True
            except Exception as e:
                conn.rollback()
                print(f"Ошибка при удалении таблицы {table_name}: {e}")
                return False
            finally:
                conn.close()
        except Exception as e:
            print(f"Исключение при удалении таблицы {table_name}: {e}")
            return False
    
    # ==================== НОВЫЙ МЕТОД: УДАЛЕНИЕ ВСЕХ ТАБЛИЦ ====================
    def reset_all_tables(self):
        """Удалить ВСЕ таблицы из базы данных"""
        try:
            conn = self.get_connection(dict_cursor=False)
            if not conn:
                return False, "Ошибка подключения к БД"
            
            cursor = conn.cursor()
            
            # Получаем все таблицы в публичной схеме
            cursor.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)
            tables = cursor.fetchall()
            
            if not tables:
                conn.close()
                return True, "В базе данных нет таблиц"
            
            table_names = [table[0] for table in tables]
            tables_removed = 0
            
            # Удаляем все таблицы каскадно
            for table_name in table_names:
                try:
                    # Отключаем внешние ключи для этой сессии
                    cursor.execute('SET CONSTRAINTS ALL DEFERRED')
                    
                    # Удаляем таблицу с каскадом
                    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    tables_removed += 1
                    print(f"Удалена таблица: {table_name}")
                    
                except Exception as e:
                    print(f"Ошибка при удалении таблицы {table_name}: {e}")
                    conn.rollback()
                    conn.close()
                    return False, f"Ошибка при удалении таблицы {table_name}: {str(e)}"
            
            conn.commit()
            conn.close()
            
            return True, f"Успешно удалено {tables_removed} таблиц из {len(table_names)}"
            
        except Exception as e:
            print(f"Исключение при удалении всех таблиц: {e}")
            return False, f"Ошибка при удалении всех таблиц: {str(e)}"
    
    def export_table_to_excel(self, table_name):
        """Экспорт таблицы в Excel файл"""
        try:
            data = self.get_table_data(table_name, limit=50000)
            if not data:
                return None, "Нет данных для экспорта"
            
            # Создаем папку с текущей датой
            export_dir = self.create_timestamp_dir(self.base_dirs['exports'])
            filename = f"{table_name}_{datetime.now().strftime('%H%M%S')}.xlsx"
            export_path = export_dir / filename
            
            df = pd.DataFrame(data)
            df.to_excel(str(export_path), index=False)
            
            return str(export_path), filename
            
        except Exception as e:
            return None, str(e)
    
    def export_table_to_json(self, table_name):
        """Экспорт таблицы в JSON файл"""
        try:
            data = self.get_table_data(table_name, limit=50000)
            if not data:
                return None, "Нет данных для экспорта"
            
            # Создаем папку с текущей датой
            export_dir = self.create_timestamp_dir(self.base_dirs['exports'])
            filename = f"{table_name}_{datetime.now().strftime('%H%M%S')}.json"
            export_path = export_dir / filename
            
            # Преобразуем данные для JSON сериализации
            json_data = []
            for row in data:
                json_row = {}
                for key, value in row.items():
                    if isinstance(value, (datetime, pd.Timestamp)):
                        json_row[key] = value.isoformat()
                    elif hasattr(value, '__dict__'):
                        json_row[key] = str(value)
                    else:
                        json_row[key] = value
                json_data.append(json_row)
            
            # Сохраняем JSON файл
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2, default=str)
            
            return str(export_path), filename
            
        except Exception as e:
            return None, str(e)
    
    def export_query_results_to_excel(self, result_data, query_name="query_result"):
        """Экспорт результатов запроса во временную таблицу и затем в Excel"""
        try:
            if not result_data:
                return None, "Нет данных для экспорта"
            
            # Создаем уникальное имя для временной таблиции
            temp_table_name = f"temp_export_{datetime.now().strftime('%H%M%S')}"
            
            # Создаем временную таблицу с данными
            conn = self.get_connection(dict_cursor=False)
            if not conn:
                return None, "Ошибка подключения к БД"
            
            try:
                with conn.cursor() as cursor:
                    # Получаем структуру данных из первого элемента
                    first_row = result_data[0]
                    columns = list(first_row.keys())
                    
                    # Создаем временную таблицу
                    column_defs = []
                    for col in columns:
                        column_defs.append(f'"{col}" TEXT')
                    
                    create_table_sql = f"""
                        CREATE TEMPORARY TABLE {temp_table_name} (
                            {', '.join(column_defs)}
                        )
                    """
                    cursor.execute(create_table_sql)
                    
                    # Вставляем данные
                    for row in result_data:
                        placeholders = ', '.join(['%s'] * len(columns))
                        insert_sql = f"""
                            INSERT INTO {temp_table_name} ({', '.join([f'"{c}"' for c in columns])})
                            VALUES ({placeholders})
                        """
                        values = [str(row.get(col, '')) for col in columns]
                        cursor.execute(insert_sql, values)
                    
                    conn.commit()
                    
                    # Экспортируем временную таблицу в Excel
                    export_dir = self.create_timestamp_dir(self.base_dirs['exports'])
                    filename = f"{query_name}_{datetime.now().strftime('%H%M%S')}.xlsx"
                    export_path = export_dir / filename
                    
                    # Получаем данные из временной таблицы
                    cursor.execute(f'SELECT * FROM {temp_table_name}')
                    rows = cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description]
                    
                    # Создаем DataFrame и сохраняем в Excel
                    df = pd.DataFrame(rows, columns=column_names)
                    df.to_excel(str(export_path), index=False)
                    
                    # Удаляем временную таблицу
                    cursor.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
                    conn.commit()
                    
                    return str(export_path), filename
                    
            except Exception as e:
                conn.rollback()
                return None, f"Ошибка при экспорте: {str(e)}"
            finally:
                conn.close()
                
        except Exception as e:
            return None, str(e)
    
    def export_query_to_csv(self, result_data):
        """Экспорт результатов запроса в CSV"""
        try:
            if not result_data:
                return None, "Нет данных для экспорта"
            
            # Создаем временную таблицу и экспортируем через Excel
            return self.export_query_results_to_excel(result_data, "query_result")
            
        except Exception as e:
            return None, str(e)
    
    # ==================== МЕТОДЫ ДЛЯ БЭКАПА ====================
    
    def create_backup(self):
        """
        Создать полный бэкап базы данных в формате .backup
        """
        try:
            db_name = os.getenv('DB_NAME', 'my_app_db')
            db_user = os.getenv('DB_USER', 'postgres')
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = os.getenv('DB_PORT', '5432')
            
            # Создаем папку с текущей датой
            backup_dir = self.create_timestamp_dir(self.base_dirs['backups'])
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = backup_dir / f"backup_{db_name}_{timestamp}.backup"
            
            # Формируем команду pg_dump с флагами для избежания проблемных параметров
            cmd = [
                self.pg_dump_path,
                '-h', db_host,
                '-U', db_user,
                '-p', db_port,
                '-d', db_name,
                '-F', 'c',
                '--no-tablespaces',
                '--no-unlogged-table-data',
                '-f', str(backup_file),
                '-v'
            ]
            
            # Выполняем команду
            env = os.environ.copy()
            env['PGPASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')
            
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                shell=False
            )
            
            if result.returncode == 0:
                return True, str(backup_file), None
            else:
                error_msg = f"Ошибка pg_dump:\n{result.stderr}\n{result.stdout}"
                return False, None, error_msg
                
        except Exception as e:
            error_msg = f"Исключение при создании бэкапа: {str(e)}"
            return False, None, error_msg
    
    def create_table_backup(self, table_name, backup_dir):
        """Создать бэкап отдельной таблицы"""
        try:
            db_user = os.getenv('DB_USER', 'postgres')
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'my_app_db')
            
            backup_file = backup_dir / f"backup_{table_name}_{datetime.now().strftime('%H%M%S')}.backup"
            
            # Формируем команду pg_dump для конкретной таблицы
            cmd = [
                self.pg_dump_path,
                '-h', db_host,
                '-U', db_user,
                '-p', db_port,
                '-d', db_name,
                '-t', table_name,
                '-F', 'c',
                '--no-tablespaces',
                '--no-unlogged-table-data',
                '-f', str(backup_file),
                '-v'
            ]
            
            # Устанавливаем пароль
            env = os.environ.copy()
            env['PGPASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')
            
            # Выполняем команду
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                shell=False
            )
            
            if result.returncode == 0:
                return True, str(backup_file), None
            else:
                error_msg = f"Ошибка pg_dump для таблицы {table_name}:\n{result.stderr}\n{result.stdout}"
                return False, None, error_msg
                
        except Exception as e:
            error_msg = f"Исключение при создании бэкапа таблицы {table_name}: {str(e)}"
            return False, None, error_msg
    
    def restore_backup(self, backup_file):
        """
        Восстановить БД из файла .backup
        """
        try:
            if not os.path.exists(backup_file):
                return False, f"Файл не найден: {backup_file}"
            
            db_user = os.getenv('DB_USER', 'postgres')
            db_host = os.getenv('DB_HOST', 'postgres')
            db_port = os.getenv('DB_PORT', '5432')
            db_name = os.getenv('DB_NAME', 'my_app_db')
            
            # Сначала получаем список всех таблиц и удаляем их
            try:
                conn = psycopg2.connect(**self.connection_params)
                cursor = conn.cursor()
                # Получаем все таблицы
                cursor.execute("""
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                """)
                tables = cursor.fetchall()
                
                # Удаляем все таблицы каскадно
                for table in tables:
                    cursor.execute(f'DROP TABLE IF EXISTS "{table[0]}" CASCADE')
                    print(f"Удалена таблица: {table[0]}")
                
                conn.commit()
                conn.close()
                print(f"Удалено {len(tables)} таблиц перед восстановлением")
            except Exception as e:
                print(f"Ошибка при удалении таблиц: {e}")
            
            # Формируем команду pg_restore с флагом для игнорирования ошибок
            cmd = [
                self.pg_restore_path,
                '-h', db_host,
                '-U', db_user,
                '-p', db_port,
                '-d', db_name,
                '-v',
                '--clean',
                '--if-exists',
                '--no-comments',
                '--no-tablespaces',
                str(backup_file)
            ]
            
            # Устанавливаем пароль
            env = os.environ.copy()
            env['PGPASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')
            
            # Выполняем команду
            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True,
                shell=False
            )
            
            output = result.stdout + result.stderr
            
            # Проверяем наличие ошибки transaction_timeout
            if "unrecognized configuration parameter \"transaction_timeout\"" in output:
                # Игнорируем эту ошибку
                print("Предупреждение: Игнорируется ошибка transaction_timeout")
                return True, "Восстановление выполнено (некоторые предупреждения проигнорированы)"
            
            if result.returncode == 0:
                return True, "Восстановление успешно выполнено"
            else:
                error_msg = f"Ошибка pg_restore:\n{result.stderr}\n{result.stdout}"
                return False, error_msg
                
        except Exception as e:
            error_msg = f"Исключение при восстановлении: {str(e)}"
            return False, error_msg
    
    # ==================== МЕТОДЫ ДЛЯ ЭКСПОРТА ====================
    
    def export_tables_to_excel(self, table_names):
        """Экспорт таблиц в Excel файл (одна таблица - один лист)"""
        try:
            if not table_names:
                return None, "Не выбраны таблицы для экспорта"
            
            # Проверяем существование всех таблиц
            valid_tables = []
            for table in table_names:
                if self.table_exists(table):
                    valid_tables.append(table)
            
            if not valid_tables:
                return None, "Нет существующих таблиц для экспорта"
            
            # Создаем папку с текущей датой
            export_dir = self.create_timestamp_dir(self.base_dirs['exports'])
            filename = f"export_{datetime.now().strftime('%H%M%S')}.xlsx"
            export_path = export_dir / filename
            
            # Создаем Excel файл
            with pd.ExcelWriter(str(export_path), engine='openpyxl') as writer:
                for table in valid_tables:
                    data = self.get_table_data(table, limit=50000)
                    if data:
                        df = pd.DataFrame(data)
                        # Ограничиваем имя листа до 31 символа (ограничение Excel)
                        sheet_name = table[:31]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            return str(export_path), filename
            
        except Exception as e:
            return None, str(e)
    
    def export_tables_to_json(self, table_names):
        """Экспорт таблиц в JSON файл"""
        try:
            if not table_names:
                return None, "Не выбраны таблицы для экспорта"
            
            # Проверяем существование всех таблиц
            valid_tables = []
            for table in table_names:
                if self.table_exists(table):
                    valid_tables.append(table)
            
            if not valid_tables:
                return None, "Нет существующих таблиц для экспорта"
            
            # Создаем папку с текущей датой
            export_dir = self.create_timestamp_dir(self.base_dirs['exports'])
            filename = f"export_{datetime.now().strftime('%H%M%S')}.json"
            export_path = export_dir / filename
            
            result = {}
            for table in valid_tables:
                data = self.get_table_data(table, limit=50000)
                if data:
                    # Преобразуем данные для JSON сериализации
                    json_data = []
                    for row in data:
                        json_row = {}
                        for key, value in row.items():
                            if isinstance(value, (datetime, pd.Timestamp)):
                                json_row[key] = value.isoformat()
                            elif hasattr(value, '__dict__'):
                                json_row[key] = str(value)
                            else:
                                json_row[key] = value
                        json_data.append(json_row)
                    result[table] = json_data
            
            # Сохраняем JSON файл
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            
            return str(export_path), filename
            
        except Exception as e:
            return None, str(e)
    
    def export_all_to_excel(self):
        """Экспорт всех таблиц в Excel"""
        tables = self.get_tables()
        return self.export_tables_to_excel(tables)
    
    def export_all_to_json(self):
        """Экспорт всех таблиц в JSON"""
        tables = self.get_tables()
        return self.export_tables_to_json(tables)
    
    # ==================== МЕТОДЫ ДЛЯ АРХИВАЦИИ ====================
    
    def archive_tables(self, table_names):
        """
        Архивация таблиц - бэкап, экспорт в файлы и удаление из БД
        """
        try:
            if not table_names:
                return False, "Не выбраны таблицы для архивации"
            
            # Проверяем существование таблиц
            valid_tables = []
            for table in table_names:
                if self.table_exists(table):
                    valid_tables.append(table)
            
            if not valid_tables:
                return False, "Нет существующих таблиц для архивации"
            
            # Создаем папку архива с текущей датой
            archive_dir = self.create_timestamp_dir(self.base_dirs['archives'])
            
            results = []
            all_success = True
            
            for table in valid_tables:
                try:
                    # 1. Создаем бэкап таблицы
                    backup_success, backup_file, backup_error = self.create_table_backup(table, archive_dir)
                    
                    if not backup_success:
                        results.append(f"Ошибка при создании бэкапа таблицы '{table}': {backup_error}")
                        all_success = False
                        continue
                    
                    # 2. Экспортируем в Excel
                    excel_filename = f"{table}_{datetime.now().strftime('%H%M%S')}.xlsx"
                    excel_path = archive_dir / excel_filename
                    
                    data = self.get_table_data(table)
                    row_count = len(data) if data else 0
                    
                    if data:
                        df = pd.DataFrame(data)
                        df.to_excel(str(excel_path), index=False)
                    
                    # 3. Экспортируем в JSON
                    json_filename = f"{table}_{datetime.now().strftime('%H%M%S')}.json"
                    json_path = archive_dir / json_filename
                    
                    json_data = []
                    if data:
                        for row in data:
                            json_row = {}
                            for key, value in row.items():
                                if isinstance(value, (datetime, pd.Timestamp)):
                                    json_row[key] = value.isoformat()
                                elif hasattr(value, '__dict__'):
                                    json_row[key] = str(value)
                                else:
                                    json_row[key] = value
                            json_data.append(json_row)
                    
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=2, default=str)
                    
                    # 4. Удаляем таблицу из БД
                    drop_result = self.drop_table(table)
                    
                    if drop_result:
                        results.append({
                            'table': table,
                            'backup_file': os.path.basename(backup_file),
                            'excel_file': excel_filename,
                            'json_file': json_filename,
                            'rows_archived': row_count,
                            'status': 'success'
                        })
                    else:
                        results.append(f"Ошибка при удалении таблицы '{table}'")
                        all_success = False
                    
                except Exception as e:
                    results.append(f"Ошибка при архивации таблицы '{table}': {str(e)}")
                    all_success = False
            
            # Создаем файл с информацией об архивации
            info_filename = f"archive_info_{datetime.now().strftime('%H%M%S')}.json"
            info_path = archive_dir / info_filename
            
            successful_tables = [r for r in results if isinstance(r, dict) and r.get('status') == 'success']
            
            archive_info = {
                'timestamp': datetime.now().isoformat(),
                'tables_archived': len(successful_tables),
                'total_tables': len(valid_tables),
                'results': results
            }
            
            with open(info_path, 'w', encoding='utf-8') as f:
                json.dump(archive_info, f, ensure_ascii=False, indent=2, default=str)
            
            if all_success:
                return True, {
                    'message': f"Архивация выполнена успешно",
                    'archive_dir': str(archive_dir),
                    'tables_archived': len(successful_tables),
                    'total_tables': len(valid_tables),
                    'details': results
                }
            else:
                # Если хотя бы одна таблица успешно заархивирована, считаем частично успешным
                if successful_tables:
                    return True, {
                        'message': f"Архивация частично выполнена. Успешно: {len(successful_tables)} из {len(valid_tables)}",
                        'archive_dir': str(archive_dir),
                        'tables_archived': len(successful_tables),
                        'total_tables': len(valid_tables),
                        'details': results
                    }
                else:
                    return False, "Не удалось заархивировать ни одну таблицу"
                
        except Exception as e:
            return False, f"Исключение при архивации: {str(e)}"
    
    def archive_all_tables(self):
        """Архивация всех таблиц"""
        tables = self.get_tables()
        return self.archive_tables(tables)
    
    def get_backup_files(self):
        """Получить список файлов бэкапов"""
        backup_files = []
        for backup_dir in self.base_dirs['backups'].iterdir():
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
    
    def get_export_files(self):
        """Получить список файлов экспортов"""
        export_files = []
        for export_dir in self.base_dirs['exports'].iterdir():
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
    
    def get_archive_files(self):
        """Получить список файлов архивов"""
        archive_files = []
        for archive_dir in self.base_dirs['archives'].iterdir():
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

# Создаем глобальный экземпляр
db = Database()