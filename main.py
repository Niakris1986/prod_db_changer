import logging
import sys
import unittest
from typing import List, Dict

import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseSynchronizer:
    """
    A class for transferring changes from the test db to the prod db
    without damaging the existing data.
    """

    def __init__(self, test_conn_params: Dict, prod_conn_params: Dict):
        """
        :param test_conn_params: connection parameters with test db
        :param prod_conn_params: connection parameters with prod db
        """
        self.test_conn_params = test_conn_params
        self.prod_conn_params = prod_conn_params

    def synchronize(self):
        """
        The main method to start synchronization: structure and data.
        """
        logger.info("Starting the synchronization process")
        self.sync_schema()
        self.sync_reference_tables()
        self.sync_data()
        logger.info("Completing the synchronization process")

    def sync_schema(self):
        """
        Schema synchronization:
         - Check if tables exist in the prod db; create them if they don't exist.
         - Compare columns, their types, add missing ones.
         - Do not touch tables/columns in the prod database to avoid data corruption.
        """
        logger.info("Synchronization of db structure")
        with self._get_connection(self.test_conn_params) as test_conn, \
                self._get_connection(self.prod_conn_params) as prod_conn:

            # check which tables are in the test and which are in prod db
            test_tables = self._get_tables(test_conn)
            prod_tables = self._get_tables(prod_conn)

            # 1. Creating tables that are not present in prod db
            missing_tables = [t for t in test_tables if t not in prod_tables]
            for table_name in missing_tables:
                logger.info(f"Table {table_name} is not in prod db. Creating table.")
                create_ddl = self._get_create_table_ddl(test_conn, table_name)
                with prod_conn.cursor() as cursor:
                    cursor.execute(create_ddl)
                prod_conn.commit()

            # 2. For existing tables we compare the columns
            common_tables = [t for t in test_tables if t in prod_tables]
            for table_name in common_tables:
                self._sync_table_structure(test_conn, prod_conn, table_name)

    def sync_reference_tables(self):
        """
         Synchronization of reference tables.
        In the example - just one table - 'some_ref_table',
        """
        logger.info("Synchronization of reference tables")
        with self._get_connection(self.test_conn_params) as test_conn, \
                self._get_connection(self.prod_conn_params) as prod_conn:
            # suppose we have a list of reference tables that we want to synchronize
            reference_tables = ["some_ref_table"]

            for ref_table in reference_tables:
                test_records = self.load_reference_data(test_conn, ref_table)
                prod_records = self.load_reference_data(prod_conn, ref_table)

                # Calculating what needs to be added and what needs to be updated.
                to_insert = self.calculate_inserts(test_records, prod_records)
                to_update = self.calculate_updates(test_records, prod_records)

                logger.info(
                    f"Тable {ref_table}: for adding {len(to_insert)} entries, for updating {len(to_update)} entries.")

                self.insert_records(prod_conn, ref_table, to_insert)
                self.update_records(prod_conn, ref_table, to_update)

    def _get_connection(self, conn_params: Dict):
        """
        Creating and returning the db connection.
        """
        conn = psycopg2.connect(**conn_params)
        return conn

    def sync_data(self):
        """
        Additional logic for updating data in other tables.
        for example:
         - updating tables linked by foreign keys
         - Populating them with records
         - Or run specific migrations
        """
        logger.info("Synchronizing the rest of the data (if necessary).")

        # Example of specific migration for the 'orders' table.
        logger.info("Applying a specific data migration for 'orders' table.")

        with self._get_connection(self.test_conn_params) as test_conn, \
                self._get_connection(self.prod_conn_params) as prod_conn:
            # 1. Loading data from the test and combat database
            test_orders = self.load_reference_data(test_conn, 'orders')
            prod_orders = self.load_reference_data(prod_conn, 'orders')

            # 2. Calculate which records to insert and which to update
            to_insert = self.calculate_inserts(test_orders, prod_orders)
            to_update = self.calculate_updates(test_orders, prod_orders)

            logger.info(f"'orders' table: {len(to_insert)} records to insert, {len(to_update)} to update.")

            # 3. Inserting and updating.
            self.insert_records(prod_conn, 'orders', to_insert)
            self.update_records(prod_conn, 'orders', to_update)

        logger.info("Custom data migration for 'orders' table completed.")

    def _get_tables(self, conn) -> List[str]:
        """
        Get a list of tables (only public schemas to simplify) in the passed database.
        Can be adapted to other schemas.
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE';
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            tables = [row[0] for row in cursor.fetchall()]
        return tables

    def _get_create_table_ddl(self, conn, table_name: str) -> str:
        """
        Get the DDL (CREATE TABLE ...) from the test db to then
        reproduce it in the prod db. (example)
        also we can use 'pg_dump --schema-only'.
        """
        # simplified example:
        ddl = f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, dummy_field VARCHAR(100));"
        return ddl

    def _sync_table_structure(self, test_conn, prod_conn, table_name: str):
        """
        Comparing the table structure in test and prod db. Adding missing columns,
        changing the column type if necessary (if it is safe).
        """
        test_columns = self._get_table_columns(test_conn, table_name)
        prod_columns = self._get_table_columns(prod_conn, table_name)

        # test_columns, prod_columns: {column_name: column_type, ...}
        for col_name, col_type in test_columns.items():
            if col_name not in prod_columns:
                logger.info(f"In table {table_name} column {col_name} is absent. Adding column.")
                self._add_column(prod_conn, table_name, col_name, col_type)
            else:
                prod_type = prod_columns[col_name]
                if col_type != prod_type:
                    logger.info(
                        f"In table {table_name} the column {col_name} has type {prod_type},\
                         but must be {col_type}. Bringing valid type.")
                    self._alter_column_type(prod_conn, table_name, col_name, col_type)

    def _get_table_columns(self, conn, table_name: str) -> Dict[str, str]:
        """
        Getting the dictionary {column_name: column_type} for the specified table.
        Simplified version, does not take into account varchar length, nullable, etc.
        Can be augmented as needed.
        """
        query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
                AND table_name = %s
            ORDER BY ordinal_position;
        """
        with conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            rows = cursor.fetchall()
            # example: {'id': 'integer', 'dummy_field': 'character varying'}
            columns = {row[0]: row[1] for row in rows}
        return columns

    def _add_column(self, conn, table_name: str, column_name: str, column_type: str):
        """
        Adding a column with the specified name and type.
        Here for simplicity is an example for some types.
        """
        with conn.cursor() as cursor:
            # Simplified mapping logic of types
            pg_type = self._map_type_to_postgres(column_type)
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {pg_type};"
            cursor.execute(alter_query)
        conn.commit()

    def _alter_column_type(self, conn, table_name: str, column_name: str, column_type: str):
        """
        Changing the column type if necessary.
        """
        with conn.cursor() as cursor:
            pg_type = self._map_type_to_postgres(column_type)
            # simple example below.
            alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {pg_type};"
            cursor.execute(alter_query)
        conn.commit()

    def _map_type_to_postgres(self, data_type: str) -> str:
        """
        Simplified type mapping returning a string for DDL.
        """
        # data_type is returning, for example, as 'integer', 'character varying', 'boolean', etc.
        if data_type == 'character varying':
            return 'VARCHAR(255)'
        if data_type == 'integer':
            return 'INTEGER'
        if data_type == 'boolean':
            return 'BOOLEAN'
        return data_type.upper()

    def load_reference_data(self, conn, table_name: str) -> List[Dict]:
        """
        Get data from a reference table as a list of dictionaries.
        It is important that the table should have some pk (id)
        or unique key, otherwise updates will have to be implemented differently.
        """
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table_name}")
            columns = [desc[0] for desc in cursor.description]
            records = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                records.append(record)
            return records

    def calculate_inserts(self, test_records: List[Dict], prod_records: List[Dict]) -> List[Dict]:
        """
        Determine which records do not exist in the prod db by key and return a list for insertion.
        """
        prod_ids = {rec['id'] for rec in prod_records if 'id' in rec}
        to_insert = []
        for r in test_records:
            # Assume there is an 'id' field in each record.
            if r.get('id') not in prod_ids:
                to_insert.append(r)
        return to_insert

    def calculate_updates(self, test_records: List[Dict], prod_records: List[Dict]) -> List[Dict]:
        """
        Determine the records that need to be updated (for example, if the fields are different).
        Simplified here: update all records that have the same 'id',
        but the fields differ by at least one value.
        """
        # Match by id.
        prod_dict = {rec['id']: rec for rec in prod_records if 'id' in rec}
        to_update = []
        for test_rec in test_records:
            test_id = test_rec.get('id')
            if test_id in prod_dict:
                # check the differences in the fields (except 'id').
                different_fields = False
                for k, v in test_rec.items():
                    if k == 'id':
                        continue
                    if k in prod_dict[test_id] and prod_dict[test_id][k] != v:
                        different_fields = True
                        break
                if different_fields:
                    to_update.append(test_rec)
        return to_update

    def insert_records(self, conn, table_name: str, records: List[Dict]):
        """
        Inserting records into the specified table.
        It is assumed that dict keys = column names in db.
        """
        if not records:
            return
        with conn.cursor() as cursor:
            for record in records:
                columns = list(record.keys())
                values = list(record.values())
                col_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(values))
                query = f"INSERT INTO {table_name} ({col_str}) VALUES ({placeholders})"
                cursor.execute(query, values)
            conn.commit()

    def update_records(self, conn, table_name: str, records: List[Dict]):
        """
        Update records by pk.
        For the example we assume that all tables have the 'id' field as pk.
        """
        if not records:
            return
        with conn.cursor() as cursor:
            for record in records:
                if 'id' not in record:
                    continue  # Can't update without an id
                record_id = record['id']
                # Forming the SET part.
                set_clause_parts = []
                values = []
                for k, v in record.items():
                    if k == 'id':
                        continue
                    set_clause_parts.append(f"{k} = %s")
                    values.append(v)
                if not set_clause_parts:
                    continue
                set_clause = ', '.join(set_clause_parts)
                query = f"UPDATE {table_name} SET {set_clause} WHERE id = %s"
                values.append(record_id)
                cursor.execute(query, values)
            conn.commit()


if __name__ == "__main__":

    if "test" in sys.argv:
        # enter command 'python main.py test' from terminal
        tests = unittest.defaultTestLoader.discover('.', pattern='tests.py')
        runner = unittest.TextTestRunner()
        result = runner.run(tests)
        sys.exit(not result.wasSuccessful())
    else:
        # example with dummy parameters
        test_conn_params = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'host': 'localhost',
            'port': 5432
        }

        prod_conn_params = {
            'dbname': 'prod_db',
            'user': 'prod_user',
            'password': 'prod_pass',
            'host': 'localhost',
            'port': 5432
        }

        synchronizer = DatabaseSynchronizer(test_conn_params, prod_conn_params)
        synchronizer.synchronize()
