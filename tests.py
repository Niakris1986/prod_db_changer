import logging
import unittest
from unittest.mock import patch, MagicMock

from main import DatabaseSynchronizer

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(name)s: %(message)s')
console_handler.setFormatter(formatter)

# Configure the root logger overwriting the previous configuration
logging.basicConfig(level=logging.DEBUG, handlers=[console_handler], force=True)

logger = logging.getLogger(__name__)


class TestDatabaseSynchronizer(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Starting the test method: %s", self._testMethodName)

    @patch('psycopg2.connect')
    def test_sync_schema(self, mock_connect):
        """
        Check that _sync_table_structure is called correctly when calling sync_schema
        for some_ref_table table in the test db
        """
        self.logger.debug("Starting test_sync_schema")
        mock_test_conn = MagicMock()
        mock_prod_conn = MagicMock()

        mock_test_conn.__enter__.return_value = mock_test_conn
        mock_prod_conn.__enter__.return_value = mock_prod_conn
        mock_connect.side_effect = [mock_test_conn, mock_prod_conn]

        synchronizer = DatabaseSynchronizer({'dbname': 'test'}, {'dbname': 'prod'})

        synchronizer._get_tables = MagicMock(return_value=['some_ref_table'])
        synchronizer._get_create_table_ddl = MagicMock(return_value="CREATE TABLE some_ref_table (...);")
        synchronizer._sync_table_structure = MagicMock()

        self.logger.debug("Calling the synchronizer.sync_schema()")
        synchronizer.sync_schema()

        self.logger.debug("Checking that _sync_table_structure is called exactly once")
        synchronizer._sync_table_structure.assert_called_once_with(
            mock_test_conn, mock_prod_conn, 'some_ref_table'
        )
        # Check that _get_create_table_ddl has not been called,
        # since some_ref_table already exists
        synchronizer._get_create_table_ddl.assert_not_called()
        self.logger.debug("Ending of test_sync_schema")

    @patch('psycopg2.connect')
    def test_sync_reference_tables(self, mock_connect):
        """
        Check synchronization of reference tables:
         - load_reference_data should be called 2 times (for test and prod db)
         - insert_records/update_records are called with the required data
        """
        self.logger.debug("Starting test_sync_reference_tables")
        mock_test_conn = MagicMock()
        mock_prod_conn = MagicMock()
        mock_test_conn.__enter__.return_value = mock_test_conn
        mock_prod_conn.__enter__.return_value = mock_prod_conn
        mock_connect.side_effect = [mock_test_conn, mock_prod_conn]

        synchronizer = DatabaseSynchronizer({'dbname': 'test'}, {'dbname': 'prod'})

        # Подделываем вывод данных из тестовой и боевой БД
        test_data = [
            {'id': 1, 'name': 'TestName1'},
            {'id': 2, 'name': 'TestName2'},
        ]
        prod_data = [
            {'id': 1, 'name': 'OldName'},
        ]
        synchronizer.load_reference_data = MagicMock(side_effect=[test_data, prod_data])
        synchronizer.insert_records = MagicMock()
        synchronizer.update_records = MagicMock()

        self.logger.debug("Calling the synchronizer.sync_reference_tables()")
        synchronizer.sync_reference_tables()

        self.logger.debug("Checking that load_reference_data is called twice")
        self.assertEqual(synchronizer.load_reference_data.call_count, 2)

        self.logger.debug("Checking that insert_records/update_records have been called")
        synchronizer.insert_records.assert_called_once()
        synchronizer.update_records.assert_called_once()

        # Checking that the required entry has been inserted
        args_insert, _ = synchronizer.insert_records.call_args
        # Arguments call_args: (conn, table_name, records_to_insert)
        self.assertEqual(args_insert[1], "some_ref_table")  # table_name
        inserted_records = args_insert[2]
        self.assertEqual(len(inserted_records), 1)
        self.assertEqual(inserted_records[0]['id'], 2)
        self.assertEqual(inserted_records[0]['name'], 'TestName2')

        # the same for update_records
        args_update, _ = synchronizer.update_records.call_args
        updated_records = args_update[2]
        self.assertEqual(len(updated_records), 1)
        self.assertEqual(updated_records[0]['id'], 1)
        self.assertEqual(updated_records[0]['name'], 'TestName1')
        self.logger.debug("Ending of test_sync_reference_tables")

    @patch('psycopg2.connect')
    def test_insert_records(self, mock_connect):
        """
        Testing insert_records:
         - Verify that INSERT occurs with the correct parameters
         - Check that commit is called
        """
        self.logger.debug("Starting test_insert_records")
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        mock_cursor_cm = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor_cm.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value = mock_cursor_cm

        synchronizer = DatabaseSynchronizer({'dbname': 'test'}, {'dbname': 'prod'})

        records_to_insert = [
            {'id': 1, 'name': 'Alpha'},
            {'id': 2, 'name': 'Beta'},
        ]

        self.logger.debug("Calling synchronizer.insert_records(...)")
        synchronizer.insert_records(mock_conn, "my_table", records_to_insert)

        self.logger.debug("Checking that execute is called twice (one for each record)")
        self.assertEqual(mock_cursor.execute.call_count, 2)

        expected_query = "INSERT INTO my_table (id, name) VALUES (%s, %s)"

        # first call
        first_call_args = mock_cursor.execute.call_args_list[0][0]
        self.assertEqual(first_call_args[0], expected_query)
        self.assertEqual(first_call_args[1], [1, 'Alpha'])

        # second call
        second_call_args = mock_cursor.execute.call_args_list[1][0]
        self.assertEqual(second_call_args[0], expected_query)
        self.assertEqual(second_call_args[1], [2, 'Beta'])

        self.logger.debug("Checking that commit has been called")
        mock_conn.commit.assert_called_once()
        self.logger.debug("Ending test_insert_records")

    @patch('psycopg2.connect')
    def test_update_records(self, mock_connect):
        """
        Testing update_records:
         - Check that UPDATE is formed correctly
         - Check that commit is called
        """
        self.logger.debug("Starting test_update_records")
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        mock_cursor_cm = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor_cm.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value = mock_cursor_cm

        synchronizer = DatabaseSynchronizer({'dbname': 'test'}, {'dbname': 'prod'})

        records_to_update = [
            {'id': 10, 'name': 'UpdatedName10', 'desc': 'NewDesc10'},
            {'id': 20, 'name': 'UpdatedName20'},
        ]

        self.logger.debug("Calling synchronizer.update_records(...)")
        synchronizer.update_records(mock_conn, "my_table", records_to_update)

        # Check the number of execute calls
        self.assertEqual(mock_cursor.execute.call_count, 2)

        # first UPDATE
        first_call = mock_cursor.execute.call_args_list[0][0]
        query_1 = first_call[0]
        values_1 = first_call[1]
        self.assertIn("UPDATE my_table SET name = %s, desc = %s WHERE id = %s", query_1)
        self.assertEqual(values_1, ['UpdatedName10', 'NewDesc10', 10])

        # second UPDATE
        second_call = mock_cursor.execute.call_args_list[1][0]
        query_2 = second_call[0]
        values_2 = second_call[1]
        self.assertIn("UPDATE my_table SET name = %s WHERE id = %s", query_2)
        self.assertEqual(values_2, ['UpdatedName20', 20])

        self.logger.debug("Checking that commit has been called")
        mock_conn.commit.assert_called_once()
        self.logger.debug("Ending test_update_records")


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)
