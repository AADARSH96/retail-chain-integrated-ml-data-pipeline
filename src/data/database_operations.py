import sqlite3
import json
from typing import List, Tuple


# Load configuration from JSON file
def load_config(config_path='config/config.json'):
    with open(config_path, 'r') as f:
        config = json.load(f)
    return config


class SQLiteDBAdmin:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Connect to the SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        print(f"Connected to database: {self.db_path}")

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            print(f"Connection to database {self.db_path} closed.")

    def execute_query(self, query: str, params: Tuple = ()) -> List[Tuple]:
        """
        Execute a query and return the results.

        :param query: The SQL query to execute.
        :param params: Optional parameters for the query.
        :return: A list of tuples representing the query results.
        """
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(query, params)
            results = cur.fetchall()
            print(f"Query executed: {query}")
            return results

    def execute_non_query(self, query: str, params: Tuple = ()):
        """
        Execute a non-query SQL command (e.g., INSERT, UPDATE, DELETE).

        :param query: The SQL command to execute.
        :param params: Optional parameters for the command.
        """
        with self.conn:
            cur = self.conn.cursor()
            cur.execute(query, params)
            self.conn.commit()
            print(f"Non-query executed: {query}")

    def create_table(self, table_name: str, columns: List[str]):
        """
        Create a new table with the given columns.

        :param table_name: The name of the table to create.
        :param columns: A list of column definitions (e.g., "id INTEGER PRIMARY KEY").
        """
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        self.execute_non_query(query)
        print(f"Table created: {table_name}")

    def drop_table(self, table_name: str):
        """
        Drop a table from the database.

        :param table_name: The name of the table to drop.
        """
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.execute_non_query(query)
        print(f"Table dropped: {table_name}")

    def backup_database(self, backup_path: str):
        """
        Backup the database to a new file.

        :param backup_path: The path where the backup will be saved.
        """
        with sqlite3.connect(backup_path) as backup_conn:
            self.conn.backup(backup_conn)
            print(f"Database backed up to: {backup_path}")

    def list_tables(self) -> List[str]:
        """
        List all tables in the database.

        :return: A list of table names.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        tables = self.execute_query(query)
        table_list = [table[0] for table in tables]
        print(f"Tables in database: {table_list}")
        return table_list

    def table_info(self, table_name: str) -> List[Tuple]:
        """
        Get information about the columns in a table.

        :param table_name: The name of the table to describe.
        :return: A list of tuples with column information.
        """
        query = f"PRAGMA table_info({table_name})"
        info = self.execute_query(query)
        print(f"Table info for {table_name}: {info}")
        return info


def create_tables(config):
    db_admin = SQLiteDBAdmin(config['database']['path'])
    db_admin.connect()

    # Create tables using schemas from the configuration
    for table_name, columns in config['table_schemas'].items():
        db_admin.create_table(table_name, columns)

    # Close the connection
    db_admin.close()
    print("All tables created successfully.")


if __name__ == "__main__":
    config = load_config()
    create_tables(config)
