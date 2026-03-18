import sqlite3
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def ensure_products_schema(cursor):
    cursor.execute(
        '''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            category TEXT,
            price REAL
        )
        '''
    )

    cursor.execute("PRAGMA table_info(products)")
    existing_columns = {row[1] for row in cursor.fetchall()}
    required_columns = {
        'read_count': 'INTEGER DEFAULT 0',
        'order_count': 'INTEGER DEFAULT 0',
        'bought_count': 'INTEGER DEFAULT 0',
        'appreciation': "TEXT DEFAULT 'Average'",
    }

    for column_name, column_definition in required_columns.items():
        if column_name not in existing_columns:
            cursor.execute(f'ALTER TABLE products ADD COLUMN {column_name} {column_definition}')

def extract_from_sql():
    try:
        db_path = os.path.join(os.path.dirname(__file__), '..', 'storage', 'sample.db')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        ensure_products_schema(cursor)
        cursor.execute('SELECT COUNT(*) FROM products')
        if cursor.fetchone()[0] == 0:
            sample_data = [
                (1, 'Book A', 'Fiction', 10.99, 120, 45, 38, 'Excellent'),
                (2, 'Book B', 'Non-Fiction', 15.50, 85, 31, 24, 'Good'),
                (3, 'Magazine C', 'Magazine', 5.00, 42, 18, 12, 'Average')
            ]
            cursor.executemany(
                '''
                INSERT INTO products (
                    id, name, category, price, read_count, order_count, bought_count, appreciation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                sample_data
            )
            logger.info("Inserted sample data into SQL table.")
        cursor.execute(
            """
            UPDATE products
            SET
                read_count = COALESCE(read_count, 0),
                order_count = COALESCE(order_count, 0),
                bought_count = COALESCE(bought_count, 0),
                appreciation = COALESCE(appreciation, 'Average')
            """
        )
        # Extract data
        df = pd.read_sql_query('SELECT * FROM products', conn)
        conn.commit()
        conn.close()
        output_dir = os.path.join(os.path.dirname(__file__), '..', 'storage', 'raw')
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(os.path.join(output_dir, 'products_sql.csv'), index=False)
        logger.info(f"Extracted {len(df)} products from SQL.")
    except Exception as e:
        logger.error(f"Error in extract_from_sql: {str(e)}")
        raise

if __name__ == "__main__":
    extract_from_sql()
