import psycopg2
from psycopg2 import sql

class PostgresCRUD:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.connection.cursor()

    def create_table(self, table_name, columns):
        create_table_query = sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(
                sql.SQL("{} {}").format(
                    sql.Identifier(column_name),
                    sql.SQL(column_type)
                ) for column_name, column_type in columns.items()
            )
        )
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def insert_data(self, table_name, data):
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(column) for column in data.keys()),
            sql.SQL(", ").join(sql.Literal(value) for value in data.values())
        )
        self.cursor.execute(insert_query)
        self.connection.commit()

    def select_data(self, table_name, columns="*", condition=None):
        select_query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            sql.Identifier(table_name)
        )
        if condition:
            select_query += sql.SQL(" WHERE {}").format(sql.SQL(condition))
        self.cursor.execute(select_query)
        return self.cursor.fetchall()

    def update_data(self, table_name, data, condition):
        update_query = sql.SQL("UPDATE {} SET {} WHERE {}").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.SQL("{} = {}").format(
                sql.Identifier(column), sql.Literal(value)
            ) for column, value in data.items()),
            sql.SQL(condition)
        )
        self.cursor.execute(update_query)
        self.connection.commit()

    def delete_data(self, table_name, condition):
        delete_query = sql.SQL("DELETE FROM {} WHERE {}").format(
            sql.Identifier(table_name),
            sql.SQL(condition)
        )
        self.cursor.execute(delete_query)
        self.connection.commit()

    def insert_collection(self, table_name, data):
        insert_query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({})"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(column) for column in data.keys()),
            sql.SQL(", ").join(sql.Literal(value) for value in data.values())
        )
        self.cursor.execute(insert_query)
        self.connection.commit()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
