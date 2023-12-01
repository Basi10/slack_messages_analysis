from postgres_crud import PostgresCRUD  # Assuming you've saved the PostgresCRUD class in postgres_crud.py

def create_tables(name,user_name,user_password,user_host,user_port):
    # Connect to PostgreSQL database
    postgres_crud = PostgresCRUD(
        dbname=name,
        user=user_name,
        password=user_password,
        host=user_host,
        port=user_port
    )

    # Define Channels table columns
    channels_columns = {
        "channel_id": "SERIAL PRIMARY KEY",
        "channel_name": "VARCHAR(255) UNIQUE"
    }
    postgres_crud.create_table("Channels", channels_columns)

    # Define BasicMessageInfo table columns
    basic_message_info_columns = {
        "message_id": "SERIAL PRIMARY KEY",
        "content": "TEXT",
        "sender_name": "VARCHAR(255)",
        "sent_time": "TIMESTAMP"
    }
    postgres_crud.create_table("BasicMessageInfo", basic_message_info_columns)

    # Define MessageDetails table columns
    message_details_columns = {
        "message_id": "SERIAL PRIMARY KEY",
        "distribution_type": "VARCHAR(50)",
        "thread_start_time": "TIMESTAMP",
        "reply_count": "INT",
        "reply_users_count": "INT",
        "reply_user": "VARCHAR(255)",
        "thread_end_time": "TIMESTAMP",
        "channel_id": "INT REFERENCES Channels(channel_id)",
    }
    postgres_crud.create_table("MessageDetails", message_details_columns)

    # Define MessageChannelRelationship table columns
    message_channel_relationship_columns = {
        "message_id": "INT REFERENCES BasicMessageInfo(message_id)",
        "channel_id": "INT REFERENCES Channels(channel_id)",
    }
    postgres_crud.create_table("MessageChannelRelationship", message_channel_relationship_columns)

    # Close the database connection
    postgres_crud.close_connection()

if __name__ == "__main__":
    create_tables()
    print("Tables created successfully.")
