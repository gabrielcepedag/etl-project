from dagster import op, OpExecutionContext
from sqlalchemy import Table, MetaData, URL, create_engine
from sqlalchemy.dialects import postgresql

from analytics.resources.postgresql import PostgresqlDatabaseResource

@op
def print_op(data: list[dict]) -> None:
    """Prints a list of dictionary"""
    print(data)

def upsert_to_database(
        postgres_conn: PostgresqlDatabaseResource,
        data: list[dict],
        table: Table,
        metadata: MetaData
    ) -> None:
    """Upserts data into the target database.

    Args:
        postgres_conn: a PostgresqlDatabaseResource object
        data: the transformed data
    """
    connection_url = URL.create(
        drivername="postgresql+pg8000",
        username=postgres_conn.username,
        password=postgres_conn.password,
        host=postgres_conn.host_name,
        port=postgres_conn.port,
        database=postgres_conn.database_name,
    )
    engine = create_engine(connection_url)
    metadata.create_all(engine)
    key_columns = [
        pk_column.name for pk_column in table.primary_key.columns.values()
    ]
    insert_statement = postgresql.insert(table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={
            c.key: c for c in insert_statement.excluded if c.key not in key_columns
        },
    )
    with engine.begin() as connection:
        try:
            result = connection.execute(upsert_statement)
        except Exception as e:
            raise Exception(f"Failed to upsert to database, {e}")
