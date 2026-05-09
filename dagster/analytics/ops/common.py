from dagster import op, get_dagster_logger
from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.dialects import postgresql

@op
def print_op(data: list[dict]) -> None:
    """Prints a list of dictionary"""
    print(data)


def upsert_to_database(
    engine: Engine,
    data: list[dict],
    table: Table,
    metadata: MetaData,
) -> None:
    """Upserts data into the target database.

    Args:
        engine: SQLAlchemy Engine (ya construido)
        data: the transformed data
    """

    metadata.create_all(engine)

    key_columns = [
        pk_column.name for pk_column in table.primary_key.columns.values()
    ]

    insert_statement = postgresql.insert(table).values(data)

    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={
            c.key: c
            for c in insert_statement.excluded
            if c.key not in key_columns
        },
    )

    with engine.begin() as connection:
        try:
            result = connection.execute(upsert_statement)
            logger = get_dagster_logger()
            logger.info(f"Upsert ejecutado en tabla '{table.name}' | filas afectadas: {result.rowcount}")
        except Exception as e:
            raise Exception(f"Failed to upsert to database: {e}")