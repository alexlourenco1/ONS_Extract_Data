from google.cloud.bigquery import TimePartitioning, Client, SchemaField, Table, Dataset, LoadJobConfig, \
    WriteDisposition, SourceFormat, QueryJobConfig
from google.cloud.exceptions import NotFound
from ..Enum.BigQueryEnum import WriteOption

__bgq_client = Client()


# region Create

def create_dataset(dataset_name: str, exists_ok=True) -> None:
    """
    Create a new dataset
    :param dataset_name: Name of the dataset
    :param exists_ok: If True, ignore "already exists" errors when creating the dataset.
    """
    dataset = Dataset(dataset_name)
    __bgq_client.create_dataset(dataset, exists_ok=exists_ok)


def create_schema(field_list: list) -> list:
    """
    Create a list of schema fields
    :param field_list: List of dicts containing schema structure
    :return: List of BigQuery.SchemaFields
    """
    schema = []
    for field in field_list:
        if 'fields' in field:
            fields = create_schema(field['fields'])
            schema.append(
                SchemaField(description=field['description'],
                            mode=field['mode'],
                            field_type=field['type'],
                            name=field['name'],
                            fields=fields))
        else:
            schema.append(
                SchemaField(description=field['description'],
                            mode=field['mode'],
                            field_type=field['type'],
                            name=field['name']))

    return schema


def create_table(dataset: str, table: str, table_schema: list = None, partition: TimePartitioning = None) -> None:
    """
    Creates a table on the informed dataset
    :param dataset: Dataset name
    :param table: Table name
    :param table_schema: Columns schema
    :param partition: Time Partition
    :return:
    """
    dataset = __bgq_client.get_dataset(dataset)
    table_ref = dataset.table(table)

    if table_schema is not None:
        schema = create_schema(table_schema)
        table = Table(table_ref, schema=schema)
    else:
        table = Table(table_ref)

    if partition:
        table.time_partitioning = partition

    __bgq_client.create_table(table)


def create_empty_column(dataset_name: str, table_name: str, column_name: str, column_type: str) -> list:
    """
    Add a new nullable empty column on informed table
    :param dataset_name: Name of dataset
    :param table_name: Name of table
    :param column_name: Name of column
    :param column_type: Type of column
    :return: New table schema
    """
    dataset = __bgq_client.get_dataset(dataset_name)
    table = __bgq_client.get_table(dataset.table(table_name))

    original_schema = table.schema
    new_schema = original_schema[:]
    new_schema.append(SchemaField(column_name, column_type, mode='NULLABLE'))

    table.schema = new_schema
    __bgq_client.update_table(table, ['schema'])  # API request

    return table.schema


# endregion


# region Delete

def delete_dataset(dataset_name: str, delete_contents: bool = False, not_found_ok=True):
    """
    Delete dataset
    :param dataset_name: Name of dataset
    :param delete_contents: If False and the dataset contains tables, the request will fail.
    :param not_found_ok: If True, ignore "not found" errors when deleting the dataset.
    :return:
    """
    dataset = __bgq_client.get_dataset(dataset_name)

    __bgq_client.delete_dataset(dataset, delete_contents=delete_contents, not_found_ok=not_found_ok)


def delete_table(dataset_name: str, table_name: str):
    dataset = __bgq_client.get_dataset(dataset_name)
    table = __bgq_client.get_table(dataset.table(table_name))
    __bgq_client.delete_table(table)  # API request


# endregion


# region List

def list_dataset() -> list:
    """
    Return a list of dataset
    """
    return list(map(lambda x: x.dataset_id, __bgq_client.list_datasets()))


def list_tables(dataset_name: str) -> list:
    """
    Retrieve a list of tables existent in the informed dataset
    :param dataset_name: Name of the dataset
    :return: List with tables name
    """
    return list(map(lambda table: table.table_id, __bgq_client.list_tables(dataset_name)))


def list_rows(dataset_name: str, table_name: str, start_index: int = 0, max_results: int = 1000) -> list:
    """
    Retrieve all columns data from a table.
    :param dataset_name: Name of dataset
    :param table_name: Name of table
    :param start_index: Start index of rows
    :param max_results: Max number of retrieved rows
    :return: List of rows where each row is a dict
    """
    dataset = __bgq_client.get_dataset(dataset_name)

    table = __bgq_client.get_table(dataset.table(table_name))

    # Use the start index to load an arbitrary portion of the table
    rows = __bgq_client.list_rows(table, start_index=start_index, max_results=max_results)

    result = []

    field_names = [f.name for f in rows.schema]

    for row in rows:
        result.append(dict(zip(field_names, row)))

    return result


# endregion


# region Metadata/Information

def dataset_exists(dataset_name: str) -> bool:
    """
    Verifies if a dataset exists
    :param dataset_name: Name of the dataset
    :return: True if dataset exists, otherwise returns false.
    """

    try:
        __bgq_client.get_dataset(dataset_name)
        return True
    except NotFound:
        return False


def table_exists(dataset_name: str, table_name: str) -> bool:
    """
    Verifies if a table exists
    :param dataset_name: Name of the dataset
    :param table_name: Name of the table
    :return: True if table exists, otherwise returns false.
    """

    try:
        dataset_id = __bgq_client.get_dataset(dataset_name)
        __bgq_client.get_table(dataset_id.table(table_name))
        return True
    except NotFound:
        return False


def get_table_information(dataset_name: str, table_name: str) -> dict:
    """
    Retrieve table information by name
    :param dataset_name: Name of dataset
    :param table_name: Name of table
    :return: Dict containing dataset information
    """

    dataset = __bgq_client.get_dataset(dataset_name)
    table = __bgq_client.get_table(dataset.table(table_name))
    return {
        "dataset_id": table.dataset_id,
        "table_id": table.table_id,
        "schema": table.schema,
        "description": table.description,
        "rows": table.num_rows
    }


def get_dataset_information(dataset_name: str) -> dict:
    """
    Retrieve dataset information by name
    :param dataset_name: Name of dataset
    :return: Dict containing dataset information
    """

    dataset = __bgq_client.get_dataset(dataset_name)

    return {
        "dataset_id": dataset.dataset_id,
        "description": dataset.description,
        "Labels": dataset.labels.items(),
        "Tables": list(map(lambda table: table.table_id, __bgq_client.list_tables(dataset)))
    }


# endregion


# region Ingest

def load_from_gcs(dataset_name: str, table_name: str, schema: list, gcs_uri: str, skip_header_line: int = 1,
                  delimiter: str = ';', auto_detect_schema: bool = False,
                  write_disposition: WriteOption = WriteOption.WriteTruncate):
    dataset_ref = __bgq_client.get_dataset(dataset_name)
    job_config = LoadJobConfig()
    job_config.schema = create_schema(field_list=schema)
    job_config.skip_leading_rows = skip_header_line
    job_config.field_delimiter = delimiter
    job_config.autodetect = auto_detect_schema

    write_option_swtich = {
        1: WriteDisposition.WRITE_EMPTY,
        2: WriteDisposition.WRITE_APPEND,
        3: WriteDisposition.WRITE_TRUNCATE
    }

    job_config.write_disposition = write_option_swtich.get(write_disposition.value)

    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = SourceFormat.CSV

    load_job = __bgq_client.load_table_from_uri(
        gcs_uri,
        dataset_ref.table(table_name),
        job_config=job_config)  # API request

    assert load_job.job_type == 'load'

    load_job.result()  # Waits for table load to complete.

    assert load_job.state == 'DONE'


def load_from_file(dataset_name: str, table_name: str, schema: list, source_file_path: str,
                   skip_header_line: int = 1, auto_detect_schema: bool = False, delimiter: str = ';',
                   write_disposition: WriteOption = WriteOption.WriteTruncate):
    job_config = LoadJobConfig()
    job_config.source_format = SourceFormat.CSV
    job_config.schema = create_schema(schema)
    job_config.skip_leading_rows = skip_header_line
    job_config.field_delimiter = delimiter
    job_config.autodetect = auto_detect_schema

    write_option_switch = {
        1: WriteDisposition.WRITE_EMPTY,
        2: WriteDisposition.WRITE_APPEND,
        3: WriteDisposition.WRITE_TRUNCATE
    }

    job_config.write_disposition = write_option_switch.get(write_disposition.value)

    dataset = __bgq_client.get_dataset(dataset_name)
    table = dataset.table(table_name)

    with open(source_file_path, 'rb') as source_file:
        job = __bgq_client.load_table_from_file(
            source_file,
            table,
            location=dataset.location,
            job_config=job_config)

        job.result()  # Waits for table load to complete.

        print('Loaded {} rows into {}:{}.'.format(
            job.output_rows, dataset_name, table_name))


def load_json_from_file(dataset_name: str, table_name: str, schema, file,
                        write_disposition: WriteOption = WriteOption.WriteTruncate):
    dataset = __bgq_client.get_dataset(dataset_name)
    job_config = LoadJobConfig()
    job_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = create_schema(schema)

    write_option_switch = {
        1: WriteDisposition.WRITE_EMPTY,
        2: WriteDisposition.WRITE_APPEND,
        3: WriteDisposition.WRITE_TRUNCATE
    }

    job_config.write_disposition = write_option_switch.get(write_disposition.value)

    # with open(file, 'rb') as source_file:
    source_file = file
    job = __bgq_client.load_table_from_file(
        source_file,
        dataset.table(table_name),
        location=dataset.location,  # Must match the destination dataset location.
        job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_name, table_name))


def table_insert_rows(dataset_name: str, table_name: str, rows_to_insert: list):
    dataset_id = __bgq_client.get_dataset(dataset_name)

    table = __bgq_client.get_table(dataset_id.table(table_name))

    errors = __bgq_client.insert_rows(table, rows_to_insert)

    assert errors == []


# endregion


# region Execute

def relax_column(dataset_name: str, table_name: str, column_name: str) -> list:
    dataset = __bgq_client.get_dataset(dataset_name)

    table = __bgq_client.get_table(dataset.table(table_name))

    new_schema = []

    for column in table.schema:
        mode = 'NULLABLE' if column.name == column_name else column.mode
        new_schema.append(SchemaField(column.name, column.field_type, mode=mode))

    table.schema = new_schema

    __bgq_client.update_table(table, ['schema'])

    return table.schema


def execute_query(query: str) -> list:
    query_job = __bgq_client.query(query)

    # execute query
    result_rows = query_job.result()

    # Get field names from schema
    field_names = [f.name for f in result_rows.schema]

    result = []

    for row in result_rows:
        # Zip field and results in a dict
        result.append(dict(zip(field_names, row)))

    return result


def execute_query_to_table(dataset_name: str, table_name: str, query: str,
                           location: str = 'southamerica-east1', legacy_sql: bool = False,
                           write_disposition: WriteOption = WriteOption.WriteTruncate):
    dataset = __bgq_client.get_dataset(dataset_name)
    table = dataset.table(table_name)

    write_option_switch = {
        1: WriteDisposition.WRITE_EMPTY,
        2: WriteDisposition.WRITE_APPEND,
        3: WriteDisposition.WRITE_TRUNCATE
    }
    job_config = QueryJobConfig()

    job_config.write_disposition = write_option_switch.get(write_disposition.value)
    job_config.use_legacy_sql = legacy_sql
    job_config.destination = table
    __bgq_client.query(query, location=location, job_config=job_config)


def copy_table(src_dataset: str, src_table: str, dest_table: str, dest_dataset: str = None):
    source_dataset = __bgq_client.get_dataset(src_dataset)
    source_table = source_dataset.table(src_table)

    destiny_dataset = __bgq_client.get_dataset(src_dataset if dest_dataset is None else dest_dataset)
    destiny_table = destiny_dataset.table(dest_table)

    job = __bgq_client.copy_table(
        source_table,
        destiny_table,
        location=source_dataset.location)

    job.result()  # Waits for job to complete.

    assert job.state == 'DONE'

# endregion
