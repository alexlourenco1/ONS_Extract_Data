from google.cloud import storage

__storage_client = storage.Client()


def create_bucket(bucket_name: str):
    """Creates a new bucket."""
    bucket = __storage_client.create_bucket(bucket_name)
    print('Bucket {} created'.format(bucket.name))


def delete_bucket(bucket_name: str):
    """Deletes a bucket. The bucket must be empty."""
    bucket = __storage_client.get_bucket(bucket_name)
    bucket.delete()
    print('Bucket {} deleted'.format(bucket.name))


def get_bucket_labels(bucket_name: str):
    """Prints out a bucket's labels."""
    bucket = __storage_client.get_bucket(bucket_name)
    labels = bucket.labels
    return labels


def add_bucket_label(bucket_name: str, label_key: str, label_value: str) -> dict:
    """Add a label to a bucket."""
    bucket = __storage_client.get_bucket(bucket_name)
    labels = bucket.labels
    labels[label_key] = label_value
    bucket.labels = labels
    bucket.patch()
    print('Added label {} with value {} on {}.'.format(label_key, label_value, bucket.name))
    return labels


def remove_bucket_label(bucket_name: str, label_key: str) -> dict:
    """Remove a label from a bucket."""
    bucket = __storage_client.get_bucket(bucket_name)
    labels = bucket.labels
    if label_key in labels:
        del labels[label_key]
    bucket.labels = labels
    bucket.patch()
    print('Updated labels on {}.'.format(bucket.name))
    return labels


def list_blobs(bucket_name: str) -> list:
    """Lists all the blobs in the bucket."""
    bucket = __storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()
    return list(map(lambda x: x.name, blobs))


def list_blobs_with_prefix(bucket_name: str, prefix: str, delimiter=None) -> dict:
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        /a/1.txt
        /a/b/2.txt
    If you just specify prefix = '/a', you'll get back:
        /a/1.txt
        /a/b/2.txt
    However, if you specify prefix='/a' and delimiter='/', you'll get back:
        /a/1.txt
    """
    bucket = __storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
    return {"Blobs": list(map(lambda x: x.name, blobs)), "Prefixes": blobs.prefixes}


def upload_blob(bucket_name: str, source_file_name: str, destination_blob_name: str):
    """Uploads a file to the bucket."""
    bucket = __storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))


def download_blob(bucket_name: str, source_blob_name: str, destination_file_name: str):
    """Downloads a blob from the bucket."""
    bucket = __storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print('Blob {} downloaded to {}.'.format(
        source_blob_name,
        destination_file_name))


def delete_blob(bucket_name: str, blob_name: str):
    """Deletes a blob from the bucket."""
    bucket = __storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print('Blob {} deleted.'.format(blob_name))


def blob_metadata(bucket_name: str, blob_name: str) -> dict:
    """Prints out a blob's metadata."""
    bucket = __storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    return {
        'Blob': blob.name,
        'Bucket': blob.bucket.name,
        'Storage class': blob.storage_class,
        'ID': blob.id,
        'Size': '{} bytes'.format(blob.size),
        'Updated': blob.updated,
        'Generation': blob.generation,
        'Metageneration': blob.metageneration,
        'Etag': blob.etag,
        'Owner': blob.owner,
        'Component count': blob.component_count,
        'Crc32c': blob.crc32c,
        'md5_hash': blob.md5_hash,
        'Cache-control': blob.cache_control,
        'Content-type': blob.content_type,
        'Content-disposition': blob.content_disposition,
        'Content-encoding': blob.content_encoding,
        'Content-language': blob.content_language,
        'Metadata': blob.metadata,
        'Temporary hold': 'enabled' if blob.temporary_hold else 'disabled',
        'Event based hold': 'enabled' if blob.event_based_hold else 'disabled',
        'retentionExpirationTime': blob.retention_expiration_time if blob.retention_expiration_time else 'disabled'
    }


def make_blob_public(bucket_name: str, blob_name: str):
    """Makes a blob publicly accessible."""
    bucket = __storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.make_public()
    print('Blob {} is publicly accessible at {}'.format(
        blob.name, blob.public_url))
    return blob.public_url


def rename_blob(bucket_name: str, blob_name: str, new_name: str):
    """Renames a blob."""
    bucket = __storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.rename_blob(blob, new_name)
    print('Blob {} has been renamed to {}'.format(
        blob.name, new_blob.name))


def copy_blob(bucket_name: str, blob_name: str, new_bucket_name: str, new_blob_name: str):
    """Copies a blob from one bucket to another with a new name."""
    source_bucket = __storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = __storage_client.get_bucket(new_bucket_name)
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)
    print('Blob {} in bucket {} copied to blob {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name))


def write_file_to_storage(bucket_name, blob_name, file, content_type: str = 'text/csv'):
    bucket = __storage_client.get_bucket(bucket_name)

    blob = bucket.blob(blob_name)

    with open(file if isinstance(file, str) else file.name, 'rb') as file_tmp:
        blob.upload_from_file(file_tmp, content_type=content_type)

    return 'gs://{}/{}'.format(bucket.name, blob_name)
