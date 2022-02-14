from download import download_file
from upload import upload_s3
import os
from utils import get_prev_file_name, get_next_file_name, upload_bookmark


def lambda_handler(event, context):
    global upload_res
    file_prefix = os.environ.get('FILE_PREFIX')
    bucket = os.environ.get("BUCKET_NAME")
    bookmark_file = os.environ.get("BOOKMARK_FILE")
    baseline_file = os.environ.get("BASELINE_FILE")
    while True:
        prev_file_name = get_prev_file_name(
            bucket,
            file_prefix,
            bookmark_file,
            baseline_file)
        file_name = get_next_file_name(prev_file_name)
        download_res = download_file(file_name)
        if download_res.status_code == 404:
            print("Invalid file name or dowloads caught up to most recent")
            break
        upload_res = upload_s3(
            bucket,
            f"{file_prefix}/{file_name}",
            download_res.content)
        print(f"File {file_name} successfully processed")
        upload_bookmark(bucket, file_prefix, bookmark_file, file_name)
    return upload_res
