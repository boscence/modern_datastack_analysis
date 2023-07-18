from google.cloud import storage
import config
from datetime import date
import json

TODAYS_DATE = date.today().strftime('%Y%m%d')

def write_mds_source_to_gcs(json_data,
                            stack_url,
                            gcs_storage_client,
                            bucket_name_source_mds):

    """Uploads a single stack json to the GCS bucket.

    Args:
        json_data: The stack data to upload.
        stack_url: The URL of the stack, used to get the stack name
        gcs_storage_client: The Google Cloud Storage client.
        bucket_name_source_mds: The name of the bucket to upload to.

    Returns:
        None.
    """

    # get the GCS file path to write to
    stack_url_split = stack_url.split("/")
    stack_name = stack_url_split[-1]
    file_path = f"{config.bucket_source_dir_mds}source_mds_{TODAYS_DATE}__{stack_name}.json"
    
    # Create the blob in GCS to write to
    blob = bucket_name_source_mds.blob(file_path)
    
    json_dumped = json.dumps(json_data) # prepare the json
    blob.upload_from_string(json_dumped) # upload the file
    print(f"File {stack_name} uploaded to GCS {file_path}.")
    

def write_combined_mds_source_to_gcs(json_data,
                            gcs_storage_client,
                            bucket_name_source_mds):
    
    """Uploads a file to the GCS bucket in JSONL format.

    Args:
        json_data: The JSON data to upload.
        gcs_storage_client: The Google Cloud Storage client.
        bucket_name_source_mds: The name of the bucket to upload to.

    Returns:
        None.
    """

    # get the GCS file path to write to
    file_path = f"{config.bucket_source_dir_mds_combined}source_mds_{TODAYS_DATE}__stacks.json"
    
    # Create the blob in GCS to write to
    blob = bucket_name_source_mds.blob(file_path)
        
    # Create a dict to hold all dicts and prepare for jsonL format
    combined_json = {'stack': list()}

    # add each stack json to the combined_json in jasonL
    for json_stack in json_data:
        combined_json['stack'].append(json_stack)
    
    # Upload to cloud
    blob.upload_from_string(data=json.dumps(combined_json),content_type='application/json') 

    print(f"File uploaded to GCS {file_path}.")