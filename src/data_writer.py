from google.cloud import storage
import config
from datetime import date
import json

TODAYS_DATE = date.today().strftime('%Y%m%d')

#project_id = "modern-datastack-analysis"
#bucket_name = "datalake--sources"
#source_file_name  = "data/source/mds/source_mds_20230712__1mg.json"

#destination_blob_name = "mds/test.json"

#gcs_storage_client = storage.Client(project=config.gcp_project_id)
#bucket_name_source_mds = gcs_storage_client.bucket(config.bucket_name_source_mds)


def write_mds_source_to_gcs(json_data,
                            stack_url,
                            gcs_storage_client,
                            bucket_name_source_mds):
    """Uploads a file to the bucket."""

    stack_url_split = stack_url.split("/")
    stack_name = stack_url_split[-1]
    file_path = f"{config.bucket_source_dir_mds}source_mds_{TODAYS_DATE}__{stack_name}.json"
    
    blob = bucket_name_source_mds.blob(file_path)
    
    json_dumped = json.dumps(json_data)

    blob.upload_from_string(json_dumped)
    print(f"File {stack_name} uploaded to GCS {file_path}.")
    

def write_combined_mds_source_to_gcs(json_data,
                            gcs_storage_client,
                            bucket_name_source_mds):
    """Uploads a file to the bucket."""

    file_path = f"{config.bucket_source_dir_mds_combined}source_mds_{TODAYS_DATE}__stacks.json"
    
    ##blob = bucket_name_source_mds.blob(file_path)
    
    json_dumped = json.dumps(json_data)

    blob = bucket_name_source_mds.blob(file_path)
    some_json_object = {'stack': list()}
    for json_stack in json_data:
        some_json_object['stack'].append(json_stack)
    
    #with open("data.json", 'w') as f:
     #   for json_line in json_data:
     #       f.write(json.dumps(json_line) + "\n")
    blob.upload_from_string(data=json.dumps(some_json_object),content_type='application/json') 
            #blob.upload_from_string(json.dumps(json_line) + "\n")
    #with open('t.json', 'rb') as f:
        


    #blob.upload_from_string(json_dumped)
    print(f"File uploaded to GCS {file_path}.")