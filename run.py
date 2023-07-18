from src import get_stacks_url
from src import get_data 
from src import data_writer
import os
import config
from google.cloud import storage
import time


# Get the html data containing all stack urls fron the web page.
stacks_html_data = get_stacks_url.get_stack_html_data(config.URL_GET_STACKS)
# Now get the urls for each company's stack
stack_urls = get_stacks_url.get_stack_urls(stacks_html_data, config.URL_STACK_BASE)

# Initiate the GCS client for the project.
gcs_storage_client = storage.Client(project=config.gcp_project_id)
# Get the GCS Bucket object
bucket_name_source_mds = gcs_storage_client.bucket(config.bucket_name_source_mds)


# Create a list with all stack data
json_list = []
for stack in stack_urls:
    json_data = get_data.get_mds_json(stack) # get the data
    json_list.append(json_data)
    ## The local write is no longer required
    #get_data.write_mds_source_data(json_data,stack) # write the json
    #data_writer.write_mds_source_to_gcs(json_data, 
    #                                    stack,
    #                                    gcs_storage_client,
    #                                    bucket_name_source_mds)
    #print(f'Written to local:{stack}')
    #time.sleep(3)

# Write the combined jsonL to GCS 
data_writer.write_combined_mds_source_to_gcs(json_list, 
                                    gcs_storage_client,
                                    bucket_name_source_mds)
