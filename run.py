from src import get_stacks_url
from src import get_data 
from src import data_writer
import os
import config
from google.cloud import storage
import time


### Set the constants
# get the html data containing all stack urls.
stacks_html_data = get_stacks_url.get_stack_html_data(config.URL_GET_STACKS)
# now get the urls
stack_urls = get_stacks_url.get_stack_urls(stacks_html_data, config.URL_STACK_BASE)

gcs_storage_client = storage.Client(project=config.gcp_project_id)
bucket_name_source_mds = gcs_storage_client.bucket(config.bucket_name_source_mds)


# for each url
json_list = []
for stack in stack_urls:
    json_data = get_data.get_mds_json(stack) # get the data
    json_list.append(json_data)
    #get_data.write_mds_source_data(json_data,stack) # write the json
    #data_writer.write_mds_source_to_gcs(json_data, 
    #                                    stack,
    #                                    gcs_storage_client,
    #                                    bucket_name_source_mds)
    #print(f'Written to local:{stack}')
    #time.sleep(3)

data_writer.write_combined_mds_source_to_gcs(json_list, 
                                    gcs_storage_client,
                                    bucket_name_source_mds)

# get the list of json files
#source_data_stacks = [source_data for source_data in os.listdir("data/source/mds") if source_data.endswith('.json')]

# for each json file (stack)



#for stack_data in source_data_stacks:
#    print(f"Writing {stack_data}")
#    loaded_json = get_data.load_mds_source(stack_data) # load it
#    company_df = get_data.get_company_table(loaded_json) # get the data about the company
#    stack_df = get_data.get_stack_table(loaded_json, company_df)  # get the stack data
#    get_data.write_stack_parquet(stack_df,stack_data) # write stack data to parquet
#    get_data.write_company_parquet(company_df,stack_data) # write company data to parquet

