from src import get_stacks_url
from src import get_data 
import os
import config

### Set the constants
# get the html data containing all stack urls.
html_data = get_stacks_url.get_stack_html_data(config.URL_GET_STACKS)
# now get the urls
stack_urls = get_stacks_url.get_stack_urls(html_data, config.URL_STACK_BASE)

# for each url
for stack in stack_urls:
    json_data = get_data.get_json(stack) # get the data
    get_data.get_json_file(json_data,stack) # write the json
    print(f'Written:{stack}')

# get the list of json files
json_files = [json_file for json_file in os.listdir("data/source/mds") if json_file.endswith('.json')]

# for each json file (stack)
for json_file in json_files:
    print(f"Writing {json_file}")
    data = get_data.load_json(json_file) # load it
    company_data = get_data.get_company_table(data) # get the data about the company
    stack_data = get_data.get_stack_table(data, company_data)  # get the stack data
    get_data.write_stack_parquet(stack_data,json_file) # write stack data to parquet
    get_data.write_company_parquet(company_data,json_file) # write company data to parquet