from src import get_stacks_url
from src import get_data 
import os
import config


html_data = get_stacks_url.get_stack_html_data(config.URL_GET_STACKS)
stack_urls = get_stacks_url.get_stack_urls(html_data, config.URL_STACK_BASE)


for stack in stack_urls:
    json_data = get_data.get_json(stack)
    get_data.get_json_file(json_data,stack)
    print(f'Written:{stack}')


json_files = [json_file for json_file in os.listdir("data") if json_file.endswith('.json')]


for json_file in json_files:
    data = get_data.load_json(json_file)
    company_data = get_data.get_company_table(data)
    stack_data = get_data.get_stack_table(data, company_data)
    get_data.write_parquet(stack_data,json_file)