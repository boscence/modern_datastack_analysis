from src import get_stacks_url
from src import get_data 
import config


html_data = get_stacks_url.get_stack_html_data(config.URL_GET_STACKS)
stack_urls = get_stacks_url.get_stack_urls(html_data, config.URL_STACK_BASE)

for stack in stack_urls:
    json_data = get_data.get_json(stack)
    get_data.get_json_file(json_data,stack)
    print(f'Written:{stack}')
