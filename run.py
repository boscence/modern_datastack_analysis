from src import get_stacks_url 
import config


html_data = get_stacks_url.get_stack_html_data(config.URL_GET_STACKS)
stack_urls = get_stacks_url.get_stack_urls(html_data, config.URL_STACK_BASE)


for url in stack_urls:
    print(url)