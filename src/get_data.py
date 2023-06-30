import requests
from bs4 import BeautifulSoup
import json



def get_json(stack_url):

    """Gets the JSON data from a stack's URL.

    Args:
        stack_url (str): The URL of the stack.

    Returns:
        The JSON data.

    """

    response = requests.get(stack_url)
    parsed_html = BeautifulSoup(response.content, "html.parser")
    stack_content = parsed_html.find_all("script", type="application/json")
    json_data = json.loads(stack_content[0].text)


    return json_data
    #print(type(scripts[0].text))
    #print(json_data.keys())
    #print(json_data['props']['pageProps']['dataStack'][0])
    #print(json_data['props']['pageProps']['dataStack'][1])
    #print(json_data['props']['pageProps']['dataStack'][2])
    #print(json_data['props']['pageProps']['dataStack'][3])


def get_json_file(json_data, stack_url):
    """Writes the stack data to a JSON file in the datalake.

    Args:
        json_data (dict): A dict containing the stack data .
        stack_url (str): The URL of the stack, used to get the 
        stack name.

    Returns:
        None.

    """
    
    # Get the stack's name frm the url 
    stack_url_split = stack_url.split("/")
    stack_name = stack_url_split[-1]

    # Write the data to json
    json_dumped = json.dumps(json_data)
    with open(f"data/{stack_name}.json", "w") as f:
        f.write(json_dumped)
