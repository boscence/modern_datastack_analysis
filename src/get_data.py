import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import date



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

    company_data = json_data['props']['pageProps']['stack']
    stack_data = json_data['props']['pageProps']['dataStack']

    company_data['stack_data'] = stack_data

    return company_data
    #print(type(json_data[0].text))
    #print(company_data)
    #print(json_data['props']['pageProps']['dataStack'])


def get_json_file(json_data, stack_url):
    """Writes the stack data to a JSON file in the datalake.

    Args:
        json_data (dict): A dict containing the stack data .
        stack_url (str): The URL of the stack, used to get the 
        stack name.

    Returns:
        None.

    """
    today_date = date.today().strftime(format = "%Y%m%d")
    # Get the stack's name from the url 
    stack_url_split = stack_url.split("/")
    stack_name = stack_url_split[-1]

    # Write the data to json
    json_dumped = json.dumps(json_data)
    with open(f"data/source/mds/source_mds_{today_date}__{stack_name}.json", "w") as f:
        f.write(json_dumped)


def load_json(json_file):
    loaded_json = open(f"data/source/mds/{json_file}")
    data = json.load(loaded_json)
    return data

def get_company_table(data):
    company_data = pd.json_normalize(data)
    company_data['load_date'] = date.today().strftime(format = "%Y%m%d")
    return company_data

def get_stack_table(data, company_data):
    stack_data = pd.json_normalize(data['stack_data'],"children")
    stack_data["_id"] = company_data["_id"][0]
    stack_data["companyName"] = company_data["companyName"][0]
    stack_data["verified"] = company_data["verified"][0]
    stack_data['load_date'] = date.today().strftime(format = "%Y%m%d")
    return stack_data


def write_stack_parquet(stack_data,json_file):
    today_date = date.today().strftime(format = "%Y%m%d")
    file_name = json_file.split(".")[0]
    stack_data.to_parquet(f'data/staging/mds/stack/stg_mds_{today_date}__{file_name}_stack.parquet.gzip',
              compression='gzip')


def write_company_parquet(company_data,json_file):
    today_date = date.today().strftime(format = "%Y%m%d")
    file_name = json_file.split(".")[0]
    company_data[['_id','companyName','description','organizationId','verified']].to_parquet(f'data/staging/mds/company/stg_mds_{today_date}__{file_name}_company.parquet.gzip',
              compression='gzip')
    