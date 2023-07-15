import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from datetime import date
import config


TODAYS_DATE = date.today().strftime('%Y%m%d')


def get_mds_json(stack_url):

    """Gets the JSON data from a stack's URL.

    Args:
        stack_url (str): The URL of the stack.

    Returns:
        company_data (dict): The JSON data for a stack as a dict.
    """

    response = requests.get(stack_url)
    parsed_html = BeautifulSoup(response.content, "html.parser")
    stack_content = parsed_html.find_all("script", type="application/json")
    json_data = json.loads(stack_content[0].text)

    company_data = json_data['props']['pageProps']['stack']
    stack_data = json_data['props']['pageProps']['dataStack']

    company_data['stack_data'] = stack_data
    company_data['extract_date'] = TODAYS_DATE

    return company_data



def write_mds_source_data(json_data, stack_url):
    """Writes the stack data to a JSON file in 
    the datalake's source directory.

    Args:
        json_data (dict): A dict containing the stack data .
        stack_url (str): The URL of the stack, used to get the 
        stack name.

    Returns:
        None.

    """
    # Get the stack's name from the url 
    stack_url_split = stack_url.split("/")
    stack_name = stack_url_split[-1]

    # Write the data to json
    json_dumped = json.dumps(json_data)
    with open(f"{config.source_path_mds}/source_mds_{TODAYS_DATE}__{stack_name}.json", "w") as f:
        f.write(json_dumped)


def load_mds_source(json_data):
    """A module to load the json data file from source,
       ready for processing 

    Args:
        json_data (dict): A dict containing the stack data.

    Returns:
        loaded_json (dict): a dict loaded from the json source

    """
    opened_json = open(f"{config.source_path_mds}/{json_data}")
    loaded_json = json.load(opened_json)
    return loaded_json

def get_company_table(loaded_json):
    """Isolate and create a pandas dataframe
       for the company

    Args:
        loaded_json (dict): a dict loaded from the json source

    Returns:
        company_df (pd.dataframe): a pandas dataframe for the company

    """
    company_df = pd.json_normalize(loaded_json)
    company_df['load_date'] = TODAYS_DATE
    return company_df

def get_stack_table(loaded_json, company_df):
    """Isolate and create a pandas dataframe
       for the stack data

    Args:
        loaded_json (dict): a dict loaded from the json source
        company_df (pd.dataframe): a pandas dataframe for the company

    Returns:
        stack_df (pd.dataframe): dataframe for the stack

    """
    stack_df = pd.json_normalize(loaded_json['stack_data'],"children")
    stack_df["_id"] = company_df["_id"][0]
    stack_df["companyName"] = company_df["companyName"][0]
    stack_df["verified"] = company_df["verified"][0]
    stack_df['load_date'] = TODAYS_DATE
    return stack_df


def write_stack_parquet(stack_df,json_file):

    file_name = json_file.split(".")[0].split("__")[1]
    stack_df.to_parquet(f'{config.stage_path_stack}/stg_mds_{TODAYS_DATE}_{file_name}__stack.parquet.gzip',
              compression='gzip')


def write_company_parquet(company_df,json_file):
    file_name = json_file.split(".")[0].split("__")[1]
    company_df[config.company_cols].to_parquet(f'{config.stage_path_company}/stg_mds_{TODAYS_DATE}_{file_name}__company.parquet.gzip',
              compression='gzip')
    