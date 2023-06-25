"""
This module gets the url for every data stack in the amazing
www.moderndatastack.xyz website.

This can be used to extract the components used
by each company that has listed their stack.
"""


import requests
from bs4 import BeautifulSoup  
import config


def get_stack_html_data(url):

    """
    This function gets the HTML data from ttps://www.moderndatastack.xyz/stacks.

    Args:
        url: The URL (https://www.moderndatastack.xyz/stacks)

    Returns:
        The HTML data with only the links to the listed data stacks.
    """

    response = requests.get(url)
    html_data = BeautifulSoup(response.content, 'html.parser')
    html_data_reduced = html_data.find('div', attrs={"class":"justify-content-md-center"})

    return html_data_reduced
  

def get_stack_urls(html_data, url_base):

  """
  This function gets the URLs of all listed stacks in 
  https://www.moderndatastack.xyz/stacks).

  Args:
    html_data: HTML data with only the links to the listed data stacks.
    url_base: the root url to prefix to the returned href.

  Returns:
    A list of the URLs for each stack.

  """
  urls = []
  for link in html_data.find_all('a'):
    href = link.get('href')
    if href is not None:
        if "/stacks" in href:
            urls.append("".join([url_base, href]))
  return urls
