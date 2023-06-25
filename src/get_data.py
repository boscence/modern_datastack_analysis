import requests
from bs4 import BeautifulSoup
import json

url = "https://www.moderndatastack.xyz/stacks/aggua"

response = requests.get(url)

soup = BeautifulSoup(response.content, "html.parser")

dictionary = soup.find("div", class_="__NEXT_DATA__")
script_tag = soup.select_one("script[id='__NEXT_DATA__']")
scripts = soup.find_all("script", type="application/json")
#python_dictionary = {}

#for key, value in dictionary.items():
#    python_dictionary[key.text] = value.text

json_data = data = json.loads(scripts[0].text)

#print(type(scripts[0].text))
#print(json_data.keys())
print(json_data['props']['pageProps']['dataStack'][0])
print(json_data['props']['pageProps']['dataStack'][1])
print(json_data['props']['pageProps']['dataStack'][2])
print(json_data['props']['pageProps']['dataStack'][3])
print(len(json_data['props']['pageProps']['dataStack']))
