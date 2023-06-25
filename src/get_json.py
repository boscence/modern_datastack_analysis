import requests
import json

url = "https://www.moderndatastack.xyz/stacks/aggua"

response = requests.get(url)

if response.headers['Content-Type'] == 'application/json':
#    data = json.loads(response.content)
#else:
#    raise Exception('The response content type is not application/json.')

print(response.text)
#print(response.headers['type'] == 'application/json')