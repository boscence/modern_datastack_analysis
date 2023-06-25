import requests

url = "https://www.moderndatastack.xyz/stacks/aggua"

response = requests.get(url)

html = response.content

with open("aggua.html", "wb") as f:
    f.write(html)
    print()
