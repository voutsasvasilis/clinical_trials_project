import requests

url = "http://127.0.0.1:8000/items/7"
data = {"name": "TV", "price": 4356.324, "is_offer": True, "dummy_item": "GOOD TV"}

response = requests.put(url, json=data)
print(response.json())
