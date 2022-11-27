import requests

resp = requests.post("http://localhost:8000/", params={'txt':'I love you'})
print(resp.text)

