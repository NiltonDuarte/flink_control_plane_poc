import requests
from poc.common.domain import MoveDatatypeRequest

SOURCE = "family_a"
TARGET = "family_b"

url = "http://localhost:8000/api/workflows/move_datatype"

# Replace with the JSON payload your endpoint expects
payload = MoveDatatypeRequest(
    datatype="clicks", 
    source_family=SOURCE, 
    target_family=TARGET,
).model_dump()

response = requests.post(url, json=payload)

print("Status Code:", response.status_code)
print("Response Body:", response.json())