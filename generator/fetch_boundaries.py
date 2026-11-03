import json
from pathlib import Path
import requests

api = "https://www.geoboundaries.org/api/current/gbOpen/BGD/ADM2/"

print("Fetching Bangladesh district boundary metadata...")

response = requests.get(api, timeout=60)
response.raise_for_status()
meta = response.json()

if isinstance(meta, list):
    meta = meta[0]

print("Source:", meta.get("boundarySource"))
print("License:", meta.get("boundaryLicense"))
print("Expected districts:", meta.get("admUnitCount"))

url = meta.get("tjDownloadURL")

if not url:
    raise RuntimeError("TopoJSON download URL not found.")

print("Downloading district boundaries...")

response = requests.get(url, timeout=120)
response.raise_for_status()
topo = response.json()

objects = topo.get("objects", {})
if not objects:
    raise RuntimeError("Invalid TopoJSON: no objects found.")

obj_name = next(iter(objects))
geometries = objects[obj_name].get("geometries", [])

print("Downloaded boundary features:", len(geometries))

if len(geometries) < 60:
    raise RuntimeError("Expected approximately 64 district boundaries.")

output = Path("dashboard/data/districts.topo.json")
output.parent.mkdir(parents=True, exist_ok=True)

output.write_text(
    json.dumps(topo, ensure_ascii=False),
    encoding="utf-8"
)

print("Saved:", output)
print("Sample properties:")

for feature in geometries[:5]:
    print(feature.get("properties", {}))

print("Boundary download completed.")
