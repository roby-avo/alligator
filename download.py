import os
import requests

# Define the URL and file path
GEOIP_DB_URL = "https://git.io/GeoLite2-City.mmdb"
GEOIP_DB_FILE = "GeoLite2-City.mmdb"

# Check if the file already exists
if not os.path.isfile(GEOIP_DB_FILE):
    print("Downloading GeoLite2-City.mmdb...")
    response = requests.get(GEOIP_DB_URL, stream=True)
    with open(GEOIP_DB_FILE, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
else:
    print("GeoLite2-City.mmdb already exists. Skipping download.")
