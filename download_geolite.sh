#!/bin/bash

# Define the URL and file path
GEOIP_DB_URL="https://git.io/GeoLite2-City.mmdb"
GEOIP_DB_FILE="./api/GeoLite2-City.mmdb"

# Check if the file already exists
if [ ! -f "$GEOIP_DB_FILE" ]; then
    echo "Downloading GeoLite2-City.mmdb..."
    curl -L -o "$GEOIP_DB_FILE" "$GEOIP_DB_URL"
else
    echo "GeoLite2-City.mmdb already exists. Skipping download."
fi
