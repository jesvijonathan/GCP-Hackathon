#!/bin/bash

# python3 -m pip install -r requirements.txt

# winget install MongoDB.Server
# mongod # use : https://www.mongodb.com/try/download/compass

# cd ./manual_data
# python3 .\main_data.py --preset .\preset.json --seed 777  
# python3 .\load_to_mongo.py --manifest ".\output\main_manifest_*.json"


# cd ../
# python3 preset.py generate --num 2 --out preset.json --seed 1234
# python3 preset.py ingest --preset preset.json --mongo-uri mongodb://127.0.0.1:27017 --db merchant_analytics --deep-scan
# python3 preset.py streams --preset preset.json --mongo-uri mongodb://127.0.0.1:27017 --db merchant_analytics


# python3 main.py