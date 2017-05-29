import json
with open("src/config/full_station_old.json", "r") as json_file:
    data = json.load(json_file)
    stations = data['Stations']
    for station in stations:
        station['loc'] = [station.pop("latitude"), station.pop("longitude")]
    with open("src/config/full_station_config.json", "w") as write_json:
        json_data = json.dumps(data, ensure_ascii=False)
        write_json.write(json_data)
    write_json.close()
json_file.close() 