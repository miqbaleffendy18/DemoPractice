import os
import json
import pandas as pd

def parse_json(data):
    province_code_list = []
    province_name_list = []
    city_code_list = []
    city_name_list = []
    district_code_list = []
    district_name_list = []
    subdistrict_code_list = []
    subdistrict_name_list = []

    for province_code, province_name in data['provinsi'].items():
        for city_code, city_name in data['kabupaten'].get(province_code, {}).items():
            for district_code, district_name in data['kecamatan'].get(province_code + city_code, {}).items():
                for subdistrict_code, subdistrict_name in data['kelurahan'].get(province_code + city_code + district_code, {}).items():
                    province_code_list.append(province_code)
                    province_name_list.append(province_name)
                    city_code_list.append(province_code + city_code)
                    city_name_list.append(city_name)
                    district_code_list.append(province_code + city_code + district_code)
                    district_name_list.append(district_name)
                    subdistrict_code_list.append(province_code + city_code + district_code + subdistrict_code)
                    subdistrict_name_list.append(subdistrict_name)

    parsed_df = pd.DataFrame({
        'province_code': province_code_list,
        'province_name': province_name_list,
        'city_code': city_code_list,
        'city_name': city_name_list,
        'district_code': district_code_list,
        'district_name': district_name_list,
        'subdistrict_code': subdistrict_code_list,
        'subdistrict_name': subdistrict_name_list
    })

    return parsed_df

if __name__ == '__main__':

    path = '../data'
    filename = 'wilayah.json'
    filepath = os.path.join(path, filename)

    with open(filepath) as json_file:
        file_contents = json_file.read()
    
    data = json.loads(file_contents)
    parsed_json = parse_json(data)

    parsed_json.to_csv(os.path.join(path, 'wilayah_parsed.csv'), sep = ',', header = True, index = False)

