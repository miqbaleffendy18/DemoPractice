import os
import json
import pandas as pd
from datetime import datetime
import warnings
import textdistance
import re
import logging
warnings.filterwarnings('ignore')

logging.basicConfig(level = logging.INFO, filename = 'output.log', filemode = 'w')

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

def data_prep(data, data_master):
    data_subset = data[['subdistrict_code', 'province_name', 'city_name', 'district_name', 'subdistrict_name']]
    data_master_subset = data_master[['URBAN_ID', 'PROVINCE_NAME', 'DISTRICT_NAME', 'SUBDISTRICT_NAME', 'URBAN_NAME']]
    data_master_subset[['PROVINCE_NAME', 'DISTRICT_NAME', 'SUBDISTRICT_NAME', 'URBAN_NAME']] = data_master_subset[['PROVINCE_NAME', 'DISTRICT_NAME', 'SUBDISTRICT_NAME', 'URBAN_NAME']].apply(lambda x: x.astype(str).str.upper())

    data_joined = pd.merge(
        data_subset, 
        data_master_subset, 
        how = 'outer', 
        left_on = ['province_name', 'city_name', 'district_name', 'subdistrict_name'],
        right_on = ['PROVINCE_NAME', 'DISTRICT_NAME', 'SUBDISTRICT_NAME', 'URBAN_NAME']
    )

    # data_mapped = data_joined[data_joined['URBAN_ID'].notnull()]
    data_mapped = data_joined[(data_joined['subdistrict_code'].notnull()) & (data_joined['URBAN_ID'].notnull())]
    data_mapped = data_mapped[['subdistrict_code', 'URBAN_ID']]
    # data_not_mapped = data_joined[data_joined['URBAN_ID'].isnull()]
    data_not_mapped = data_joined[(data_joined['subdistrict_code'].notnull()) & (data_joined['URBAN_ID'].isnull())]
    data_master_subset = data_joined.loc[(data_joined['subdistrict_code'].isnull()) & (data_joined['URBAN_ID'].notnull()), ['URBAN_ID', 'PROVINCE_NAME', 'DISTRICT_NAME', 'SUBDISTRICT_NAME', 'URBAN_NAME']]

    return data_mapped, data_not_mapped, data_master_subset

def jaro_winkler_similarity_concat(s1, s2):
    concat_s1 = s1['province_name'] + '_' + s1['city_name'] + '_' +  s1['district_name'] + '_' + s1['subdistrict_name']
    concat_s2 = s2['PROVINCE_NAME'] + '_' + s2['DISTRICT_NAME'] + '_' +  s2['SUBDISTRICT_NAME'] + '_' +  s2['URBAN_NAME']

    score = textdistance.jaro_winkler(concat_s1, concat_s2)

    # logging.info(f'Compare {concat_s1} and {concat_s2}, score = {score}')
    return score

def remove_parentheses(text):
    return re.sub(r'\([^)]*\)', '', text)

def run_comparison(data, data_master):
    subdistrict_data_list = []
    subdistrict_master_list = []
    similarity_score_list = []
    total_rows_checked = 0

    total_rows = len(data.index)

    start_time = datetime.now()
    print('Start Loop: {}'.format(start_time))

    for index_a, row_a in data.iterrows():
        max_similarity = 0
        max_similarity_row = None
        
        if row_a['province_name'] in (['PAPUA SELATAN', 'PAPUA TENGAH', 'PAPUA PEGUNUNGAN', 'PAPUA BARAT DAYA']):
            filtered_table_master = data_master[data_master['PROVINCE_NAME'].str[:5] == 'PAPUA']
        elif row_a['province_name'] == 'DAERAH ISTIMEWA YOGYAKARTA':
            filtered_table_master = data_master[data_master['PROVINCE_NAME'] == 'DI YOGYAKARTA']
        elif row_a['province_name'] == 'KEPULAUAN BANGKA BELITUNG':
            filtered_table_master = data_master[data_master['PROVINCE_NAME'] == 'BANGKA BELITUNG']
        else:
            filtered_table_master = data_master[data_master['PROVINCE_NAME'] == row_a['province_name']]

        if len(filtered_table_master) == 0:
            continue

        filtered_table_master['URBAN_NAME'] = filtered_table_master['URBAN_NAME'].apply(lambda x: remove_parentheses(x))
        
        for index_b, row_b in filtered_table_master.iterrows():
            similarity = jaro_winkler_similarity_concat(row_a, row_b)
            
            if similarity > max_similarity:
                max_similarity = similarity
                max_similarity_row = row_b
    
        subdistrict_data_list.append(row_a['subdistrict_code'])
        subdistrict_master_list.append(max_similarity_row['URBAN_ID'])
        similarity_score_list.append(max_similarity)
        total_rows_checked += 1

        if total_rows_checked % 100 == 0:
            end_time = datetime.now()
            print(f'{total_rows_checked} / {total_rows} {end_time}')
            print('Duration: {}'.format(end_time - start_time))

    table_check = pd.DataFrame({
        'subdistrict_a': subdistrict_data_list,
        'subdistrict_b': subdistrict_master_list,
        'similarity_score': similarity_score_list
    })

    return table_check

def merging_table(data_mapped, data_not_mapped, parsed_df, data_master):
    data_mapped_final = data_mapped.merge(parsed_df, left_on = 'subdistrict_code', right_on = 'subdistrict_code', how = 'inner').merge(data_master, left_on = 'URBAN_ID', right_on = 'URBAN_ID', how = 'inner')
    data_mapped_final = data_mapped_final[[
        'province_code',
        'province_name',
        'ISLAND_NAME',
        'city_code',
        'city_name',
        'TYPE_LABEL',
        'district_code',
        'district_name',
        'subdistrict_code',
        'URBAN_ID',
        'subdistrict_name',
        'POSTAL_CODE',
        'CODE_SICEPAT',
        'IS_COD_SICEPAT',
        'CODE_JNE_DESTINATION',
        'CODE_JNE',
        'CODE_JNE_ORIGIN',
        'CODE_TIKI'
    ]]
    data_mapped_final['similarity_score'] = 1
    data_mapped_final['URBAN_ID'] = data_mapped_final['URBAN_ID'].astype(int)

    data_not_mapped_final = data_not_mapped.merge(parsed_df, left_on = 'subdistrict_a', right_on = 'subdistrict_code', how = 'inner').merge(data_master, left_on = 'subdistrict_b', right_on = 'URBAN_ID', how = 'inner')
    data_not_mapped_final = data_not_mapped_final[[
        'province_code',
        'province_name',
        'ISLAND_NAME',
        'city_code',
        'city_name',
        'TYPE_LABEL',
        'district_code',
        'district_name',
        'subdistrict_code',
        'URBAN_ID',
        'subdistrict_name',
        'POSTAL_CODE',
        'CODE_SICEPAT',
        'IS_COD_SICEPAT',
        'CODE_JNE_DESTINATION',
        'CODE_JNE',
        'CODE_JNE_ORIGIN',
        'CODE_TIKI',
        'similarity_score'
    ]]

    # data_not_mapped_final.to_csv(os.path.join(path, 'locality_detail_final.csv'), sep = ',', header = True, index = False)
    df_merged = pd.concat([data_mapped_final, data_not_mapped_final], ignore_index=True)
    df_merged.to_csv(os.path.join(path, 'locality_detail_final.csv'), sep = ',', header = True, index = False)


if __name__ == '__main__':

    path = '../data'
    filename_json = 'wilayah.json'
    filepath_json = os.path.join(path, filename_json)

    # Download table dbt_prod.locality_detail
    filename_master = 'data_master.csv'
    filepath_master = os.path.join(path, filename_master)

    with open(filepath_json) as json_file:
        file_contents = json_file.read()
    data = json.loads(file_contents)
    parsed_json = parse_json(data)
    
    data_master = pd.read_csv(filepath_master, sep=',')

    mapped_population, not_mapped_population, data_master_subset = data_prep(parsed_json, data_master)
    not_mapped_population = not_mapped_population[not_mapped_population['subdistrict_code'].isin(['1105112004', '1105112014'])]
    not_mapped_checked = run_comparison(not_mapped_population, data_master_subset)

    merging_table(mapped_population, not_mapped_checked, parsed_json, data_master)
