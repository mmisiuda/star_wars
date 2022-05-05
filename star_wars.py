
import requests
import json
import pandas as pd
import numpy as np
import re
from azure.storage.blob import BlobServiceClient
pd.options.mode.chained_assignment = None


# get data from SWAPI.dev

def get_data(name: str, size: int) -> pd.DataFrame:
    
    records = []
    
    for i in range(1, size):
        url = f'https://swapi.dev/api/{name}/{i}/'
        r = requests.get(url)
        data = r.json()
        records.append(data)
    
    df = pd.DataFrame.from_records(records)
    
    return df

chars_help_raw = get_data('people', 84) # charaters helper dataframe since the big one has some characters missing in it
species_raw = get_data('species', 38)
planets_raw = get_data('planets', 61)
starships_raw = get_data('starships', 18)
vehicles_raw = get_data('vehicles', 63)
chars_raw = pd.read_parquet('StarWars_Characters.parquet')

# function to insert id column (for mapping, not for joins)

def insert_id_col(df: pd.DataFrame) -> pd.DataFrame:
    
    lenght = len(df)+1
    df.reset_index(drop=True, inplace=True)
    df.insert(0, 'id', range(1, lenght))
    df = df.set_index('id')
    
    return df


# Characters dataframe
# drop unwanted columns, fill missing values with 'None'

chars_col_drop = chars_raw.drop(['key', 'url', 'description', 'species_2nd', 'species_3rd'], axis=1)
chars_col_and_na_drop = chars_col_drop.fillna('None') 
characters_df = chars_col_and_na_drop.copy()

# drop unwanted columns from helper df and drop missing values

chars_help_drop_col = chars_help_raw.drop(['films', 'vehicles', 'starships',
                                          'created', 'edited', 'url', 'detail'], axis=1)

chars_help_drop_col_dropna = chars_help_drop_col.dropna()

# clean helper df, map species

chars_help_drop_col_dropna['homeworld'] = chars_help_drop_col_dropna['homeworld'].str.replace('https://swapi.dev/api/planets/', "")
chars_help_drop_col_dropna['homeworld'] = chars_help_drop_col_dropna['homeworld'].str.rstrip('/')

chars_help_worlds_map = chars_help_drop_col_dropna.copy()
chars_help_worlds_map['species'] = chars_help_worlds_map['species'].map(lambda x: str(x)[32:-3])

# use species df, insert id column

species_with_id = insert_id_col(species_raw)

#make scpecies dictionary

species_dict = pd.Series(species_with_id['name'].values, index=species_with_id.index).to_dict()

chars_help_worlds_map['species'] = chars_help_worlds_map['species'].replace('', '1')
chars_help_worlds_map['species'] = chars_help_worlds_map['species'].astype('int')
chars_help_worlds_map['species'] = chars_help_worlds_map['species'].map(species_dict)

# make planets dictionary and map planets in helper df

planets_with_id = insert_id_col(planets_raw)
planets_dict = pd.Series(planets_with_id['name'].values, index=planets_with_id.index).to_dict()
planets_dict[0] = 'None'

chars_help_worlds_map['homeworld'] = chars_help_worlds_map['homeworld'].astype('int')
chars_help_worlds_map['homeworld'] = chars_help_worlds_map['homeworld'].map(planets_dict)

# clean helper df

chars_help_worlds_map['gender'] = chars_help_worlds_map['gender'].str.replace('n/a', 'None')
chars_help_worlds_map['gender'] = chars_help_worlds_map['gender'].str.replace('none', 'None')
chars_help_worlds_map['hair_color'] = chars_help_worlds_map['hair_color'].str.replace('n/a', 'None')
chars_help_worlds_map['hair_color'] = chars_help_worlds_map['hair_color'].str.replace('none', 'None')

chars_help_clean = chars_help_worlds_map.copy()

chars_help_clean.columns = ['name', 'height', 'weight', 'hair_color', 
                            'skin_color', 'eye_color', 'birth_year', 
                            'gender', 'home_world', 'species']

cols = characters_df.columns.to_list()
chars_help_df = chars_help_clean[cols]

# concat main characters df with helper df

chars_combine = pd.concat([characters_df, chars_help_df], axis=0)
chars_combine = chars_combine.reset_index(drop=True)

chars_combine['gender'] = chars_combine['gender'].apply(lambda x: x.title())
chars_combine['eye_color'] = chars_combine['eye_color'].apply(lambda x: x.title())
chars_combine['skin_color'] = chars_combine['skin_color'].apply(lambda x: x.title())
chars_combine['hair_color'] = chars_combine['hair_color'].apply(lambda x: x.title())

# clean weight and height

chars_combine[['weight', 'height']] = chars_combine[['weight', 'height']].replace('unknown', '0')
chars_combine[['weight', 'height']] = chars_combine[['weight', 'height']].replace('None', '0')
chars_combine['height'] = chars_combine['height'].str.replace(r'.', r'', regex=False)
chars_combine['weight'] = chars_combine['weight'].str.replace(r'.', r'', regex=False)
chars_combine['weight'] = chars_combine['weight'].str.replace(r',', r'', regex=False)
chars_combine['height'] = chars_combine['height'].fillna('0', axis=0)
chars_combine['weight'] = chars_combine['weight'].apply(lambda x: x.strip())

chars_combine['weight'] = chars_combine['weight'].apply(lambda x: ''.join(letter for letter in x.split() if x.isdigit()))
# ^ creates ''
chars_combine.loc[chars_combine['weight'] == ''] = '0'

chars_combine['height'] = chars_combine['height'].apply(lambda x: ''.join(letter for letter in x.split() if x.isdigit()))

# convert weight and height to int

chars_combine[['weight', 'height']] = chars_combine[['weight', 'height']].astype('int')

# clean errors

chars_with_zeros = chars_combine.loc[chars_combine['name'] == '0'].index
chars_clean = chars_combine.drop(labels=chars_with_zeros, axis=0)

# insert id column

chars_df_final = insert_id_col(chars_clean)


# Species dataframe
# drop unwanted columns, clean data and fill missing values

species_drop = species_raw.drop(['people', 'films', 'created', 'edited', 'url'], axis=1)

species_drop['homeworld'] = species_drop['homeworld'].str.replace('https://swapi.dev/api/planets/', '')
species_drop['homeworld'] = species_drop['homeworld'].str.rstrip('/')
species_drop['homeworld'].fillna('0', axis=0, inplace=True)
species_drop['homeworld'] = species_drop['homeworld'].astype('int').map(planets_dict)

species_mapped = species_drop.copy()

species_mapped['average_height'].replace(['unknown', 'n/a'], '0', inplace=True)
species_mapped['average_height'] = species_mapped['average_height'].astype('int')

species_mapped['average_lifespan'].replace('unknown', '0', inplace=True)
species_mapped['average_lifespan'].replace('indefinite', '9999', inplace=True)
species_mapped['average_lifespan'] = species_mapped['average_lifespan'].astype('int')

species_fillna = species_mapped.copy()

species_fillna['skin_colors'].replace('n/a', 'none', inplace=True)
species_fillna['hair_colors'].replace('n/a', 'none', inplace=True)
species_fillna['eye_colors'].replace('n/a', 'none', inplace=True)
species_fillna['language'].replace('n/a', 'none', inplace=True)

for col in species_fillna.columns:
    if species_fillna[col].dtype == 'object':
        species_fillna[col] = species_fillna[col].apply(lambda x: x.title())

# insert id column in species final df

species_df = species_fillna.copy()
species_df.drop('id', axis=1, inplace=True)
species_df_final = insert_id_col(species_df)


# Planets dataframe

planets_drop = planets_with_id.drop(['residents', 'films', 'created', 'edited', 'url'], axis=1)

# drop this record as it cointains only zeros

planets_drop.drop([28], axis=0, inplace=True)

# clean data

planet_unknown_cols = ['rotation_period', 'orbital_period', 'diameter', 'surface_water', 'population']

for col in planet_unknown_cols:
    planets_drop[col].replace('unknown', '0', inplace=True)
    
planet_cols_to_int = ['rotation_period', 'orbital_period', 'diameter']

for col in planet_cols_to_int:
    planets_drop[col] = planets_drop[col].astype('int')

planets_drop['surface_water'] = planets_drop['surface_water'].astype('float')

# first convert population to float, int too large

planets_drop['population'] = planets_drop['population'].astype('float').apply(lambda x: x / 1000)
planets_drop['population'] = planets_drop['population'].astype('int')

planets_drop.rename({'population': 'population (thousands)'}, axis=1, inplace=True)

# insert id column

planets_df = planets_drop.copy()
planets_df_final = insert_id_col(planets_df)


# Starships dataframe
# drop unwanted columns and missing values

starships_drop = starships_raw.drop(['detail', 'pilots', 'films', 'created', 'edited', 'url'], axis=1)
starships_drop.loc[starships_drop['name'].isna()]
starships_drop.dropna(axis=0, how='all', inplace=True)

# clean the data and change some column types to numerics

starships_drop['cost_in_credits'].replace('unknown', '0', inplace=True)
starships_drop['length'] = starships_drop['length'].str.replace(',', '.', regex=False)
starships_drop['length'] = starships_drop['length'].str.replace('.', '', regex=False)
starships_drop['max_atmosphering_speed'].replace('n/a', '0', inplace=True)
starships_drop['crew'] = starships_drop['crew'].str.replace(',', '')
starships_drop['passengers'] = starships_drop['passengers'].str.replace('n/a', '0')
starships_drop['passengers'] = starships_drop['passengers'].str.replace(',', '')
starships_drop[['crew'][0]][1] = '165'
starships_drop[['max_atmosphering_speed'][0]][10] = '1000'

starships_astype = starships_drop.copy()

starships_col_to_int = ['length', 'max_atmosphering_speed', 
                        'crew', 'passengers', 'MGLT']

starships_col_to_float = ['cost_in_credits', 'cargo_capacity']

starships_astype[starships_col_to_int] = starships_astype[starships_col_to_int].astype('int')
starships_astype[starships_col_to_float] = starships_astype[starships_col_to_float].astype('float')

# insert id column

starships_df = starships_astype.rename({'crew': 'crew_max'}, axis=1)
starships_df_final = insert_id_col(starships_df)


# ### Vehicles dataframe
# drop unwanted columns, fill missing values and cast numeric columns as int and float

vehicles_drop = vehicles_raw.drop(['detail', 'pilots' , 'films' , 'created' , 'edited' , 'url'], axis=1)
vehicles_drop.dropna(inplace=True)

vehicles_unknown_cols = ['cost_in_credits', 'length', 'max_atmosphering_speed', 'crew', 'passengers', 'cargo_capacity']
vehicles_int_cols = ['cost_in_credits', 'max_atmosphering_speed', 'crew', 'passengers', 'cargo_capacity']

for col in vehicles_unknown_cols:
    vehicles_drop[col] = vehicles_drop[col].str.replace('unknown', '0')
    
vehicles_drop['cargo_capacity'].replace('none', '0', inplace=True)

vehicles_drop[vehicles_int_cols] = vehicles_drop[vehicles_int_cols].astype('int')
vehicles_drop['length'] = vehicles_drop['length'].astype('float')

vehicles_df_final = insert_id_col(vehicles_drop)


# ## Send CSV to Azure Blob
# provide connection string to Azure Blob Storage Account
connection_string: '****'

# function for uploading csv's

def upload_csv_to_blob(connection_string: str, container_name: str, df: pd.DataFrame):

    
    # Instantiate a new BlobServiceClient using a connection string
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Instantiate a new ContainerClient
    container_client = blob_service_client.get_container_client(f'{container_name}')
    try:
       # Create new Container in the service
       container_client.create_container()
       properties = container_client.get_container_properties()
    except ResourceExistsError:
       print("Container already exists.")

    output = df.to_csv(index_label='id', encoding='utf-8')
    
    # Instantiate a new BlobClient
    blob_client = container_client.get_blob_client(f"{container_name}.csv")
    
    # upload data
    blob_client.upload_blob(output, blob_type="BlockBlob")

# upload csv's to blob storage

upload_csv_to_blob(connection_string=connection_string, container_name='characters', df=chars_df_final)
upload_csv_to_blob(connection_string=connection_string, container_name='species', df=species_df_final)
upload_csv_to_blob(connection_string=connection_string, container_name='planets', df=planets_df_final)
upload_csv_to_blob(connection_string=connection_string, container_name='starships', df=starships_df_final)
upload_csv_to_blob(connection_string=connection_string, container_name='vehicles', df=vehicles_df_final)

