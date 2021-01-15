#!/bin/python3

import pandas as pd
from doltpy.core import Dolt
from doltpy.etl import get_df_table_writer

npi_data_path: str = "working/npi_trimmed_final_backup.csv"

column_types: dict = {
    "zip_code": "string"
}
npi_data: pd.DataFrame = pd.read_csv(filepath_or_buffer=npi_data_path, dtype=column_types)  # , nrows=100

# Fixed That Code In Main - Technically It's All Fixed, So This File Is Now Useless
# print("Dropping Old Index")
# npi_data.drop(npi_data.columns[0], axis=1, inplace=True)

print("Fixing Addresses")
npi_data['street_address'].replace(to_replace=r',$', value='', inplace=True, regex=True)

print("Fixing Postal Codes")  # (\d{5})(\d{4})
# npi_data['zip_code'].replace(to_replace=r'(\d{5})(\d{4})', value=r'\1-\2', inplace=True, regex=True)
npi_data['zip_code'] = npi_data['zip_code'].str.replace(r'(\d{5})(\d{4})', r'\1-\2', regex=True)

print("Fixing Dates")  # mm/dd/yyyy -> yyyy-mm-dd
npi_data['publish_date'] = npi_data['publish_date'].str.replace(r'(\d{2})/(\d{2})/(\d{4})', r'\3-\1-\2', regex=True)

print("Displaying Fixed DataFrame")
print(npi_data)

print("Saving Fixed DataFrame To New CSV")
npi_data.to_csv("working/npi_trimmed_final_fixed_backup.csv", index=False)

print("Writing To Dolt Repo")
raw_data_writer = get_df_table_writer('hospitals', lambda: npi_data, ['npi_number'])

repo: Dolt = Dolt('working/hospital-price-transparency/')
raw_data_writer(repo)

print("Done")
