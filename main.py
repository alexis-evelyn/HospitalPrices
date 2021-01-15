#!/bin/python3
from typing import List

import pandas as pd
import doltpy
from doltpy.core import Dolt
from doltpy.etl import get_df_table_writer

# +------------+--------------------------------+------+----------------+----------+-------+
# | npi_number | name                           | url  | street_address | zip_code | state |
# +------------+--------------------------------+------+----------------+----------+-------+
# | 1194832477 | NEW YORK PRESBYTERIAN HOSPITAL | NULL | NULL           | NULL     | NULL  |
# +------------+--------------------------------+------+----------------+----------+-------+

# Source: https://download.cms.gov/nppes/NPI_Files.html
npi_data_path: str = "working/NPPES_Data_Dissemination_January_2021/npidata_pfile_20050523-20210110.csv"
othername_path: str = "working/NPPES_Data_Dissemination_January_2021/othername_pfile_20050523-20210110.csv"
pfile_path: str = "working/NPPES_Data_Dissemination_January_2021/pl_pfile_20050523-20210110.csv"


def read_large_npi_file():
    # Columns To Keep
    # NPI
    # "Provider Organization Name (Legal Business Name)"
    # "Provider Last Name (Legal Name)"
    # "Provider First Name"
    # "Provider Middle Name"
    # "Provider Name Prefix Text"
    # "Provider Name Suffix Text"
    # "Provider First Line Business Practice Location Address"
    # "Provider Second Line Business Practice Location Address"
    # "Provider Business Practice Location Address City Name"
    # "Provider Business Practice Location Address State Name"
    # "Provider Business Practice Location Address Postal Code"
    # Provider Enumeration Date
    print("Reading In NPI Data")

    npi_columns: List[str] = ["NPI",
                              "Provider Organization Name (Legal Business Name)",
                              "Provider Last Name (Legal Name)",
                              "Provider First Name",
                              "Provider Middle Name",
                              "Provider Name Prefix Text",
                              "Provider Name Suffix Text",
                              "Provider First Line Business Practice Location Address",
                              "Provider Second Line Business Practice Location Address",
                              "Provider Business Practice Location Address City Name",
                              "Provider Business Practice Location Address State Name",
                              "Provider Business Practice Location Address Postal Code",
                              "Provider Business Practice Location Address Country Code (If outside U.S.)",
                              "NPI Deactivation Date",
                              "NPI Reactivation Date",
                              "Provider Enumeration Date"]

    npi_column_types: dict = {'NPI': 'uint64',  # uint64
                              'Provider Organization Name (Legal Business Name)': 'string',
                              'Provider Last Name (Legal Name)': 'string',
                              'Provider First Name': 'string',
                              'Provider Middle Name': 'string',
                              'Provider Name Prefix Text': 'string',
                              'Provider Name Suffix Text': 'string',
                              'Provider First Line Business Practice Location Address': 'string',
                              'Provider Second Line Business Practice Location Address': 'string',
                              'Provider Business Practice Location Address City Name': 'string',
                              'Provider Business Practice Location Address State Name': 'string',
                              'Provider Business Practice Location Address Postal Code': 'string',  # uint32
                              'Provider Business Practice Location Address Country Code (If outside U.S.)': 'string',
                              'NPI Deactivation Date': 'string',  # datetime
                              'NPI Reactivation Date': 'string',  # datetime
                              'Provider Enumeration Date': 'string'  # datetime
                              }

    npi_data: pd.DataFrame = pd.read_csv(filepath_or_buffer=npi_data_path, usecols=npi_columns, dtype=npi_column_types)

    print("Dropping Non-US Businesses")
    # Drop All Not Null Rows Of "Provider Business Practice Location Address Country Code (If outside U.S.)"
    # TODO: Only Two Null Values To Care About Checking '421 RT 59', 'RUNDU INTERMEDIATE HOSPITAL'
    # npi_data = npi_data[
    #             npi_data["Provider Business Practice Location Address Country Code (If outside U.S.)"].isnull() or
    #             npi_data["Provider Business Practice Location Address Country Code (If outside U.S.)"] == "US"]
    npi_data = npi_data.loc[
        npi_data["Provider Business Practice Location Address Country Code (If outside U.S.)"] == "US"]

    print("Dropping Country Column")
    npi_data.drop(columns=["Provider Business Practice Location Address Country Code (If outside U.S.)"])

    print("Displaying Current State of DataFrame")
    print(npi_data)
    npi_data.info(verbose=False, memory_usage="deep")

    # print("Unique Values For Outside U.S.")
    # print(npi_data['Provider Business Practice Location Address Country Code (If outside U.S.)'].unique())

    # print("Unique Values For Address First Line")
    # print(npi_data['Provider First Line Business Practice Location Address'].unique())

    print("Saving File To CSV For Backup")
    npi_data.to_csv("working/npi_trimmed_backup.csv")

    # "Provider Organization Name (Legal Business Name)",
    # "Provider Last Name (Legal Name)",
    # "Provider First Name",
    # "Provider Middle Name",
    # "Provider Name Prefix Text",
    # "Provider Name Suffix Text",

    print("Filling All Null Data With Empty String")
    npi_data.fillna('', inplace=True)

    name_columns: List[str] = ["Provider Organization Name (Legal Business Name)",
                               "Provider Name Prefix Text",
                               "Provider First Name",
                               "Provider Middle Name",
                               "Provider Last Name (Legal Name)",
                               "Provider Name Suffix Text"]

    # This Only Works Because Organization Name Will Always Be Null If Other Name Is Not Null And Vice Versa
    print("Combining Name Columns To One Column")
    npi_data["name"] = npi_data[name_columns].agg(' '.join, axis=1)

    # Destroy All Old Columns To Save Memory
    print("Dropping All Old Name Columns")
    npi_data.drop(columns=name_columns)

    # Fix For Only One Space Between Columns
    print("Ensuring Only One Space Between Words In Name")
    npi_data["name"] = ' '.join(npi_data["name"].str.split())

    # Uppercase All Names
    print("Uppercase Name Column")
    npi_data["name"] = npi_data["name"].str.upper()

    print("Displaying Current State of DataFrame")
    print(npi_data)
    npi_data.to_csv("working/npi_trimmed_name_backup.csv")

    print("Dropping Deactivated NPIs That Were Never Reactivated")
    npi_data = npi_data.loc[~((not npi_data['NPI Deactivation Date'].isnull()) &
                              (npi_data['NPI Reactivation Date'].isnull()))]

    print("Dropping Re/Deactivation Date Columns")
    npi_data.drop(columns=["NPI Deactivation Date", "NPI Reactivation Date"])

    address_columns: List[str] = ["Provider First Line Business Practice Location Address",
                                  "Provider Second Line Business Practice Location Address"]

    print("Combining Address Columns To One Column")
    npi_data["street_address"] = npi_data[address_columns].agg(', '.join, axis=1)

    print("Dropping All Old Address Columns")
    npi_data.drop(columns=address_columns)

    print("Ensuring Only One Space Between Words In Address")
    npi_data["street_address"] = ' '.join(npi_data["street_address"].str.split())

    print("Uppercase Address Column")
    npi_data["street_address"] = npi_data["street_address"].str.upper()

    rename_columns: dict = {
        "NPI": "npi_number",
        "Provider Business Practice Location Address City Name": "city",
        "Provider Business Practice Location Address State Name": "state",
        "Provider Business Practice Location Address Postal Code": "zip_code",
        "Provider Enumeration Date": "publish_date"
    }

    print("Renaming Columns For Dolt Repo")
    npi_data.rename(columns=rename_columns, inplace=True)

    print("Performing One Last Backup")
    npi_data.to_csv("working/npi_trimmed_final_backup.csv")

    print("Writing To Dolt Repo")
    raw_data_writer = get_df_table_writer('hospitals', lambda: npi_data, ['npi_number'])

    repo: Dolt = Dolt('working/hospital-price-transparency/')
    raw_data_writer(repo)

    print("Done")


# Columns To Keep
# NPI
# "Provider Other Organization Name"
# othername_data: pd.DataFrame = pd.read_csv(filepath_or_buffer=npi_data_path)

# Columns To Keep
# NPI
# "Provider Secondary Practice Location Address- Address Line 1"
# "Provider Secondary Practice Location Address-  Address Line 2"
# "Provider Secondary Practice Location Address - City Name"
# "Provider Secondary Practice Location Address - State Name"
# "Provider Secondary Practice Location Address - Postal Code"
# "Provider Secondary Practice Location Address - Country Code (If outside U.S.)"
# pfile_data: pd.DataFrame = pd.read_csv(filepath_or_buffer=pfile_path)

read_large_npi_file()
