import pandas as pd
import numpy as np
from datetime import datetime


def data_location_error(number_of_records, location_header, df):
    for i in range(0, number_of_records + 2):
        if i > 1:
            for j in range(0, len(location_header)):
                if location_header[j] != 'LOCATION':
                    df.loc[i, location_header[j]] = np.nan
    return df.sample(frac=1)


def data_itemhierarchylevelmembers_error(number_of_records, itemhierarchy_header, df):
    for i in range(0, number_of_records + 2):
        if i > 1:
            for j in range(0, len(itemhierarchy_header)):
                if itemhierarchy_header[j] == 'PRODUCTGROUPID':
                    df.loc[i, itemhierarchy_header[j]] = np.nan
    return df.sample(frac=1)


def data_items_error(number_of_records, items_header, df):
    start = datetime(2000, 1, 1)
    finish = datetime(1999, 1, 1)
    for i in range(0, number_of_records + 2):
        if i > 1:
            for j in range(0, len(items_header)):
                if items_header[j] != 'PRODUCTID':
                    df.loc[i, items_header[j]] = np.nan
                if items_header[j] == 'ACTIVEFROM':
                    df.loc[i, items_header[j]] = start
                if items_header[j] == 'ACTIVEUPTO':
                    df.loc[i, items_header[j]] = finish
    return df.sample(frac=1)


def data_itemlocation_error(number_of_records, itemlocation_header, df):
    productid = 'ERROR_ITEM'
    location = 'ERROR_LOCATION'
    type = 'REPLENISHMENT'
    start = datetime(2000, 1, 1)
    finish = datetime(1999, 1, 1)
    for i in range(0, number_of_records + 2):
        if i > 1:
            for j in range(0, len(itemlocation_header)):
                if itemlocation_header[j] != 'PRODUCTID':
                    df.loc[i, itemlocation_header[j]] = np.nan
                if itemlocation_header[j] == 'PRODUCTID':
                    df.loc[i, itemlocation_header[j]] = productid+str(i-1)
                if itemlocation_header[j] == 'LOCATION':
                    df.loc[i, itemlocation_header[j]] = location+str(i-1)
                if itemlocation_header[j] == 'TYPE':
                    df.loc[i, itemlocation_header[j]] = type
                if itemlocation_header[j] == 'ACTIVEFROM':
                    df.loc[i, itemlocation_header[j]] = start
                if itemlocation_header[j] == 'ACTIVEUPTO':
                    df.loc[i, itemlocation_header[j]] = finish
    return df.sample(frac=1)
