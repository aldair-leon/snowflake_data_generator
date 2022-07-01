import pandas as pd
import numpy as np
from datetime import datetime


def data_location_error(number_of_records, location_header, df):
    location = 'ERROR_LOCATION_'
    for i in range(0, number_of_records):
        for j in range(0, len(location_header)):
            if location_header[j] == 'LOCATION':
                df.loc[i, location_header[j]] = location + str(i + 1)
            if location_header[j] != 'LOCATION':
                df.loc[i, location_header[j]] = np.nan
    return df.sample(frac=1)


def data_itemhierarchylevelmembers_error(number_of_records, itemhierarchy_header, df):
    productID = 'ERROR_PRODUCTGROUPID'
    for i in range(0, number_of_records):
        for j in range(0, len(itemhierarchy_header)):
            if itemhierarchy_header[j] == 'PRODUCTGROUPID':
                df.loc[i, itemhierarchy_header[j]] = productID
    return df.sample(frac=1)


def data_items_error(number_of_records, items_header, df):
    start = datetime(2000, 1, 1)
    finish = datetime(1999, 1, 1)
    productid = 'ERROR_ITEM_'
    for i in range(0, number_of_records):
        for j in range(0, len(items_header)):
            if items_header[j] == 'PRODUCTID':
                df.loc[i, items_header[j]] = productid + str(i + 1)
            if items_header[j] == 'ACTIVEFROM':
                df.loc[i, items_header[j]] = start
            if items_header[j] == 'ACTIVEUPTO':
                df.loc[i, items_header[j]] = finish
            if items_header[j] != 'PRODUCTID':
                df.loc[i, items_header[j]] = np.nan
    return df.sample(frac=1)


def data_itemlocation_error(number_of_records, itemlocation_header, df):
    productid = 'ERROR_ITEM_'
    location = 'ERROR_LOCATION_'
    type = 'REPLENISHMENT_'
    start = datetime(2000, 1, 1)
    finish = datetime(1999, 1, 1)
    for i in range(0, number_of_records + 2):
        for j in range(0, len(itemlocation_header)):
            if itemlocation_header[j] != 'PRODUCTID':
                df.loc[i, itemlocation_header[j]] = np.nan
            if itemlocation_header[j] == 'PRODUCTID':
                df.loc[i, itemlocation_header[j]] = productid + str(i + 1)
            if itemlocation_header[j] == 'LOCATION':
                df.loc[i, itemlocation_header[j]] = location + str(i + 1)
            if itemlocation_header[j] == 'TYPE':
                df.loc[i, itemlocation_header[j]] = type
            if itemlocation_header[j] == 'ACTIVEFROM':
                df.loc[i, itemlocation_header[j]] = start
            if itemlocation_header[j] == 'ACTIVEUPTO':
                df.loc[i, itemlocation_header[j]] = finish
    return df.sample(frac=1)


def data_inventorytransactions_error(number_of_records, inventorytrans_header, df):
    productid = 'ERROR_ITEM_'
    location = 'ERROR_LOCATION_'
    type = 'ERROR_TYPE_'
    start = datetime(2000, 1, 1)
    finish = datetime(1999, 1, 1)
    for i in range(0, number_of_records):
        for j in range(0, len(inventorytrans_header)):
            if inventorytrans_header[j] != 'PRODUCTID':
                df.loc[i, inventorytrans_header[j]] = np.nan
            if inventorytrans_header[j] == 'PRODUCTID':
                df.loc[i, inventorytrans_header[j]] = productid + str(i + 1)
            if inventorytrans_header[j] == 'LOCATIONID':
                df.loc[i, inventorytrans_header[j]] = location + str(i + 1)
            if inventorytrans_header[j] == 'TYPE':
                df.loc[i, inventorytrans_header[j]] = type + str(i + 1)
            if inventorytrans_header[j] == 'QUANTITY':
                df.loc[i, inventorytrans_header[j]] = -1
            if inventorytrans_header[j] == 'STARTDATE':
                df.loc[i, inventorytrans_header[j]] = start
            if inventorytrans_header[j] == 'LASTSOLDTIME':
                df.loc[i, inventorytrans_header[j]] = finish
    return df.sample(frac=1)


def data_inventoryonhand_error(number_of_records, inventoryonhand_header, df):
    productid = 'ERROR_ITEM_'
    location = 'ERROR_LOCATION_'
    for i in range(0, number_of_records):
        for j in range(0, len(inventoryonhand_header)):
            if inventoryonhand_header[j] != 'PRODUCTID':
                df.loc[i, inventoryonhand_header[j]] = np.nan
            if inventoryonhand_header[j] == 'PRODUCTID':
                df.loc[i, inventoryonhand_header[j]] = productid + str(i)
            if inventoryonhand_header[j] == 'LOCATION':
                df.loc[i, inventoryonhand_header[j]] = location + str(i)
            if inventoryonhand_header[j] == 'QUANTITY':
                df.loc[i, inventoryonhand_header[j]] = -1
    return df.sample(frac=1)
