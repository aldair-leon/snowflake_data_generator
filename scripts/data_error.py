import pandas as pd
import numpy as np


def data_location_error(number_of_records, location_header, df):
    for i in range(0, number_of_records + 2):
        if i > 1:
            for j in range(0, len(location_header)):
                if location_header[j] != 'LOCATION':
                    df.loc[i, location_header[j]] = np.nan
    return df


def data_itemhierarchylevelmembers_error(number_of_records, itemhierarchy_header, df):
    for i in range(0, number_of_records + 2):
        if i > 1:
            for j in range(0, len(itemhierarchy_header)):
                if itemhierarchy_header[j] == 'PRODUCTGROUPID':
                    df.loc[i, itemhierarchy_header[j]] = np.nan
    return df
