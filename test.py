import random
from datetime import datetime, date, timedelta
from faker import Faker
import pandas as pd
from faker.providers import BaseProvider

fake = Faker(["en_US"])

location_type_options = ['SUPPLIER', 'DISTRIBUTION_CENTER', 'STORE']

header_location = ['LOCATION',
                   'LOCATIONNAME',
                   'LOCATIONTYPE',
                   'COUNTRY',
                   'POSTALCODE',
                   'CITY',
                   'ADDRESS',
                   'LATITUDE',
                   'LONGITUDE',
                   'ACTIVEFROM',
                   'ACTIVEUPTO',
                   'STATE']


class MyProvider(BaseProvider):
    def location(self) -> str:
        date_end = date.today() + timedelta(days=10)
        date_start = date_end - timedelta(days=9)
        locations = [fake.bothify(text='???-####', letters='ABCDEF') for i in range(100)]
        locations_name = [fake.company() for i in range(100)]
        location_type = [random.choice(location_type_options) for i in range(100)]
        country = ['US' for i in range(100)]
        postal_code = [fake.building_number() for i in range(100)]
        city = [fake.city() for i in range(100)]
        street_address = [fake.street_address() for i in range(100)]
        latitud_ = [float(fake.latitude()) for i in range(100)]
        longitud_ = [float(fake.latitude()) for i in range(100)]
        active_from = [str(fake.date('%m-%d-%y')) for i in range(100)]
        active_up = [datetime.strptime(str(fake.date_between_dates(date_start, date_end)),
                                       '%Y-%m-%d').strftime('%m-%d-%y') for i in range(100)]
        state = [fake.country_code() for i in range(100)]
        return locations, locations_name, location_type, country, postal_code, city, street_address, latitud_, longitud_, \
               active_from, active_up, state


fake.add_provider(MyProvider)
data = list(fake.location())

df = pd.DataFrame(columns=header_location)
for i in range(0, len(header_location)):
    df[header_location[i]] = data[i]
df.to_csv('file_name.csv', encoding='utf-8', index=False)
