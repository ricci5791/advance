"""Module with show of implemented functionality for YELP problem"""
import os
import re

from minio import Minio

from yelp_reader import YelpReader

if __name__ == '__main__':
    results = []

    yelp_reader = YelpReader()

    results.append(yelp_reader.get_business_all_day_opened())

    results.append(yelp_reader.get_wifi_business_df())

    results.append(yelp_reader.get_business_with_parking_df())

    results.append(yelp_reader.get_station_with_cafe_shop_df())

    results.append(yelp_reader.get_user_friends_df())

    client = Minio('s3:9000/',
                   '"access_key"',
                   '"secret_key"',
                   secure=False, )

    if not client.bucket_exists('testbucket'):
        client.make_bucket('testbucket')

    titles = ['24_hours_business', 'business_with_wifi', 'business_with_parking',
              'gas_stations_with_food', 'user_friends_attendies']
    file_names = []

    root_dir = os.getcwd()
    regex = re.compile('(.*csv$)')

    for title, df in zip(titles, results):
        yelp_reader.save_to_csv(df, title)

    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if regex.match(file):
                file_names.append(root + "/" + file)

    for title, file_name in zip(titles, file_names):
        client.fput_object('testbucket', title, file_name)
