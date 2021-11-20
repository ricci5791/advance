"""Module with show of implemented functionality for YELP problem"""

from yelp_reader import YelpReader

if __name__ == '__main__':
    yelp_reader = YelpReader()

    yelp_reader.get_business_all_day_opened().show()

    yelp_reader.get_wifi_business_df().show()

    yelp_reader.get_business_with_parking_df().show()

    yelp_reader.get_station_with_cafe_shop_df().show()

    # yelp_reader.get_checkin_per_business_df().show()

    yelp_reader.get_user_friends_df().show()
