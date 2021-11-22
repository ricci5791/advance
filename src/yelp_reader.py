"""Module with YELP task implementation"""
import os

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql import Window

import file_paths

parking_schema = t.StructType([t.StructField('garage', t.BooleanType()),
                               t.StructField('street', t.BooleanType()),
                               t.StructField('validated', t.BooleanType()),
                               t.StructField('lot', t.BooleanType()),
                               t.StructField('valet', t.BooleanType())])

SECONDS_IN_WEEK = 25200


class YelpReader:
    """Class with data load from YELP datasets and extract of useful information"""

    def __init__(self):
        cores_num = os.environ.get('spark_cores')

        if isinstance(cores_num, int):
            master_str = f'local[{cores_num}]'
        else:
            master_str = 'local'

        self.spark = (SparkSession.builder
                      .master(master_str)
                      .appName('sparkYelp')
                      .getOrCreate())

        self.spark.conf.set("spark.sql.session.timeZone", "UTC")

        raw_business_df = (self.spark
                           .read
                           .json(file_paths.business_dataset)
                           .select('business_id', 'name', 'hours', 'attributes', 'attributes.BikeParking',
                                   'attributes.BusinessParking', 'attributes.WiFi',
                                   f.from_json(f.lower(f.col('attributes.BusinessParking')), parking_schema)
                                   .alias('car_parking'),
                                   f.split(f.col('categories'), ', ').alias('categories'))
                           )

        raw_business_df = (raw_business_df
                           .select('*',
                                   f.col('attributes.RestaurantsPriceRange2').astype(t.IntegerType())
                                   .alias('price_range'))
                           .drop('Open24Hours'))

        self.business_df = raw_business_df
        #
        # self.checkins_df = (self.spark
        #                     .read
        #                     .json(file_paths.checkin_dataset))

        review_df = (self.spark
                     .read
                     .json(file_paths.review_dataset)
                     .select('business_id', 'user_id')
                     .distinct())

        # tip_df = (self.spark
        #           .read
        #           .json(file_paths.tip_dataset)
        #           .select('business_id', 'user_id')
        #           .distinct())
        #
        self.reviews_df = review_df

        self.user_df = (self.spark
                        .read
                        .json(file_paths.user_dataset)
                        .select('user_id', 'name', 'friends')
                        .withColumn('friends',
                                    f.split(f.col('friends'), ', '))
                        )

    def save_to_csv(self, df: pyspark.sql.DataFrame, file_path):
        """Internal function for saving dataframes to csv files"""
        (df
         .coalesce(1)
         .write
         .csv(file_path, mode='overwrite', header=True))

    def get_business_all_day_opened(self):
        """
        Create a dataframe with businesses that on average works more than 12 hours

        :return: Businesses that works that on average works more than 12 hours
        :rtype: pyspark.DataFrame
        """
        days_list = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        open24_condition = ((f.col('Monday') == '0:0-24:0') | (f.col('Tuesday') == '0:0-24:0')
                            | (f.col('Wednesday') == '0:0-24:0') | (f.col('Thursday') == '0:0-24:0')
                            | (f.col('Friday') == '0:0-24:0') | (f.col('Saturday') == '0:0-24:0')
                            | (f.col('Sunday') == '0:0-24:0'))

        filtered_business_df = (self.business_df
                                .filter(f.col('hours').isNotNull())
                                .select('business_id', 'name', 'hours',
                                        *[f.when(f.col('hours.' + day) == '0:0-0:0', '0:0-24:0')
                                        .otherwise(f.col('hours.' + day)).alias(day) for day in days_list])
                                .withColumn('open24', f.coalesce(open24_condition, f.lit(False))))

        split_hours = ([f.split(f.col(day), '-').alias(day) for day in days_list])

        business_hours_df = filtered_business_df.select('business_id', 'name', 'hours', 'open24', *split_hours)

        business_hours_df = (business_hours_df.select('*',
                                                      *[f.unix_timestamp(f.col(day)[0], 'H:m').alias(day + '_open')
                                                        for day in days_list],
                                                      *[f.unix_timestamp(f.col(day)[1], 'H:m').alias(day + '_closed')
                                                        for day in days_list])
                             .select('business_id', 'name', 'open24',
                                     *[day + '_open' for day in days_list],
                                     *[day + '_closed' for day in days_list])
                             )

        business_avg_hours_df = (business_hours_df.withColumn('avg_hours',
                                                              (f.coalesce(f.col('Monday_closed'), f.lit(0))
                                                               + f.coalesce(f.col('Tuesday_closed'), f.lit(0))
                                                               + f.coalesce(f.col('Wednesday_closed'), f.lit(0))
                                                               + f.coalesce(f.col('Thursday_closed'), f.lit(0))
                                                               + f.coalesce(f.col('Friday_closed'), f.lit(0))
                                                               + f.coalesce(f.col('Saturday_closed'), f.lit(0))
                                                               + f.coalesce(f.col('Sunday_closed'), f.lit(0))
                                                               - f.coalesce(f.col('Monday_open'), f.lit(0))
                                                               - f.coalesce(f.col('Tuesday_open'), f.lit(0))
                                                               - f.coalesce(f.col('Wednesday_open'), f.lit(0))
                                                               - f.coalesce(f.col('Thursday_open'), f.lit(0))
                                                               - f.coalesce(f.col('Friday_open'), f.lit(0))
                                                               - f.coalesce(f.col('Saturday_open'), f.lit(0))
                                                               - f.coalesce(f.col('Sunday_open'), f.lit(0)))
                                                              / SECONDS_IN_WEEK)
                                 .filter(f.col('avg_hours') >= 12)
                                 .select('business_id', 'name', 'avg_hours', 'open24')
                                 )

        return business_avg_hours_df

    def get_wifi_business_df(self):
        """
        Create dataframe with businesses that has WIFI and specify whether it is free or not

        :return: dataframe with column of WiFi presence
        :rtype: pyspark.DataFrame
        """
        business_wifi_df = (self.business_df
                            .filter(f.col('WiFi').isNotNull())
                            .withColumn('WiFi', f.regexp_extract(f.col('WiFi'), "'([A-z]+)'", 1))
                            .withColumn('free_wifi', f.col('WiFi') == 'free')
                            .select('business_id', 'name', 'free_wifi'))

        return business_wifi_df

    def get_business_with_parking_df(self):
        """
        Create dataframe with businesses that has parking lots for cars and bikes

        :return: dataframe with columns of parking lot presence
        :rtype: pyspark.DataFrame
        """
        business_parking_df = self.business_df

        business_with_parking_df = (business_parking_df
                                    .withColumn('has_car_parking', f.col('car_parking.lot'))
                                    .withColumn('has_bike_parking', f.col('BikeParking').astype(t.BooleanType()))
                                    .filter(f.col('has_bike_parking') | f.col('has_car_parking'))
                                    .select('business_id', 'name', 'has_bike_parking', 'has_car_parking'))

        return business_with_parking_df

    def get_station_with_cafe_shop_df(self):
        """
        Create dataframe with gas stations that has food related shop with some price rating

        :return: dataframe of gas stations with food
        :rtype: pyspark.DataFrame
        """
        cafe_condition = (f.array_contains(f.col('categories'), 'Cafes')
                          & (f.col('price_range') <= 3))

        gas_stations_df = (self.business_df
                           .select('business_id', 'name', 'categories', 'price_range')
                           .filter(f.array_contains(f.col('categories'), 'Gas Stations'))
                           )

        gas_stations_food_df = (gas_stations_df
                                .withColumn('has_store',
                                            f.array_contains(f.col('categories'), 'Convenience Stores').cast('string'))
                                .withColumn('has_cafe', cafe_condition)
                                .withColumn('categories', f.col('categories').cast('string'))
                                )

        return gas_stations_food_df

    def get_checkin_per_business_df(self):
        """
        Create dataframe with businesses with count checkins by year for each business

        :return: dataframe with count column
        :rtype: pyspark.DataFrame
        """
        split_checkins_time_df = self.checkins_df.withColumn('date', f.split(f.col('date'), ','))

        exploded_checkins_df = split_checkins_time_df.select('business_id', f.explode('date').alias('date'))

        datetime_casted_checkins_df = exploded_checkins_df.select('business_id',
                                                                  f.year(f.col('date').cast(t.DateType())).alias(
                                                                      'year'))

        business_window = Window.partitionBy('business_id', 'year')

        business_checkins_count = (datetime_casted_checkins_df
                                   .withColumn('checkin_count', f.count('business_id').over(business_window))
                                   .distinct())

        result_checkins_df = (self.business_df
                              .select('business_id', 'name')
                              .join(business_checkins_count, on='business_id', how='inner'))

        return result_checkins_df

    def get_user_friends_df(self):
        """
        Create dataframe with businesses and users friends that have attended the same place

        :return: dataframe with business and user friends attendees information
        :rtype: pyspark.DataFrame
        """

        # Due to really big amount of data produced (more than 20 GB), used tip input data (200k rows)
        reviews_with_names_df = self.reviews_df.join(self.user_df.select('user_id', 'name'), on='user_id', how='inner')

        business_reviews_df = (self.business_df
                               .select('business_id')
                               .join(reviews_with_names_df, on='business_id', how='inner'))

        visited_df = (business_reviews_df
                      .groupBy('business_id')
                      .agg(f.collect_set('user_id').alias('visited_by')))

        business_visited_df = (business_reviews_df
                               .join(visited_df, on='business_id', how='inner'))

        users_with_friends = (self.user_df
                              .select('user_id', 'friends')
                              .filter(f.size(f.col('friends')) >= 1)
                              )

        business_visited_friends_df = business_visited_df.join(users_with_friends, on='user_id', how='inner')

        intersected_friends_df = (business_visited_friends_df
                                  .withColumn('friends_attendees',
                                              f.array_intersect(f.col('visited_by'), f.col('friends'))
                                              .astype("string"))
                                  .select('business_id', 'user_id', 'name', 'friends_attendees'))

        return intersected_friends_df
