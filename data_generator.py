import re
from MyProvider import MyProvider
import pandas as pd
import csv
from faker import Faker
import os
import random


def limit_country_city(number_rows):
    worldcities_df = pd.read_csv("worldcities.csv",
                                 sep=',',
                                 names=["City", "Country"])
    country_df = worldcities_df["Country"]
    country_df = country_df.drop_duplicates()

    divide_num_city = 1
    if number_rows >= 1000:
        divide_num_city = 10
    if number_rows >= 100000:
        divide_num_city = 100
    elif number_rows <= 100:
        divide_num_city = number_rows
    else:
        divide_num_city = 10

    divide_num_countries = 1
    if number_rows <= 10000:
        divide_num_countries = number_rows
    elif 10000 >= number_rows <= 100000:
        divide_num_countries = int(number_rows / 10)
    elif 100000 >= number_rows <= 1000000:
        divide_num_countries = int(number_rows / 50)
    else:
        number_millions = int(number_rows / 1000000)
        divide_num_countries = 100 * number_millions

    num_cities = int(number_rows / divide_num_city)
    num_countries = int(number_rows / divide_num_countries)

    country_research = country_df.head(num_countries)

    worldcities_df_filtered = worldcities_df.query("Country in @country_research").head(num_cities)
    worldcities_df_filtered.to_csv("city_country_generate.csv", header=False, index=False)
    return "city_country_generate.csv"


def fake_person(number_rows):
    id_person = 0
    limit_country_city(number_rows)
    fake = Faker()
    fake.add_provider(MyProvider)

    with open('C:\\Users\\alicj\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-1135eac1-af43-4af5-bcea-dcb29f1a1b3b\\import\\person.csv', 'a+', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if os.stat("C:\\Users\\alicj\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-1135eac1-af43-4af5-bcea-dcb29f1a1b3b\\import\\person.csv").st_size == 0:
            writer.writerow(
                ["ID", "First Name", "Last Name", "genre", "orientation", "birthdate", "Country", "City",
                 "Has_Characteristic", "Wants_Characteristic", "Hobbies"])
        else:
            with open(
                    'C:\\Users\\alicj\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-1135eac1-af43-4af5-bcea-dcb29f1a1b3b\\import',
                    'r') as f:
                last_line = f.readlines()[-1]
            result = re.split(',', last_line)
            id_person = int(result[0]) + 1

        for _ in range(number_rows):
            hobbies = ""
            characteristics_has = ""
            characteristics_want = ""
            iterator_hobby = 0
            iterator_ch_has = 0
            iterator_ch_wants = 0
            hobbies_amount = random.randint(1, 5)
            ch_has_amount = random.randint(1, 8)
            ch_want_amount = random.randint(1, 10)
            for _ in range(hobbies_amount):
                if iterator_hobby > 0:
                    hobbies = hobbies + ":" + str(fake.hobby())
                else:
                    hobbies = str(fake.hobby())
                iterator_hobby = iterator_hobby + 1

            for _ in range(ch_has_amount):
                if iterator_ch_has > 0:
                    characteristics_has = characteristics_has + ":" + str(fake.characteristic())
                else:
                    characteristics_has = str(fake.characteristic())
                iterator_ch_has = iterator_ch_has + 1

            for _ in range(ch_want_amount):
                if iterator_ch_wants > 0:
                    characteristics_want = characteristics_want + ":" + str(fake.characteristic())
                else:
                    characteristics_want = str(fake.characteristic())
                iterator_ch_wants = iterator_ch_wants + 1

            genre = fake.genre()
            country_and_city = fake.country_city()
            country = country_and_city[country_and_city.find(':') + 1:len(country_and_city)]
            city = country_and_city[0:country_and_city.find(':')]

            writer.writerow([id_person, fake.first_name_female() if genre == 'Female' else fake.first_name_male(),
                             fake.last_name(), genre, fake.sexual_orientation(),
                             fake.date_of_birth(minimum_age=18, maximum_age=70), country, city,
                             characteristics_has, characteristics_want, hobbies])
            id_person = id_person + 1
