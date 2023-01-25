import csv
from faker.providers import BaseProvider


class MyProvider(BaseProvider):
    __provider__ = "hobby"
    __provider__ = "characteristic"
    __provider__ = "genre"
    __provider__ = "orientation"
    __provider__ = "country_city"

    with open('hobbies.csv', newline='') as f:
        reader = csv.reader(f)
        hobbies = list(reader)

    with open('characteristics.csv', newline='') as f:
        reader = csv.reader(f)
        characteristics = list(reader)

    genres = ["Male", "Female"]
    sexual_orientations = ["heterosexual", "homosexual", "bisexual"]

    def hobby(self):
        h = str(self.random_element(self.hobbies))
        return h[2:len(h) - 2]

    def characteristic(self):
        h = str(self.random_element(self.characteristics))
        return h[2:len(h) - 2]

    def genre(self):
        return str(self.random_element(self.genres))

    def sexual_orientation(self):
        return str(self.random_element(self.sexual_orientations))

    def country_city(self):
        countries_cities = []
        with open('city_country_generate.csv', newline='\n') as f:
            reader = csv.reader(f)
            for row in reader:
                cc = row[0] + ':' + row[1]
                countries_cities.append(cc)
        return str(self.random_element(countries_cities))
