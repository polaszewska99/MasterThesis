SQL_HOBBY_TABLE = '''
                    SET IDENTITY_INSERT Hobbies ON
                    INSERT INTO Hobbies (HobbyID, Name)
                    VALUES (?, ?)
                    SET IDENTITY_INSERT Hobbies OFF
                    '''
TABLE_HOBBY_COLUMNS = ['HobbyID', 'Name']
SQL_CHARACTERISTIC_TABLE = '''
                    SET IDENTITY_INSERT Characteristics ON
                    INSERT INTO Characteristics (CharacteristicID, Name)
                    VALUES (?, ?)
                    SET IDENTITY_INSERT Characteristics OFF
                    '''
TABLE_CHARACTERISTIC_COLUMNS = ['CharacteristicID', 'Name']
SQL_CITY_TABLE = '''
                    SET IDENTITY_INSERT Cities ON
                    INSERT INTO Cities (CityID, Name, CountryID)
                    VALUES (?, ?, ?)
                    SET IDENTITY_INSERT Cities OFF
                    '''
TABLE_CITY_COLUMNS = ['CityID', 'Name', 'CountryID']
SQL_COUNTRY_TABLE = '''
                    SET IDENTITY_INSERT Countries ON
                    INSERT INTO Countries (CountryID, Name)
                    VALUES (?, ?)
                    SET IDENTITY_INSERT Countries OFF
                    '''
TABLE_COUNTRY_COLUMNS = ['CountryID', 'Name']
SQL_PERSON_TABLE = '''
                    SET IDENTITY_INSERT Persons ON
                    INSERT INTO Persons (PersonID, birthdate, first_name, genre, last_name, sexual_orientation, CityID)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    SET IDENTITY_INSERT Persons OFF
                    '''
TABLE_PERSON_COLUMNS = ['PersonID', 'birthdate', 'first_name', 'genre',
                        'last_name', 'sexual_orientation', 'CityID']
