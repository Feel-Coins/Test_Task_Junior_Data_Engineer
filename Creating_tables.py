import psycopg2
from psycopg2 import Error
from datetime import datetime
from random import randint

try:
    # Connection to BD
    connection = psycopg2.connect(user="postgres",
                                  password="1111",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres_db")

    # Making a cursor to work with db
    cursor = connection.cursor()

    # Create tables
    create_table_query = ['''CREATE TABLE Users(
                          userId   BIGSERIAL    PRIMARY KEY     NOT NULL,
                          age        SMALLINT       NOT NULL); ''',
                          '''CREATE TABLE Purchases(
                          purchaseId  BIGSERIAL     PRIMARY KEY      NOT NULL,
                          date       date      NOT NULL); ''',
                          '''CREATE TABLE Items(
                          itemId     BIGSERIAL   PRIMARY KEY      NOT NULL,
                          price      INT        NOT NULL); '''
                          ]

    for table in create_table_query:
        cursor.execute(table)
        connection.commit()

    # Add references in Purchaises
    insert_query = ["ALTER TABLE Purchases ADD UserId BIGINT REFERENCES Users (userId);",
                    "ALTER TABLE Purchases ADD itemId BIGINT REFERENCES Items (itemId);"]
    for i in insert_query:
        cursor.execute(i)
        connection.commit()

    # Insert data
    # Users
    for age in range(16, 99):
        insert_query = f"INSERT INTO Users (age) VALUES ({age})"
        cursor.execute(insert_query)
        connection.commit()
    # Items
    price_list = [27794, 11791, 57235, 40121, 47012, 22747, 17030, 42888, 11591, 50788, 56737, 44938, 16805, 42536, 6896, 47959, 53417, 12495, 43618,
                  43201, 25358, 6137, 12047, 6478, 10118, 25024, 49061, 58174, 29838, 8479, 16541, 51467, 19158, 3458, 32037, 7198, 52415, 49170,
                  28452, 45257, 58471, 5285, 43330, 56202, 12486, 46636, 53197, 24980, 17917, 47415, 34462, 7822, 45204, 26522, 26341, 54004, 41602,
                  27027, 55532, 14185, 19496, 3695, 24440, 41417, 55770, 33606, 28527, 56393, 9688, 46872, 50328, 17950, 46510, 22373, 3967, 47890,
                  54817, 27389, 50764, 38910, 45432, 24109, 54135, 37510, 4969, 29662, 26660, 18249, 12894, 59177, 23447, 54814, 44152, 36533, 48024,
                  11818, 43780, 5118, 59489, 47967]
    for price in price_list:
        insert_query = f"INSERT INTO Items (price) VALUES ({price})"
        cursor.execute(insert_query)
        connection.commit()

    # Generate and insert dates
    for year in range(2009, 2023):
        for month in range(1, 13):
            if month < 10:
                month = f'0{month}'
            for day in range(1, 29):
                if day < 10:
                    day = f'0{day}'
                    userId = randint(1, 83)
                    itemId = randint(1, 100)
                    insert_query = f"INSERT INTO Purchases (date, userId, itemId) VALUES ('{day}-{month}-{year}', {userId}, {itemId})"
                    cursor.execute(insert_query)

                connection.commit()


except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")
