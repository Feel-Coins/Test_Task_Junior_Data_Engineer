import psycopg2
from psycopg2 import Error

# Задание А1
task_a1 = '''select sum(price) / count (date_part('month', p.Date)) as avg_sum
from users as u
join purchases as p on p.userid = u.userid
join items as i on i.itemid = p.itemid
where u.age between 18 and 25;'''
# Задание А2
task_a2 = '''select sum(price) / count (date_part('month', p.Date)) as avg
from users as u
join purchases as p on p.userid = u.userid
join items as i on i.itemid = p.itemid
where u.age between 26 and 35;'''
# Задание Б
task_b ='''select date_part('month', p.Date) as month,
sum(price) as sum_price
from users as u
join purchases as p on p.userid = u.userid
join items as i on i.itemid = p.itemid
where u.age >= 35 and date_part('year', p.Date) = 2022
group by  month
order by sum_price Desc
LIMIT 1;'''
# Задание В
task_c ='''select p.itemid,
sum(price) as sum_price
from users as u
join purchases as p on p.userid = u.userid
join items as i on i.itemid = p.itemid
where date_part('year', p.Date) = 2022
group by p.itemid, date_part('year', p.Date)
order by sum_price Desc
Limit 1;'''
# Задание Г
task_g = '''select p.itemid,
	sum(price) as sum_price,
	(sum(price) * 100. / (select sum(price)
		from users as u 
		join purchases as p on p.userid = u.userid
		join items as i on i.itemid = p.itemid
		where date_part('year', p.Date) = 2022
		group by date_part('year', p.Date))) as percentage
from users as u
	join purchases as p on p.userid = u.userid
	join items as i on i.itemid = p.itemid
where date_part('year', p.Date) = 2022
group by p.itemid, date_part('year', p.Date), i.price
order by sum_price Desc
Limit 3;'''

try:
    # Connection to BD
    connection = psycopg2.connect(user="postgres",
                                  password="1111",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres_db")

    # Making a cursor to work with db
    cursor = connection.cursor()

    cursor.execute(task_a1)
    out_A1 = cursor.fetchall()[0][0]
    cursor.execute(task_a2)
    out_A2 = cursor.fetchall()[0][0]
    cursor.execute(task_b)
    out_B = cursor.fetchall()
    cursor.execute(task_c)
    out_C = cursor.fetchall()
    cursor.execute(task_g)
    out_G = cursor.fetchall()
    print(f'Пользователи от 18 до 25 лет в среднем тратят {out_A1} У.Е. в месяц')
    print(f'Пользователи от 26 до 35 лет в среднем тратят {out_A2} У.Е. в месяц\n')
    print(f'В {int(out_B[0][0])} месяце 2022 года выручка от пользователей старше 35 лет стала самой большой и составила {out_B[0][1]} У.Е.\n')
    print(f'Товар под номером {out_C[0][0]} в этом году дает наибольшую выручку ({out_C[0][1]} У.Е.)\n')
    for i in [0,1,2]:
        print(f'Топ 3 товаров по доходности и их доля к общей годовой выручке:',
              f'Товар №{out_G[i][0]} принес {out_G[i][1]} У.Е., что составило {float("{:.3f}".format(out_G[i][2]))}% от всех продаж в 2022 году',
              sep='\n *')

except (Exception, Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
