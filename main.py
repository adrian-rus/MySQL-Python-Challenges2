import random
from d.mysql import MySQLDatabase
from settings import db_config

if __name__ == "__main__":
    db = MySQLDatabase(db_config.get('db_name'),
             db_config.get('user'),
             db_config.get('pass'),
             db_config.get('host'))

    tables = db.get_available_tables()
    print tables

    columns = db.get_columns_for_table('people')
    print 'people columns: ', columns

    results = db.select('people')
    for row in results:
        print row

    people = db.select('people', columns=["CONCAT(first_name, ' ', second_name) AS full_name",
                       "SUM(amount) AS total_spend"],
                       named_tuples=True, where="people.id=1",
                       join="orders ON people.id=orders.person_id")

    for person in people:
        print person.full_name, "spent ", person.total_spend

    people = db.select('people', columns=["first_name", "AVG(amount) AS average_spend"],
                       named_tuples=True, where="people.id=2",
                       join="orders ON people.id=orders.person_id")

    for person in people:
        print person.first_name, "spent on average ", person.average_spend

    db.insert('people', first_name='Adi', second_name='Rus', DOB='STR_TO_DATE("10-03-1987", "%d-%m-%Y")')

    adi = db.select('people', ["id", "first_name"], where="first_name='Adi'", named_tuples=True)

    adi = adi[0]

    db.insert('profiles', person_id="%s" % adi.id, address="15 Main Street, Dun Laoghaire")

    db.insert('orders', person_id="%s" % adi.id, amount="%s" % random.randint(2, 18))
    db.insert('orders', person_id="%s" % adi.id, amount="%s" % random.randint(2, 18))

    orders = db.select('orders', where="person_id=%s" % adi.id)

    for order in orders:
        print order

    people = db.select('people', columns=["CONCAT(first_name, ' ', second_name) AS full_name",
                                          "MIN(amount) AS min_spend"],
                       named_tuples=True, where="people.id=5",
                       join="orders ON people.id=orders.person_id")

    for person in people:
        print person.full_name, "spent ", person.min_spend

    x = db.select('people', named_tuples=True)[4]
    db.update('orders', where="person_id=%s" % x.id, amount='20.02')

"""
columns_o = db.get_columns_for_table('orders')
print 'orders columns: ', columns_o

columns_a = db.get_columns_for_table('articles')
print 'articles columns: ', columns_a"""

""""# Get all the records from the people table
all_records = db.select('people')
print 'All records: %s' % str(all_records)

# Get all the records from the people table but only the `id` and `first name` column
column_specific_records = db.select('people', ['id', 'first_name'])
print 'Column specific records %s' % str(column_specific_records)

# Select data using WHERE clause
where_expression_records = db.select('people', ['first_name'], where="first_name='John'")
print 'Where records: %s' % str(where_expression_records)

# select data using WHERE and JOIN clause
joined_records = db.select('people', ['first_name'], where="people.id=3", join="orders ON people.id=orders.person_id")
print 'Joined records: %s' % str(joined_records)

# select data and printing a LIMIT of 4 results
orders_records = db.select('orders', limit=2)
print 'First 2 order records are: %s' % str(orders_records)

# print orders in ascending order ORDERED BY amount
ascending_orders = db.select('orders', order_asc='amount')
for results in ascending_orders:
    print results

# print orders in descending order ORDERED BY amount
descending_orders = db.select('orders', order_desc='amount')
for results in descending_orders:
    print results"""

"""print '----------------------'

db.delete('orders', id='=3')
db.delete('orders', id='>=4')
print '----------------'
print db.select('orders')"""

# db.insert('orders', id='3', amount='10.10', person_id='1')
# db.insert('orders', id='4', amount='25.26', person_id='2')
# db.update('orders', where='id=1', amount='25')
