import MySQLdb as _mysql
from collections import namedtuple
import re


Point = namedtuple('Point', 'x y')
pt = Point(1.0, 5.0)

float_match = re.compile(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?$').match


def is_number(string):
    return bool(float_match(string))


class MySQLDatabase(object):
    def __init__(self, database_name, username, password, host='localhost'):
        try:
            self.db = _mysql.connect(db=database_name, host=host, user=username, passwd=password)
            self.database_name = database_name

            print "Connected to MySQL!"

        except _mysql.Error, e:
            print e

    def get_available_tables(self):
        cursor = self.db.cursor()
        cursor.execute("SHOW TABLES;")

        self.tables = cursor.fetchall()

        cursor.close()
        return self.tables

    def get_columns_for_table(self, table_name):
        cursor = self.db.cursor()
        cursor.execute("SHOW COLUMNS FROM `%s`" % table_name)
        self.columns = cursor.fetchall()

        cursor.close()

        return self.columns

    def convert_to_named_tuples(self, cursor):
        results = None
        names = " ".join(d[0] for d in cursor.description)
        klass = namedtuple('Results',names)

        try:
            results = map(klass._make, cursor.fetchall())
        except _mysql.ProgrammingError, e:
            print e

        return results

    def select(self, table, columns=None, named_tuples=False, **kwargs):
        sql_str = "SELECT "

        # add columns or just use the wildcard
        if not columns:
            sql_str += "* "
        else:
            for column in columns:
                sql_str += "%s, " % column

            sql_str = sql_str[:-2]  # remove the last comma!

        # add the table to the SELECT query
        sql_str += " FROM `%s`.`%s`" % (self.database_name, table)

        # if there is a JOIN clause attached
        if kwargs.has_key('join'):
            sql_str += " JOIN %s " % kwargs.get('join')

        # if there is a WHERE clause attached
        if kwargs.has_key('where'):
            sql_str += " WHERE %s " % kwargs.get('where')

        # if there is a LIMIT clause attached
        if kwargs.has_key('limit'):
            sql_str += " LIMIT %s" % kwargs.get('limit')

        # if there is a ORDER BY clause attached
        # order_asc
        if kwargs.has_key('order_asc'):
            sql_str += " ORDER BY %s" % kwargs.get('order_asc')

        # order_desc
        if kwargs.has_key('order_desc'):
            sql_str += " ORDER BY %s DESC" % kwargs.get('order_desc')

        sql_str += ";"  # finalise out SQL string

        cursor = self.db.cursor()
        cursor.execute(sql_str)

        if named_tuples:
            results = self.convert_to_named_tuples(cursor)
        else:
            results = cursor.fetchall()

        cursor.close()

        return results

    def insert(self, table, **column_names):
        sql_str = "INSERT INTO `%s`.`%s` " % (self.database_name, table)
        if column_names is not None:
            columns = "("
            values = "("
            for arg, value in column_names.iteritems():
                columns += "`%s`, " % arg
                if is_number(value) or arg == 'DOB':
                    values += "%s, " % value
                else:
                    values += "'%s', " % value
            columns = columns[:-2]
            values = values[:-2]
            columns += ") VALUES"
            values += ");"

            sql_str += "%s %s" % (columns, values)

            cursor = self.db.cursor()
            cursor.execute(sql_str)
            self.db.commit()
            cursor.close()

    def update(self, table, where=None, **column_names):
        sql_str = "UPDATE `%s`.`%s` SET " % (self.database_name, table)
        if column_names is not None:
            for column_name, value in column_names.iteritems():
                sql_str += "`%s`=" % column_name
                if is_number(value):
                    sql_str += "%s, " % value
                else:
                    sql_str += "'%s', " % value
            sql_str = sql_str [:-2]
            if where:
                sql_str += " WHERE %s" % where

            cursor = self.db.cursor()
            cursor.execute(sql_str)
            self.db.commit()
            cursor.close()

    def delete(self, table, **wheres):
        sql_str = "DELETE FROM `%s`.`%s`"% (self.database_name, table)

        if wheres is not None:
            first_where_clause = True
            for where, term in wheres.iteritems():
                if first_where_clause:
                    # This is the first WHERE clause
                    sql_str += " WHERE `%s`.`%s`%s" % (table, where, term)
                    first_where_clause = False
                else:
                    # this is an additional clause to use AND
                    sql_str += " AND `%s`.`%s`%s" (table, where, term)
        sql_str += ";"

        cursor = self.db.cursor()
        cursor.execute(sql_str)
        self.db.commit()
        cursor.close()

    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close()
            print "MySQL connection closed."


