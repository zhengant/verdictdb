from datetime import datetime, date
import os
import pyverdict
import psycopg2


test_schema = 'pyverdict_datatype_test_schema'
test_table = 'pyverdict_datatype_test_table'


def test_data_types():
    (postgresql_conn, verdict_conn) = setup_sandbox()

    result = verdict_conn.sql('select * from {}.{};'.format(test_schema, test_table))
    int_types = result.typeJavaInt()
    types = result.types()
    rows = result.rows()
    # print(int_types)
    # print(types)
    print(rows)
    # print([type(x) for x in rows[0]])

    cur = postgresql_conn.cursor()
    cur.execute('select * from {}.{};'.format(test_schema, test_table))
    expected_rows = cur.fetchall()
    print(expected_rows)
    cur.close()

    # Now test
    assert len(expected_rows) == len(rows)
    assert len(expected_rows) == result.rowcount

    for i in range(len(expected_rows)):
        expected_row = expected_rows[i]
        actual_row = rows[i]
        for j in range(len(expected_row)):
            compare_value(expected_row[j], actual_row[j])

    tear_down(postgresql_conn)


def compare_value(expected, actual):
    if isinstance(expected, bytes):
        if isinstance(actual, bytes):
            assert expected == actual
        else:
            assert int.from_bytes(expected, byteorder='big') == actual
    elif isinstance(expected, int) and isinstance(actual, date):
        # due to the limitation of the underlying MySQL JDBC driver, both year(2) and year(4) are
        # returned as the 'date' type; thus, we check the equality in this hacky way.
        assert expected % 100 == actual.year % 100
    else:
        assert expected == actual


def setup_sandbox():
    url = '127.0.0.1'
    dbname = 'test'
    port = 5432
    user = 'postgres'
    password = ''

    # create table and populate data
    postgresql_conn = postgresql_connect(url, dbname, port, user, password)
    cur = postgresql_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema + ' CASCADE;')
    cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema + ';')
    # cur.execute('CREATE TYPE enumType AS ENUM (\'test1\', \'test2\');')
    # cur.execute("""
    #     CREATE TYPE compositeType AS (
    #         i   INT,
    #         r   REAL,
    #         t   TEXT
    #     );"""
    # )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {}.{} (
          smallIntCol       SMALLINT,
          intCol            INTEGER,
          bigIntCol         BIGINT,
          realCol           REAL,
          doubleCol         DOUBLE PRECISION,
          serialCol         SERIAL,
          bigSerialCol      BIGSERIAL,
          varcharCol        VARCHAR(4),
          charCol           CHAR(4),
          textCol           TEXT,
          byteCol           BYTEA,
          timestampCol      TIMESTAMP,
          timestampZCol     TIMESTAMP WITH TIME ZONE,
          dateCol           DATE,
          timeCol           TIME,
          timeZCol          TIME WITH TIME ZONE,
          boolCol           BOOLEAN,
          bitCol            BIT(4),
          bitVarCol         BIT VARYING(8)
        );""".format(test_schema, test_table)
    )

    cur.execute("""
        INSERT INTO {}.{} VALUES (
            1, 1, 1, 1.5, 1.5, DEFAULT, DEFAULT,
            'abc', 'abc', 'hello world', '\\xDEADBEEF',
            '2018-12-31 12:59:59', '2018-12-31 12:59:59+08', '2018-12-31', '12:59:59', '12:59:59+08',
            TRUE, B'1010', B'10101' 
        );""".format(test_schema, test_table)
    )

    cur.execute("""
        INSERT INTO {}.{} VALUES (
            NULL, NULL, NULL, NULL, NULL, DEFAULT, DEFAULT,
            NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL
        );""".format(test_schema, test_table)
    )

    postgresql_conn.commit()
    cur.close()

    # create verdict connection
    thispath = os.path.dirname(os.path.realpath(__file__))
    postgresql_jar = os.path.join(thispath, 'lib', 'postgresql-42.2.5.jre7.jar')
    verdict_conn = verdict_connect(url, dbname, user, password, postgresql_jar)

    return (postgresql_conn, verdict_conn)


def tear_down(postgresql_conn):
    cur = postgresql_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema + ' CASCADE;')
    cur.close()
    postgresql_conn.close()


def verdict_connect(host, dbname, usr, pwd, class_path):
    connection_string = \
        'jdbc:postgresql://{:s}/{:s}?user={:s}&password={:s}'.format(host, dbname, usr, pwd)
    return pyverdict.VerdictContext(connection_string, class_path)


def postgresql_connect(host, dbname, port, usr, pwd):
    return psycopg2.connect(host=host, dbname=dbname, port=port, user=usr)

if __name__ == '__main__':
    test_data_types()