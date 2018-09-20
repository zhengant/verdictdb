from datetime import datetime, date
import os
import pyverdict
import psycopg2


test_schema = 'pyverdict_datatype_test_schema'
test_table = 'pyverdict_datatype_test_table'


def test_data_types():
    (mysql_conn, verdict_conn) = setup_sandbox()

    result = verdict_conn.sql('select * from {}.{}'.format(test_schema, test_table))
    int_types = result.typeJavaInt()
    types = result.types()
    rows = result.rows()
    # print(int_types)
    # print(types)
    print(rows)
    # print([type(x) for x in rows[0]])

    cur = mysql_conn.cursor()
    cur.execute('select * from {}.{}'.format(test_schema, test_table))
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

    tear_down(mysql_conn)


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
    url = 'localhost'
    dbname = 'test'
    user = 'root'
    password = ''

    # create table and populate data
    postgresql_conn = postgresql_connect(url, dbname, user, password)
    cur = postgresql_conn.cursor()
    # cur.execute('DROP SCHEMA IF EXISTS ' + test_schema)
    # cur.execute('CREATE SCHEMA IF NOT EXISTS ' + test_schema)
    cur.execute('CREATE TYPE enumType AS ENUM (\'test1\', \'test2\')')
    cur.execute("""
        CREATE TYPE compositeType AS (
            i   INT
            r   REAL
            t   TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS {} (
          smallIntCol       SMALLINT,
          intCol            INTEGER,
          bigIntCol         BIGINT,
          decimalCol        DECIMAL,
          numericCol        NUMERIC,
          realCol           REAL,
          doubleCol         DOUBLE PRECISION,
          smallSerialCol    SMALLSERIAL,
          serialCol         SERIAL,
          bigSerialCol      BIGSERIAL,
          moneyCol          MONEY,
          varcharCol        VARCHAR(4),
          charCol           CHAR(4),
          textCol           TEXT,
          byteCol           BYTEA,
          timestampCol      TIMESTAMP,
          timestampZCol     TIMESTAMP WITH TIME ZONE,
          dateCol           DATE,
          timeCol           TIME,
          timeZCol          TIME WITH TIME ZONE,
          intervalCol       INTERVAL,
          boolCol           BOOLEAN,
          enumCol           enumType,
          pointCol          POINT,
          lineCol           LINE,
          lsegCol           LSEG,
          boxCol            BOX,
          pathCol           PATH,
          polygonCol        POLYGON,
          circleCol         CIRCLE,
          cidrCol           CIDR,
          inetCol           INET,
          macaddrCol        MACADDR,
          bitCol            BIT(4),
          bitVarCol         BIT VARYING(4),
          uuidCol           UUID,
          xmlCol            XML,
          jsonCol           JSON,
          jsonbCol          JSONB,
          arrayCol          TEXT [],
          
        );""".format(test_table)
        )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
          1, 'abcd', 'abcd', 'abcde', 1, 2, DEFAULT, 0.5, 0.05, 
        )""".format(test_schema, test_table)
        )
    cur.execute("""
        INSERT INTO {}.{} VALUES (
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )""".format(test_schema, test_table)
        )
    cur.close()

    # create verdict connection
    thispath = os.path.dirname(os.path.realpath(__file__))
    mysql_jar = os.path.join(thispath, 'lib', 'mysql-connector-java-5.1.46.jar')
    verdict_conn = verdict_connect(url, dbname, user, password, mysql_jar)

    return (postgresql_conn, verdict_conn)


def tear_down(mysql_conn):
    cur = mysql_conn.cursor()
    cur.execute('DROP SCHEMA IF EXISTS ' + test_schema)
    cur.close()
    mysql_conn.close()


def verdict_connect(host, port, usr, pwd, class_path):
    connection_string = \
        'jdbc:mysql://{:s}:{:d}?user={:s}&password={:s}'.format(host, port, usr, pwd)
    return pyverdict.VerdictContext(connection_string, class_path)


def postgresql_connect(host, dbname, usr, pwd):
    return psycopg2.connect(host=host, dbname=dbname, user=usr, password=pwd)
