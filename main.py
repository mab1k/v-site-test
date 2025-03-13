# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import psycopg2
import json
import pytest
import tabulate


def contains_all(list1, list2):
    return all(item in list1 for item in list2)


def get_elements_only_in_list1(list1, list2):

    return [item for item in list1 if item not in list2]


def db_to_json():
    test = {
        "testdb": {
            "testtable": ["id", "name"],
            "testtable1": ["id", "name1"]
        }
    }
    with open('data.txt', 'w') as outfile:
        json.dump(test, outfile, indent=4)


def test_conn_DB():
    global conn
    print("connDB")
    print("Проверка соединения с базой")
    flag = 0
    try:
        conn = psycopg2.connect(dbname="testdb", user="postgres",
                                password="postgres",
                                host="localhost", port='5433')
        flag = 1
        assert True

    except psycopg2.Error as e:
        assert False

    finally:
        if flag:
            conn.close()
            print()


def test_tables_DB():
    print("test_tables_DB")
    print("Проверка на работоспособность запроса и нахождение всех таблиц в базе.")
    global conn
    flag = 0
    try:
        conn = psycopg2.connect(dbname="testdb", user="postgres",
                                password="postgres",
                                host="localhost", port='5433')
        cursor = conn.cursor()
        flag = 1
        cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")

        assert True
    except psycopg2.Error as e:
        assert False

    finally:
        if flag:
            conn.close()
            print()


def test_fields_testtable_DB():
    print("test_fields_testtable_DB")
    print("Проверка на получение всех полей из таблицы")
    global conn
    flag = 0
    try:
        conn = psycopg2.connect(dbname="testdb", user="postgres",
                                password="postgres",
                                host="localhost", port='5433')
        cursor = conn.cursor()
        flag = 1
        cursor.execute("SELECT * FROM testtable LIMIT 0")

        assert True

    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"

    finally:
        if flag:
            conn.close()
            print()


def test_fields_testtable1_DB():
    print("test_fields_testtable1_DB")
    print("Проверка на получение всех полей из таблицы")
    global conn
    flag = 0
    try:
        conn = psycopg2.connect(dbname="testdb", user="postgres",
                                password="postgres",
                                host="localhost", port='5433')

        cursor = conn.cursor()
        flag = 1
        cursor.execute("SELECT * FROM testtable1 LIMIT 0")

        assert True
    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"

    finally:
        if flag:
            conn.close()
            print()


def test_testdb_tables_JSON_DB():
    global conn
    print("test_testdb_tables_JSON_DB")
    print("Проверка на соответсвие таблиц JSON и базы")
    flag = 0
    try:
        with open('data.json', 'r') as file:
            data = json.load(file)
        name_base = list(data.keys())[0]

        conn = psycopg2.connect(dbname=name_base, user="postgres",
                                password="postgres",
                                host="localhost", port='5433')

        flag = 1
        cursor = conn.cursor()
        cursor.execute("""SELECT table_name FROM information_schema.tables
                       WHERE table_schema = 'public'""")

        list_tables_json = list(data[name_base].keys())

        list_tables_DB = [table[0] for table in cursor.fetchall()]

        if len(list_tables_json) == len(list_tables_DB) and (sorted(list_tables_DB) == sorted(list_tables_json)):
            assert True
        elif len(list_tables_json) > len(list_tables_DB):
            print("В JSON таблиц больше чем в базе данных.")
            print("Эти таблицы содержаться в json, но не содержаться в базе данных")

            data = [
                ['id', 'name'],
            ]

            i = 0

            for i in range(0, len(get_elements_only_in_list1(list_tables_json, list_tables_DB))):
                data.append([i, get_elements_only_in_list1(list_tables_json, list_tables_DB)[i]])
                i += 1

            print(tabulate.tabulate(data))
            assert False
        else:
            if contains_all(list_tables_DB, list_tables_json):
                print("В JSON таблиц меньше чем в базе данных.")
                print("Название таблиц не входящих в JSON.")

                data = [
                    ['id', 'name'],
                ]

                i = 0

                for i in range(0, len(get_elements_only_in_list1(list_tables_DB, list_tables_json))):
                    data.append([i, get_elements_only_in_list1(list_tables_DB, list_tables_json)[i]])
                    i += 1

                print(tabulate.tabulate(data))

                assert True
            else:
                print("Таблицы в JSON и базе данных не пересекаются.")
                assert False

    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"
    except FileNotFoundError:
        assert False, "Файл data.json не найден"
    except json.JSONDecodeError:
        assert False, "Ошибка при чтении JSON из файла data.json"

    finally:
        if flag:
            conn.close()
            print()


def test_fields_testtable_JSON_DB():
    global conn
    print("test_fields_testtable_JSON_DB")
    print("Проверка на соответсвие полей")
    flag = 0
    try:
        with open('data.json', 'r') as file:
            data = json.load(file)
        name_base = list(data.keys())[0]
        name_table = list(data[name_base].keys())[0]

        conn = psycopg2.connect(dbname=name_base, user="postgres",
                                password="postgres",
                                host="localhost", port='5433')

        flag = 1
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {name_table} LIMIT 0")
        coll_names = [desc[0] for desc in cursor.description]

        fields = data[name_base][name_table]

        fields_sort = sorted(fields)
        coll_names_sorted = sorted(coll_names)

        if len(fields) == len(coll_names):
            if fields_sort == coll_names_sorted:
                print(f"Все поля в таблице {name_table} совпадают.")
                assert True
            else:
                print("Обнаружены несовпадения:")
                for i in range(len(max(fields_sort, coll_names_sorted))):
                    if fields_sort[i] != coll_names_sorted[i]:
                        print(f"Имя базы: {name_base}")
                        print(f"    Имя таблицы: {name_table}")
                        print(f"        Индекс {i}: JSON: {fields_sort[i]}, DB = {coll_names_sorted[i]}")
                assert False
        else:
            print("Количество полей в базе и в JSON не совпадают")
            assert False

    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"
    except FileNotFoundError:
        assert False, "Файл data.json не найден"
    except json.JSONDecodeError:
        assert False, "Ошибка при чтении JSON из файла data.json"

    finally:
        if flag:
            conn.close()
            print()


def test_fields_testtable1_JSON_DB():
    global conn
    print("test_fields_testtable1_JSON_DB")
    print("Проверка на соответсвие полей")
    flag = 0
    try:
        with open('data.json', 'r') as file:
            data = json.load(file)
        name_base = list(data.keys())[0]
        name_table = list(data[name_base].keys())[1]

        conn = psycopg2.connect(dbname=name_base, user="postgres",
                                password="postgres",
                                host="localhost", port='5433')

        flag = 1
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {name_table} LIMIT 0")
        coll_names = [desc[0] for desc in cursor.description]

        fields = data[name_base][name_table]

        fields_sort = sorted(fields)
        coll_names_sorted = sorted(coll_names)

        if len(fields) == len(coll_names):
            if fields_sort == coll_names_sorted:
                print(f"Все поля в таблице {name_table} совпадают.")
                assert True
            else:
                print("Обнаружены несовпадения:")
                for i in range(len(max(fields_sort, coll_names_sorted))):
                    if fields_sort[i] != coll_names_sorted[i]:
                        print(f"Имя базы: {name_base}")
                        print(f"    Имя таблицы: {name_table}")
                        print(f"        Индекс {i}: JSON: {fields_sort[i]}, DB = {coll_names_sorted[i]}")
                assert False
        else:
            print("Количество полей в базе и в JSON не совпадают")
            assert False

    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"
    except FileNotFoundError:
        assert False, "Файл data.json не найден"
    except json.JSONDecodeError:
        assert False, "Ошибка при чтении JSON из файла data.json"

    finally:
        if flag:
            conn.close()
            print()


if __name__ == '__main__':
    db_to_json()
    test_testdb_tables_JSON_DB()
    test_fields_testtable1_JSON_DB()
