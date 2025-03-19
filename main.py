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


def load_json_data(config_file):
    try:
        with open(config_file, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Файл {config_file} не найден.")
    except json.JSONDecodeError:
        raise ValueError(f"Ошибка при чтении JSON из файла {config_file}.")


class DatabaseConnection:
    def __init__(self, config_file='config.json'):
        with open(config_file, 'r') as f:
            config = json.load(f)

        self.config_file = config.get('config_file')
        self.name_base = config.get('name_base')
        self.user = config.get('user')
        self.password = config.get('password')
        self.host = config.get('host')
        self.port = config.get('port')
        self.conn = None

    def get_config_file(self):
        return self.config_file

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.name_base,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return True
        except psycopg2.Error as e:
            print(f"Ошибка подключения: {e}")
            return False

    def close(self):
        if self.conn:
            self.conn.close()

    def get_connection(self):
        return self.conn

    def get_name_base(self):
        return self.name_base


def test_conn_DB():
    db_connection = DatabaseConnection()
    print("test_conn_DB")
    print("Проверка соединения с базой")
    flag = 0

    if db_connection.connect():
        flag = 1
        assert True
    else:
        assert False

    if flag:
        db_connection.close()
        print()


def test_tables_DB():
    print("test_tables_DB")
    db_connection = DatabaseConnection()
    print("Проверка на работоспособность запроса и нахождение всех таблиц в базе.")
    flag = 0
    try:
        if db_connection.connect():
            flag = 1
            cursor = db_connection.get_connection().cursor()
            cursor.execute("""SELECT table_name FROM information_schema.tables
                           WHERE table_schema = 'public'""")
            assert True
        else:
            assert False

        assert True
    except psycopg2.Error as e:
        assert False

    finally:
        if flag:
            db_connection.close()
            print()


@pytest.fixture(params=["testtable", "testtable1"])
def test_fields_table_DB(request):
    table_name = request.param  # Получаем параметр из фикстуры
    print(f"test_fields_table_DB для таблицы: {table_name}")
    print("Проверка на получение всех полей из таблицы")
    db_connection = DatabaseConnection()
    flag = 0
    try:
        if db_connection.connect():
            flag = 1
            cursor = db_connection.get_connection().cursor()
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
            cursor.close()
        else:
            print("Не удалось подключиться к базе данных.")
            assert False

    except psycopg2.Error as e:
        print(f"Ошибка при подключении или выполнении запроса: {e}")
        assert False

    finally:
        if flag:
            db_connection.close()
            print()


def test_db_fields(test_fields_table_DB):
    assert True


def test_testdb_tables_JSON_DB():
    print("test_testdb_tables_JSON_DB")
    print("Проверка на соответсвие таблиц JSON и базы")
    flag = 0
    db_connection = DatabaseConnection()
    try:
        if db_connection.connect():
            flag = 1
            cursor = db_connection.get_connection().cursor()
            cursor.execute("""SELECT table_name FROM information_schema.tables
                                   WHERE table_schema = 'public'""")
            data = load_json_data(db_connection.get_config_file())

            list_tables_json = list(data[db_connection.get_name_base()].keys())

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
            cursor.close()
        else:
            print("Не удалось подключиться к базе данных.")
            assert False

    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"
    except FileNotFoundError:
        assert False, "Файл data.json не найден"
    except json.JSONDecodeError:
        assert False, "Ошибка при чтении JSON из файла data.json"

    finally:
        if flag:
            db_connection.close()
            print()


@pytest.fixture(params=["testtable", "testtable1"])
def test_fields_table_JSON_DB(request):
    name_table = request.param
    print("test_fields_table_JSON_DB")
    print("Проверка на соответсвие полей")
    flag = 0
    db_connection = DatabaseConnection()
    try:
        if db_connection.connect():
            flag = 1
            cursor = db_connection.get_connection().cursor()
            data = load_json_data(db_connection.get_config_file())
            name_base = list(data.keys())[0]

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
        else:
            print("Не удалось подключиться к базе данных.")
            assert False

    except psycopg2.Error as e:
        assert False, f"Ошибка при подключении или выполнении запроса: {e}"
    except FileNotFoundError:
        assert False, "Файл data.json не найден"
    except json.JSONDecodeError:
        assert False, "Ошибка при чтении JSON из файла data.json"

    finally:
        if flag:
            db_connection.close()
            print()


def test_db_fields1(test_fields_table_JSON_DB):
    assert True


if __name__ == '__main__':
    db_to_json()
    test_testdb_tables_JSON_DB()
