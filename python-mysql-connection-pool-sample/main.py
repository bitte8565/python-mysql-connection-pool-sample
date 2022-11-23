from concurrent import futures
from pprint import pprint

import sqlalchemy

import my_logger

user = 'root'
password = 'root'
host = 'localhost'
port = '3306'
database = 'sample'

logger = my_logger.get_logger(__name__)

engine = sqlalchemy.create_engine(
    f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=10, max_overflow=10)


def query_to_dict(data):
    """
    select結果を辞書型に変換する
    https://stackoverflow.com/questions/20743806/sqlalchemy-execute-return-resultproxy-as-tuple-not-dict
    :param data: select結果
    :return: 辞書型のselect結果
    """
    if data is not None:
        return [{key: value for key, value in row._mapping.items()} for row in data if row is not None]
    else:
        return [{}]


def exec_query(sql):
    """
    クエリ実行
    :param sql: SQL
    :return: 辞書型クエリ結果
    """
    with engine.connect() as conn:
        data = conn.execute(sql).fetchall()
        result = query_to_dict(data)
        return result


def main_func():
    try:
        sql = 'select * from world.city limit 3;'
        city_list = exec_query(sql)
        for city in city_list:
            print(city['Name'])
    except Exception as e:
        logger.error(e)


def main():
    future_list = []
    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(20):
            future = executor.submit(main_func)
            future_list.append(future)
            _ = futures.as_completed(fs=future_list)


if __name__ == '__main__':
    main()
