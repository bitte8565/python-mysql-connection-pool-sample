from pprint import pprint

import sqlalchemy


user = 'root'
password = 'root'
host = 'localhost'
port = '3306'
database = 'sample'


def main():
    engine = sqlalchemy.create_engine(
        f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', pool_size=10, max_overflow=10)

    for i in range(20):
        conn = engine.connect()
        data = conn.execute('select 1 as test;').fetchall()
        result = query_to_dict(data)
        pprint(result)


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


if __name__ == '__main__':
    main()
