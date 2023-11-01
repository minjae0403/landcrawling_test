import pandas as pd
import pymysql
from sqlalchemy import create_engine, Column
from sqlalchemy.types import Float, String, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect

file_path_2022 = "./house_data/서울특별시_전월세가_2022.csv"
file_path_2021 = "./house_data/서울특별시_전월세가_2021.csv"
file_path_2020 = "./house_data/서울특별시_전월세가_2020.csv"
file_path_2019 = "./house_data/서울특별시_전월세가_2019.csv"
file_path_2018 = "./house_data/서울특별시_전월세가_2018.csv"

file_path = [file_path_2018 ,file_path_2019 ,file_path_2020 ,file_path_2021 ,file_path_2022]

# MySQL 접속 정보
db_host = 'mysqltest.c5aznfazxhi5.ap-northeast-2.rds.amazonaws.com'
db_user = 'admin'
db_password = 'qwer1234'
db_database = 'house_data'
table_name = 'seoul_house_data'

# MySQL 데이터베이스에 연결
db_engine = create_engine(f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_database}')

# table 형태 미리 정의
Base = declarative_base()
class User(Base):
    __tablename__ = table_name
    unique_id = Column(Integer, primary_key=True, index=True)
    regist_date = Column(String(255), index=True)
    location_code = Column(Float, index=True)
    location_name = Column(String(255), index=True)
    legal_dong_code = Column(Float, index=True)
    legal_dong_name = Column(String(255), index=True)
    lot_number_code = Column(Float, index=True)
    lot_number_id = Column(String(255), index=True)
    lot_number_id_1 = Column(String(255), index=True)
    lot_number_id_2 = Column(String(255), index=True)
    floor = Column(String(255), index=True)
    contract_date = Column(Date, index=True)
    rent_class = Column(String(255), index=True)
    size = Column(String(255), index=True)
    deposit = Column(Float, index=True)
    rent_price = Column(Float, index=True)
    building_name = Column(String(255), index=True)
    build_year = Column(String(255), index=True)
    buidling_use = Column(String(255), index=True)
    contract_term = Column(String(255), index=True)
    contract_class = Column(String(255), index=True)
    contreact_class_2 = Column(String(255), index=True)
    before_deposite = Column(Float, index=True)
    before_price = Column(Float, index=True)

def db_table():
    if not inspect(db_engine).has_table(User.__tablename__):
        Base.metadata.create_all(db_engine)

#csv to map
def to_map(csv_file_path):
    data_list = []
    try:
        csvfile = pd.read_csv(csv_file_path, encoding="cp949")
        csvfile = csvfile.fillna(0)
        data_list = csvfile.to_numpy().tolist()

        return data_list
    except Exception as e:
        print(e)

def import_data(data_list):
    
    connection = pymysql.connect(host = db_host, user = db_user, password = db_password, db = db_database)
    cursor = connection.cursor()

    query =f"""
        INSERT INTO {table_name} (`regist_date`,
        `location_code`,
        `location_name`,
        `legal_dong_code`,
        `legal_dong_name`,
        `lot_number_code`,
        `lot_number_id`,
        `lot_number_id_1`,
        `lot_number_id_2`,
        `floor`,
        `contract_date`,
        `rent_class`,
        `size`,
        `deposit`,
        `rent_price`,
        `building_name`,
        `build_year`,
        `buidling_use`,
        `contract_term`,
        `contract_class`,
        `contreact_class_2`,
        `before_deposite`,
        `before_price`
        )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # sql 쿼리 적용
    cursor.executemany(query, data_list)

    # sql 쿼리 실행
    connection.commit()

    # DB연결 종료
    connection.close()

    print("Finish Import.")

db_table()
for csv_file in file_path:
    data_list = to_map(csv_file)
    import_data(data_list)