import pandas as pd
import numpy as np
import requests
import os
import Base


# settings
source_url = "https://randomuser.me/api/?results=4500"
path = 'C:/Users/einav/Desktop/Datasets/'
first = 'first.json'
second = 'second.json'

# Connection Settings
connection = Base.connection

def download_snapshot(src_url):
    """
    Download a new snapshot from the source
    :param src_url:
        The API
    :return:
        JSON object
    """
    response = requests.get(src_url, verify=False)
    json = response.json()
    return json


def table_columns(df):
    """
    :param df:
    :return:
        Get a df and return a list of columns
        Replace dots
    """
    cols = "`,`".join([str(i.replace(".", "")) for i in df.columns.tolist()])
    return cols


def drop_table(name):
    for n in name:
        sql_stmt = "DROP TABLE IF EXISTS manage.galmizrahi_test_{};".format(n)

        try:
            with connection.cursor() as cur:
                cur.execute(sql_stmt)
        except NameError:
            print("Something went wrong")
        else:
            print('Table `galmizrahi_test_{}` dropped successfully'.format(n))


def create_table(name):
    """
    :param name:
        the table's suffix
    :return:
        Create table statement
    """
    for n in name:
        sql_stmt = "CREATE TABLE IF NOT EXISTS `galmizrahi_test_{}` ( \
    `gender` varchar(50) DEFAULT NULL,\
    `email` varchar(50) DEFAULT NULL,\
    `phone` varchar(50) DEFAULT NULL,\
    `cell` varchar(50) DEFAULT NULL,\
    `nat` varchar(50) DEFAULT NULL,\
    `nametitle` varchar(50) DEFAULT NULL,\
    `namefirst` varchar(50) DEFAULT NULL,\
    `namelast` varchar(50) DEFAULT NULL,\
    `locationstreetnumber` varchar(50) DEFAULT NULL,\
    `locationstreetname` varchar(50) DEFAULT NULL,\
    `locationcity` varchar(50) DEFAULT NULL,\
    `locationstate` varchar(50) DEFAULT NULL,\
    `locationcountry` varchar(50) DEFAULT NULL,\
    `locationpostcode` varchar(50) DEFAULT NULL,\
    `locationcoordinateslatitude` varchar(50) DEFAULT NULL,\
    `locationcoordinateslongitude` varchar(50) DEFAULT NULL,\
    `locationtimezoneoffset` varchar(50) DEFAULT NULL,\
    `locationtimezonedescription` varchar(50) DEFAULT NULL,\
    `loginuuid` varchar(50) DEFAULT NULL,\
    `loginusername` varchar(50) DEFAULT NULL,\
    `loginpassword` varchar(50) DEFAULT NULL,\
    `loginsalt` varchar(50) DEFAULT NULL,\
    `loginmd5` varchar(250) DEFAULT NULL,\
    `loginsha1` varchar(250) DEFAULT NULL,\
    `loginsha256` varchar(250) DEFAULT NULL,\
    `dobdate` varchar(50) DEFAULT NULL,\
    `dobage` varchar(50) DEFAULT NULL,\
    `registereddate` varchar(50) DEFAULT NULL,\
    `registeredage` varchar(50) DEFAULT NULL,\
    `idname` varchar(50) DEFAULT NULL,\
    `idvalue` varchar(50) DEFAULT NULL,\
    `picturelarge` varchar(250) DEFAULT NULL,\
    `picturemedium` varchar(250) DEFAULT NULL,\
    `picturethumbnail` varchar(250) DEFAULT NULL\
    ) ".format(n)

        try:
            with connection.cursor() as cur:
                cur.execute(sql_stmt)
        except NameError:
            print("Something went wrong")
        else:
            print('Table `galmizrahi_test_{}` created successfully'.format(n))


def insert_table(dic):
    try:
        with connection.cursor() as cur:
            table_col = cols
            for n, d in dic.items():
                for i, row in d.iterrows():
                    sql = "INSERT INTO `galmizrahi_test_" + n + "`(`" + table_col + "`) VALUES (" + "%s," * (
                            len(row) - 1) + "%s)"
                    cur.execute(sql, tuple(row))
                    # connection.commit()
    except NameError:
        print("Something went wrong")
    else:
        print('Insert clause was executed successfully')


def top20registers():
    sqlstmt = """create table if not exists  galmizrahi_test_20 as
with male as(
SELECT   
	*
, 	row_number() over (partition by loginusername order by registereddate) as RN
FROM manage.galmizrahi_test_male
),

female as(
SELECT   
	*
, 	row_number() over (partition by loginusername order by registereddate) as RN
FROM manage.galmizrahi_test_female)
,
top20male as 
(
select * 
from male
where RN = 1
order by registereddate desc
limit 20),

top20female as 
(
select * 
from female
where RN = 1
order by registereddate desc
limit 20
)

select * from top20male
union all
select * from top20female;"""
    try:
        with connection.cursor() as cur:
            cur.execute(sqlstmt)
    except NameError:
        print("Something went wrong")
    else:
        print('top20registers was created successfully')


def Gettable(name):
    """
    :return:
        Select * from table and return a df
    """
    sql_stmt = """SELECT 
	`gender`,
    `email`,
    `phone`,
    `cell`,
    `nat`,
    `nametitle`,
    `namefirst`,
    `namelast`,
    `locationstreetnumber`,
    `locationstreetname`,
    `locationcity`,
    `locationstate`,
    `locationcountry`,
    `locationpostcode`,
    `locationcoordinateslatitude`,
    `locationcoordinateslongitude`,
    `locationtimezoneoffset`,
    `locationtimezonedescription`,
    `loginuuid`,
    `loginusername`,
    `loginpassword`,
    `loginsalt`,
    `loginmd5`,
    `loginsha1`,
    `loginsha256`,
    `dobdate`,
    `dobage`,
    `registereddate`,
    `registeredage`,
    `idname`,
    `idvalue`,
    `picturelarge`,
    `picturemedium`,
    `picturethumbnail`
FROM `manage`.`galmizrahi_test_{}`;""".format(name)
    try:
        with connection.cursor() as cur:
            cur.execute(sql_stmt)
            tbl = pd.DataFrame(cur.fetchall())
            tbl.columns = main_df.columns

    except NameError:
        print("Something went wrong")
    else:
        print('galmizrahi_test_{} table was fetched successfully'.format(name))
    return tbl


def save_new_json_file(df, directory, filename):
    """
    :return: create new json file if it does not exist
    """
    fullpath = os.path.join(directory, filename)
    if os.path.exists(fullpath):
        print(f'{filename} is already exists')
    else:
        df.to_json(fullpath)
        print(f'{filename} has been created')


# Q1
data = download_snapshot(source_url)

# Q2
main_df = pd.json_normalize(data['results'])
cols = table_columns(main_df)
male = main_df[main_df['gender'] == 'male']
female = main_df[main_df['gender'] == 'female']
df_dic = {'male': male, 'female': female}
drop_table(['male', 'female'])
create_table(['male', 'female'])
insert_table(df_dic)

# Q3
binslbl = np.arange(1, 11)
bins = np.arange(0, 110, 10)
df_bins = main_df.copy()
"""Generate a new flag column indicating the row group"""
df_bins['bins'] = pd.cut(x=df_bins['dob.age'], bins=bins, labels=binslbl)
"""Creating a dictionary for any bin group"""
bins_dic = {str(i): df_bins[df_bins['bins'] == i].loc[:, df_bins.columns != 'bins'] for i in binslbl}

# Drop tables if exists
drop_table(bins_dic.keys())
create_table(bins_dic.keys())
insert_table(bins_dic)

# Q4 + Q5
drop_table(['20'])
top20registers()

# Q6
df_top20 = Gettable('20')
df_5 = pd.DataFrame(bins_dic.get('5'))
df_q6 = pd.concat([df_top20, df_5]).drop_duplicates().reset_index(drop=True)
save_new_json_file(df_q6, path, first)

# Q7
df_2 = Gettable('3')
df_q7 = pd.concat([df_top20, df_2], axis=0).reset_index(drop=True)  # pd.merge(left=df_top20, right=df_2)
save_new_json_file(df_q7, path, second)

#Close Connection
connection.close()
print('connection closed')
