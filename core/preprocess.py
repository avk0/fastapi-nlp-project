""" Module that tokenizes descriptions and saves to database. """

import re
import sqlite3
import pandas as pd
import pymorphy2
from nltk.corpus import stopwords


IN_TABLE = 'hh_descriptions' 
OUT_TABLE = 'hh_tokens'


def tokenize(s):
    morph = pymorphy2.MorphAnalyzer()
    tokens = re.findall("[\/\-а-яёa-z]+", s.lower())
    filtered = [morph.parse(i)[0].normal_form for i in tokens if i not in stopwords.words("russian")]  # нормализация - лемматизация
    return filtered


def preprocess_data(con, in_table_name, out_table_name):
    print('Start tokenizing')

    cur = con.cursor()
    try:
        cur.execute(f"DROP TABLE {out_table_name}") # reset table
    except Exception as e:
        print(e)
    cur.execute(f"CREATE TABLE {out_table_name}(hhid, title, tokens, skills, url)")
    con.commit()
        
    df = pd.read_sql_query(f"SELECT * FROM {in_table_name}", con)
    print(df.shape)
    df_tokens = df.copy()
    df_tokens['tokens'] = df_tokens.apply(lambda row: ' '.join(tokenize(row['description'])), axis=1)   # 9min 15s
    print(df_tokens.shape)
    df_tokens.to_sql(out_table_name, con, if_exists='replace')


if __name__ == '__main__':
    DB_NAME = "/db/descriptions2.db"
    try:
        con = sqlite3.connect(os.getenv("DB_NAME"))
        preprocess_data(con, IN_TABLE, OUT_TABLE)
    except Exception as e:
        print(e)
    finally:
        con.close()

