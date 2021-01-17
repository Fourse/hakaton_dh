import pandas as pd

from pymongo import MongoClient


def dates_by_authors():
    client = MongoClient()
    db = client['hackaton']
    table = db['dates_by_authors']
    df = pd.read_csv('whole_table_with_lemm.csv', sep='\t')
    for index, row in df.iterrows():
        table.insert_one({'author': str(row['id']), 'date': str(row['dates'])})


def remove_eng():
    df = pd.read_csv('whole_table_with_lemm.csv', sep='\t')
    df = df.drop(['notes'], axis=1)
    df = df.rename(columns={"notes\n": "notes"})
    df['notes'] = df['notes'].str.replace(r"[^а-яА-Я ]", '')
    for index, row in df.iterrows():
        try:
            if row['notes'].strip() == '':
                df = df.drop(index)
                continue
        except Exception as e:
            print(f'Error: {e}\nAt index {index}')
    pd.DataFrame(df)
    df.to_csv('clean_table.csv', sep='\t')
    print(df)


def create_tables_by_age():
    client = MongoClient()
    db = client['hackaton_ages']
    df = pd.read_csv('clean_table.csv', sep='\t')
    for index, row in df.iterrows():
        year = int(str(row['dates']).split('/')[0])
        if 1 <= year < 1600:
            table = db['16']
        elif 1600 <= year < 1700:
            table = db['17']
        elif 1700 <= year < 1800:
            table = db['18']
        elif 1800 <= year < 1900:
            table = db['19']
        elif 1900 < year < 2000:
            table = db['20']
        elif 2000 < year < 2100:
            table = db['21']
        else:
            continue
        table.insert_one({'index': index, 'notes': row['notes'], 'date': row['dates'], 'id': row['id']})


def create_csv_by_age():
    df = pd.read_csv('clean_table.csv', sep='\t')
    df['dates'] = df['dates'].str.replace(r"/", '-')
    start = 1600
    end = 1700
    for age in range(17, 22):
        mask = (df['dates'] > f'{start}-0-0') & (df['dates'] <= f'{end}-0-0')
        new_df = df.loc[mask]
        new_df.to_csv(f'{age}_age.csv', sep='\t')

        start += 100
        end += 100

        del new_df


def count_words():
    df_list = [pd.read_csv(f'{age}_age.csv', sep='\t') for age in range(17, 22)]

    age = 17
    for df in df_list:
        words = set()
        doc = []
        for index, row in df.iterrows():
            try:
                doc.append(len(str(row['notes']).strip().split(' ')))
                for word in str(row['notes']).strip().split(' '):
                    words.add(word)
            except Exception as e:
                print(e, 'index:', index)

        print(f'Count words in  age: {age}, Count docs: {index}, Median len: {int(sum(doc) / len(doc))}')
        age += 1

        del words
        del doc


def create_csv_with_len_sentences():
    df_list = [pd.read_csv(f'{age}_age.csv', sep='\t') for age in range(17, 22)]

    age = 17
    for df in df_list:
        doc = []
        for index, row in df.iterrows():
            doc.append(len(str(row['notes']).strip().split(' ')))

        df['sentence_len'] = doc
        df.to_csv(f'{age}_age_with_len.csv', sep='\t')
        age += 1

        del doc


def analyze_by_age():
    for age in range(17, 22):
        df = pd.read_csv(f'{age}_age_with_len.csv', sep='\t')
        tmp = df.describe()
        print(f'Median in {age} age\n', tmp, '\n')


if __name__ == '__main__':
    dates_by_authors()    # смотрим на даты и количество записей по ним
    remove_eng()    # приводим датафрейм в порядок, удаляя записи не на русском языке
    create_tables_by_age()  # делаем базу, чтобы узнать количетсов записей по векам
    create_csv_by_age()  # делим очищенный .csv на отдельные, по векам
    count_words()   # считаем общее кол-во слов, кол-во записей и среднюю длину предложения
    create_csv_with_len_sentences()  # создаем .csv с отдельной колонкой с длиной предложения
    analyze_by_age()    # анализируем среднее значение, раасхождение, на кол-во слов
