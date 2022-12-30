import requests as req
from pandas import DataFrame, read_sql, Timestamp
import datetime as dt
import time
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from sqlalchemy import create_engine


RETRY_PERIOD = 60 * 60  # Таймаут между запросами
HEADERS = {
    'User-Agent': UserAgent().chrome
}
URL_LIST = [
    'https://tass.ru/rss/v2.xml',
    'https://www.vedomosti.ru/rss/news',
    'https://lenta.ru/rss/'
]
# Известные категории. Дополнение вручную из new_categories
POSSIBLE_CATEGORIES = {
    'Спорт':
    'Спорт',
    'Россия и СНГ':
    'Россия, Бывший СССР, Моя страна, Новости партнеров, Москва',
    'Экономика и бизнес':
    'Экономика и бизнес, Бизнес, Экономика, Финансы, Недвижимость, Авто',
    'Происшествия':
    'Происшествия',
    'Медиа':
    'Медиа	Культура, Интернет и СМИ',
    'Наука и техника':
    'Наука и техника, Космос, Технологии',
    'Общество':
    'Общество, Забота о себе, Среда обитания, Из жизни, Ценности',
    'Мир':
    'Мир, Международная панорама, Путешествия',
    'Политика':
    'Политика, Силовые структуры',
    'Прочее':
    'Прочее'
}
# Параметры подключения к БД
POSTGRES_USERNAME = 'docker'
POSTGRES_PASSWORD = 'docker'
POSTGRES_ADDRESS = 'db'
POSTGRES_PORT = '5432'  
POSTGRES_DBNAME = 'test'
engine = create_engine(
    'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'.format(
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        ipaddress=POSTGRES_ADDRESS,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DBNAME
    )
)

connection = engine.connect()
# Словарь для записи новых категорий
new_categories = []


def category_generalizer(category):
    # Определение категории. Если не нашли, то относим к категории Прочее
    for general, total in POSSIBLE_CATEGORIES.items():
        if category in total:
            return general
    new_categories.append(category)
    return POSSIBLE_CATEGORIES['Прочее']


def url_parse_news(url):
    # Получение данных
    response = req.get(url, headers=HEADERS)
    response.encoding = 'utf-8'
    # If response not OK, returns empty list
    if response.status_code != 200:
        return []
    soup = BeautifulSoup(response.text, 'xml')
    return soup.find_all('item')


def date_cutter(date):
    # Преобразование даты для загрузки в dataframe
    date_components = date.split(' ')
    sql_date_components = ' '.join(date_components[0:5])
    return Timestamp(
        dt.datetime.strptime(sql_date_components, "%a, %d %b %Y %H:%M:%S")
    )


def check_already_exist(article, df_articles, df_new_articles):
    # Проверка наличия в БД
    old_articles = df_articles.to_dict('records')
    new_articles = df_new_articles.to_dict('records')
    if article in old_articles or article in new_articles:
        return True
    return False


def main():
    """Collects data and saves to database"""
    timestamp = Timestamp(0)
    while True:
        # Зарузка из базы
        df_articles = read_sql('articles', connection)
        # Новые статьи
        df_new_articles = DataFrame(
            columns=["source", "title", "link", "pub_date", "category"]
        )
        for url in URL_LIST:
            for tag in url_parse_news(url):
                if date_cutter(tag.pubDate.text) > timestamp:
                    post = {
                        "source": url,
                        "title": tag.title.text,
                        "link": tag.link.text,
                        "pub_date": date_cutter(tag.pubDate.text),
                        "category": category_generalizer(tag.category.text)
                    }
                    if not check_already_exist(
                        post, df_articles,
                        df_new_articles
                    ):
                        df_new_articles = df_new_articles.append(
                            post,
                            ignore_index=True
                        )
        timestamp = Timestamp.now()
        df_new_articles.to_sql(
            "articles",
            connection,
            if_exists="append",
            index=False
        )
        time.sleep(RETRY_PERIOD)        


main()
