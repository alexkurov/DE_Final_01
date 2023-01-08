## Анализ публикуемых новостей

### Быстрый старт

  ```sh
  git clone https://github.com/alexkurov/DE_Final_01
  ```
  ```sh
  docker-compose up
  ```
  
### Настройка источников новостей

  При инициалиазции контейнера по-умолчанию добавлен только один источник новостей https://lenta.ru/rss/news
  Для добавления дополнительных источников необходимо дополнить переменную `Airflow` `sources`. 
  Ссылку на новый источник необходимо писать с новой строки

### Доступ к витрине данных

  Витрина реализована посредством PostgresSQL Materialized View
  Можно использовать контейнер pgAdmin http://localhost:5050/
    `hostname`: `db`
    `port`    : `5432`
    `username`: `admin`
    `password`: `admin`
    
  Для запроса к витрине:
    `select * from categories_summary_view`
