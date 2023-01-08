from datetime import datetime
from time import mktime
import feedparser
import psycopg2
import requests


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    return BaseHook.get_connection(conn_id)  

def load_dict(pg_conn, tablename, keyname, valname):
    db_dict = {}
    cursor = pg_conn.cursor()
    sql_string = 'select ' + keyname + ', ' + valname + ' from ' + tablename
    try:
        cursor.execute(sql_string)
        for row in cursor:
            db_dict[row[1]] = row[0]
    finally:
        cursor.close()
        return db_dict

def dowload_raw_data(**kwargs):
    url = kwargs['url']
    name = kwargs['name']

    last_update_sql = 'select max(item_date) from news where source_id = %s'
    add_source_sql = 'insert into sources(source_url, source_name) values(%s, %s) returning source_id'
    add_categ_sql = 'insert into categories(categ_name) values(%s) returning categ_id'

    conn = get_conn_credentials('news_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    categories = load_dict(pg_conn, 'categories', 'categ_id', 'categ_name')
    sources = load_dict(pg_conn, 'sources', 'source_id', 'source_url')

    source_id = sources.get(url)    

    try:
        cursor = pg_conn.cursor()
        # если такого новостного источника ещё нет - добавляем
        if not source_id:
            cursor.execute(add_source_sql, [url, name])
            source_id = cursor.fetchone()[0]

        # проверяем дату последней новости по данному источник
        cursor.execute(last_update_sql, [source_id])
        last_update = cursor.fetchone()[0]
        print(last_update)

        with requests.Session() as s:
            # получаем rss feed
            raw_data = s.get(url).text                              
            
            # парсим
            feed = feedparser.parse(raw_data)
            feed = feed.entries
            for entry in feed:  
                item_date = datetime.fromtimestamp(mktime(entry.published_parsed))
                
                # досрочно останавливаемся, если находим статью старее, чем последняя загруженная
                if last_update and last_update > item_date:
                    break

                # проверяем категорию и добавляем, если нет
                categ_name = entry.get('category')
                if categ_name:
                    categ_id = categories.get(categ_name)
                    if not categ_id:
                        cursor.execute(add_categ_sql, [categ_name])
                        categ_id = cursor.fetchone()[0]
                        categories[categ_name] = categ_id

                cursor.execute('insert into news (item_date, item_title, item_url, item_description, source_id, categ_id) values (%s, %s, %s, %s, %s, %s)',
                    (item_date, entry.get('title'), entry.get('link'), entry.get('description'), source_id, categ_id))

        pg_conn.commit()
        cursor.close()
    finally:            
        pg_conn.close()

def clear_data():
    # было обнаружено, что иногда в ленте дублируются item
    # для ускорения загрузки было решено не проверять каждый раз на наличие дублей
    # также было решено не делать guid новости на основании ссылки на неё - на всякий случай
    delete_duplicates_sql = 'delete from news n0 where exists (select * from news n where n.item_url = n0.item_url and n.item_date = n0.item_date and n.item_id < n0.item_id)'

    conn = get_conn_credentials('news_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    
    try:
        cursor = pg_conn.cursor()            
        cursor.execute(delete_duplicates_sql)
        pg_conn.commit()
        cursor.close()
    finally:            
        pg_conn.close()

def check_mart_exists():
    check_sql = '''
        select count(*)
        from
            (select replace(replace(replace(a.attname, 'src_', ''), '_count_24', ''), '_count', '') source_name
            from pg_attribute a
            left join pg_class t on a.attrelid = t.oid
            left join pg_namespace s on t.relnamespace = s.oid
            where a.attnum > 0 and not a.attisdropped 
            and t.relname = 'categories_summary_view' and s.nspname = 'public' 
            and a.attname like 'src_%') mv
        left join sources s on s.source_name = mv.source_name
        where s.source_id is null'''

    conn = get_conn_credentials('news_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    
    try:
        cursor = pg_conn.cursor()            
        cursor.execute(check_sql)        
        if cursor.fetchone()[0] > 0:
            return 'recreate_mart'
        else:
            return 'do_nothing'          
    except:
        return 'recreate_mart'            
    finally:  
        cursor.close()          
        pg_conn.close()


def recreate_mart():
    view_template_sql = '''
        drop  materialized view categories_summary_view;
        create materialized view categories_summary_view as(
        with counts as(
            select 
                coalesce(cg.categ_group_name, c.categ_name) categ_name, 
                count(*) total_count, 
                %s	  
                count(*) filter (where n.item_date > current_timestamp - interval '1 day') total_count_24,
                %s
                count(*) / count(distinct cast(n.item_date as date)) avg_count
            from news n
            left join categories c on n.categ_id = c.categ_id
            left join category_groups cg on c.categ_group_id = cg.categ_group_id
            group by coalesce(cg.categ_group_name, c.categ_name)),
            
        max_day as(
            -- возвращаем максимальную дату, если есть несколько дат с одинаковым максимальным количеством новостей
            select categ_name, max(item_date) max_date
            from
                (select md.*, max(total_count) over(partition by categ_name) max_total_count
                from
                    (select 
                        coalesce(cg.categ_group_name, c.categ_name) categ_name, 
                        cast(n.item_date as date) item_date,
                        count(*) total_count
                    from news n
                    left join categories c on n.categ_id = c.categ_id
                    left join category_groups cg on c.categ_group_id = cg.categ_group_id
                    group by coalesce(cg.categ_group_name, c.categ_name), cast(n.item_date as date)) md) md
                where total_count = max_total_count
            group by categ_name),
            
        week_days as(
            select 
                coalesce(cg.categ_group_name, c.categ_name) categ_name, 	  	
                count(*) filter (where extract(isodow from n.item_date) = 1) mon_count,
                count(*) filter (where extract(isodow from n.item_date) = 2) tue_count,
                count(*) filter (where extract(isodow from n.item_date) = 3) wed_count,
                count(*) filter (where extract(isodow from n.item_date) = 4) thu_count,
                count(*) filter (where extract(isodow from n.item_date) = 5) fri_count,
                count(*) filter (where extract(isodow from n.item_date) = 6) sat_count,
                count(*) filter (where extract(isodow from n.item_date) = 7) sun_count
            from news n
            left join categories c on n.categ_id = c.categ_id
            left join category_groups cg on c.categ_group_id = cg.categ_group_id
            group by coalesce(cg.categ_group_name, c.categ_name))
            
        select c.*, md.max_date, wd.mon_count, wd.tue_count, wd.wed_count, wd.thu_count, wd.fri_count, wd.sat_count, wd.sun_count
        from counts c
        left join max_day md on c.categ_name = md.categ_name
        left join week_days wd on c.categ_name = wd.categ_name)'''

    conn = get_conn_credentials('news_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    try:
        sources = load_dict(pg_conn, 'sources', 'source_id', 'source_name')        
        count_list = []
        count_24_list = []
        for source_name, source_id in sources.items():            
            count_list.append(f'  	count(*) filter (where n.source_id = {source_id}) src_{source_name}_count,')
            count_24_list.append(f"  	count(*) filter (where n.source_id = {source_id} and n.item_date > current_timestamp - interval '1 day') src_{source_name}_count_24,")
        cursor = pg_conn.cursor()  

        view_template_sql = view_template_sql % ('\n'.join(count_list), '\n'.join(count_24_list))                 
        cursor.execute(view_template_sql)        
        pg_conn.commit()
    finally:            
        pg_conn.close()  	

def do_nothing():
    pass

def refresh_mart():
    refresh_mart_sql = 'refresh materialized view categories_summary_view'

    conn = get_conn_credentials('news_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    try:        
        cursor = pg_conn.cursor()  
        cursor.execute(refresh_mart_sql)        
        pg_conn.commit()
    finally:            
        pg_conn.close()  

def process_data():
    pass


with DAG(dag_id="rss_process", start_date=datetime(2023, 1, 1), catchup=False, schedule_interval="*/5 * * * *") as dag:

    # получаем список ссылок
    sources = Variable.get("sources")

    source_list = []
    for url in sources.split():  
        source_list.append([url.replace('https://', '').replace('www.', '').split('.')[0], url])

    # отдельные операторы для загрузки из каждого источника
    download_op = []    
        
    for name, url in source_list:
        download_op.append(PythonOperator(task_id=f"dowload_raw_data_{name}", python_callable=dowload_raw_data, op_kwargs={'url': url, 'name': name}, do_xcom_push=False))

    # первичная очистка данных
    clear_op = PythonOperator(task_id="clear_and_prepare", python_callable=clear_data, do_xcom_push=False)

    # проверка актуальности витрины
    check_mart_exists_op = BranchPythonOperator(task_id='check_mart_exists', python_callable=check_mart_exists, do_xcom_push=False)
    
    # если появились новые источники новостей, то пересоздаём витрину
    recreate_mart_op = PythonOperator(task_id="recreate_mart", python_callable=recreate_mart, do_xcom_push=False)
    do_nothing_op = PythonOperator(task_id="do_nothing", python_callable=do_nothing, do_xcom_push=False)

    # обновление витрины
    refresh_mart_op = PythonOperator(task_id="refresh_mart", python_callable=refresh_mart, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, do_xcom_push=False)
        

    download_op >> clear_op >> check_mart_exists_op >> [recreate_mart_op, do_nothing_op] >> refresh_mart_op
