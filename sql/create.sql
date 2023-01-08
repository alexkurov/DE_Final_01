-- Пункты задания "Разработать скрипты загрузки данных в 2-х режимах: Инициализирующий, Инкрементальный" и "организовать Сырой слой данных" противоречат друг другу в текущих условиях.
-- В rss нет возможности отфильтровать выдачу по периоду. Следовательно, мы всегда получаем полный response и последовательно его парсим
-- Мы могли бы делать запрос к каждому источнику, сохранять полученный response в сыром виде, чтобы потом его распарсить, но это выглядит не оптимально в текущих условиях
-- Поэтому по диазайну, полученный rss сразу передаётся в feedparser, а результат записывается в news

create table if not exists news ( 
  item_id int generated always as identity primary key,
  item_date timestamp not null,
  item_title varchar,
  item_url varchar,
  item_description text,
  source_id int not null,  
  categ_id int,  
  author_id int   
);

create table if not exists sources (
  source_id int generated always as identity primary key,    
  source_url varchar
  source_name varchar
);

-- Категории. categ_group_id заполняется пользователем для более корректной аггрегации данных в отчётах
-- Возможно имеет смысл ввести parent_categ_id, однако решено пока не усложнять
create table if not exists categories (
  categ_id int generated always as identity primary key,  
  categ_group_id int,
  categ_name varchar
);

-- Группы категорий
create table if not exists category_groups (
  categ_group_id int generated always as identity primary key,  
  categ_group_name varchar
);

-- Авторы
create table if not exists authors (
  author_id int generated always as identity primary key,  
  author_name varchar
);

-- Заглушка витрины. Обновляется в коде по мере появления новых источников
create materialized view categories_summary_view as(
with counts as(
	select 
		coalesce(cg.categ_group_name, c.categ_name) categ_name, 
	  	count(*) total_count, 
	  	count(*) filter (where n.source_id = 0) src_dummy_count,	  
	  	count(*) filter (where n.item_date > current_timestamp - interval '1 day') total_count_24,
	  	count(*) filter (where n.source_id = 0 and n.item_date > current_timestamp - interval '1 day') src_dummy_count_24,
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
left join week_days wd on c.categ_name = wd.categ_name)