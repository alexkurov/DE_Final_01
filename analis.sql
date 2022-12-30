select category, count(title) as total_count_titles from articles a 
group by a.category
order by a.category;

select category, source, count(title) as count_by_source from articles a 
group by a.category , a."source"
order by a.category;

select category, count(title) as count_today from articles a
where date(pub_date) = current_date
group by a.category
order by a.category;

select category, source, count(title) as count_today_by_source from articles a
where date(pub_date) = current_date
group by a.category , a."source"
order by a.category;

select category, avg(count_by_date) from (
	select category, count(title) as count_by_date from articles a
	group by a.category, date(a.pub_date)
) as title_count
group by title_count.category
order by title_count.category;

select category, max(count_by_date) from (
	select category, date(pub_date) as cut_pub_date, count(title) as count_by_date from articles a
	group by a.category, cut_pub_date
) as title_count
group by title_count.category
order by title_count.category;

select category, to_char(pub_date, 'DY') as week_day, count(title) as count_day_by_source from articles a
group by a.category , week_day
order by a.category;
