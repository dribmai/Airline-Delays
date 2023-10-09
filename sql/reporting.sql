/* Here I define the `reporting` schema */
CREATE SCHEMA IF NOT EXISTS reporting;

/*
Here I am scripting the definition of the reporting.flight view which:
- will delete data about canceled flights `cancelled = 0`
- will contain the `is_delayed` column, as defined earlier (notebook 04_Data_analysis_01), that is `is_delayed = 1 if dep_delay_new > 15 else 0` (implemented in SQL)
*/
CREATE OR REPLACE VIEW reporting.flight as
SELECT *,
    case
        when f.dep_delay_new > 15 then 1
        else 0
    END AS is_delayed
FROM public.flight f
where f.cancelled = 0;
/*
Here I'm scripting the definition of the reporting.top_reliability_roads view, that will contain the following columns:
- `origin_airport_id`,
- `origin_airport_name`,
- `dest_airport_id`,
- `dest_airport_name`,
- `year`,
- `cnt` - as the number of flights performed on a given route,
- `reliability` - as a percentage of delays on a given route,
- `nb` - numbered from 1, 2, 3 according to the `reliability` column. In case of the same values ​​it should return 1, 2, 2, 3...
The result should include routes with over 10,000 flights.
*/
CREATE OR REPLACE VIEW reporting.top_reliability_roads AS
with flight_ext as (
    SELECT *,
        case when f.dep_delay_new > 15 then 1 else 0 END AS is_delayed
    FROM public.flight f
    where f.cancelled = 0),

    flight_ext2 as(
                    SELECT
                        fe.origin_airport_id,
                        fe.dest_airport_id,
                        fe.year,
                        count(fe.dep_delay_new) as cnt,
                        round(sum(fe.is_delayed)*1.0/count(fe.dep_delay_new),2) as reliability
                    from flight_ext fe
                    group by fe.origin_airport_id, fe.dest_airport_id, fe.year
                    having count(fe.dep_delay_new) > 10000),

    flight_ext3 as(
                    select p.*, al.origin_city_name as dest_airport_name
                    from
                        (select fee.*, a.origin_city_name as origin_airport_name
                        from flight_ext2 fee
                        left join airport_list a on fee.origin_airport_id = a.origin_airport_id) as p
                    left join airport_list al on p.dest_airport_id = al.origin_airport_id)


SELECT *, dense_rank() over (my_window) as nb
FROM flight_ext3 feee
WINDOW my_window as (order by reliability desc)
;

/*
Here I'm scripting the definition of the reporting.year_to_year_comparision view, that will contain the following columns:
- `year`
- `month`,
- `flights_amount`
- `reliability`
*/
CREATE OR REPLACE VIEW reporting.year_to_year_comparision AS
with flight_ext as (
    SELECT *,
        case when f.dep_delay_new > 15 then 1 else 0 END AS is_delayed
    FROM public.flight f
    where f.cancelled = 0)
SELECT
    fe.year,
    fe.month,
    count(fe.dep_delay_new) as flights_amount,
    round(sum(fe.is_delayed)*1.0/count(fe.dep_delay_new),2) as reliability
from flight_ext fe
group by fe.year, fe.month
;
/*
Here I'm scripting the definition of reporting.day_to_day_comparision view, that will contain the following columns:
- `year`
- `day_of_week`
- `flights_amount`
*/
CREATE OR REPLACE VIEW reporting.day_to_day_comparision AS
with flight_ext as (
    SELECT *,
        case when f.dep_delay_new > 15 then 1 else 0 END AS is_delayed
    FROM public.flight f
    where f.cancelled = 0)
SELECT
    fe.year,
    fe.day_of_week,
    count(fe.dep_delay_new) as flights_amount
from flight_ext fe
group by fe.year, fe.day_of_week
;
/*
Here I'm scripting the definition of reporting.day_by_day_reliability view, that will contain the following columns:
- `date` as a combination of columns `year`, `month`, `day`, should be `date` type
- `reliability` as a percentage of delays on a given day

Note: Remember not to add a semicolon at the end - when using split, an empty query will appear, which will result in an error when trying to execute the script from jupyter notebook
*/
CREATE OR REPLACE VIEW reporting.day_by_day_reliability AS
with flight_ext as (
    SELECT *,
        to_date(cast(f.year as varchar) ||  lpad(cast(f.month as varchar),2,'0') || lpad(cast(f.day_of_month as varchar),2,'0'),'YYYYMMDD') as date,
        case when f.dep_delay_new > 15 then 1 else 0 END AS is_delayed
    FROM public.flight f
    where f.cancelled = 0)

SELECT
    date,
    round(sum(fe.is_delayed)*1.0/count(fe.dep_delay_new),2) as reliability
from flight_ext fe
group by date