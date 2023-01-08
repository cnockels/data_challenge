{%- set v_academic_year_offset = 8 -%}
{%- set v_academic_year_fall_semester_start_month = 9 -%}

{%- set v_fiscal_year_offset = 4 -%}
{%- set v_fiscal_year_start_month = 9 -%}


with 
    qryDates as 
    (
        select
            cal_date as calendar_date,
            extract(day from cal_date) as day_of_month,
            extract(month from cal_date) as calendar_month,
            extract(year from cal_date ) as calendar_year,

            trim(format_date("%d", cal_date)) as day_of_month_string,
            trim(format_date("%m", cal_date)) as calendar_month_string,
            trim(format_date("%Y", cal_date)) as calendar_year_string,

            extract(dayofweek from cal_date) as day_of_week,
            extract(dayofyear from cal_date) as day_of_year,
            extract(week from cal_date) as week_of_year,
            extract(quarter from cal_date) as quarter_of_year,
            cast(format_date('%G', cal_date) AS INT64) as year_iso,
            extract(isoweek from cal_date) as week_of_year_iso,
            --
            date_sub(cal_date, interval 1 day) as prev_day,
            date_add(cal_date, interval 1 day) as next_day,
            date_sub(cal_date, interval 1 week) as prev_week,
            date_add(cal_date, interval 1 week) as next_week,
            date_sub(cal_date, interval 1 month) as prev_month,
            date_add(cal_date, interval 1 month) as next_month,
            date_sub(cal_date, interval 1 year) as prev_year,
            date_add(cal_date, interval 1 year) as next_year,
            -- this is due to the way the date math works, if you want exactly 30 days, you have to specify it like this
            date_sub(cal_date, interval 30 day) as prev_30_days,
            date_add(cal_date, interval 30 day) as next_30_days,
            date_sub(cal_date, interval 180 day) as prev_180_days,
            date_add(cal_date, interval 180 day) as next_180_days,
            --
            date_trunc(cal_date, week) as first_day_of_week,
            last_day(cal_date, week) as last_day_of_week,
            date_trunc(cal_date, isoweek) as first_day_of_week_iso,
            last_day(cal_date, isoweek) as last_day_of_week_iso,
            date_trunc(cal_date, month) as first_day_of_month,
            last_day(cal_date, month) as last_day_of_month,
            date_trunc(cal_date, quarter) as first_day_of_quarter,
            last_day(cal_date, quarter) as last_day_of_quarter,
            date_trunc(cal_date, year) as first_day_of_year,
            last_day(cal_date, year) as last_day_of_year,
            date_trunc(cal_date, isoyear) as first_day_of_year_iso,
            last_day(cal_date, isoyear) as last_day_of_year_iso,
            --
            format_date("%A ", cal_date) as day_of_week_name,
            format_date("%a ", cal_date) as day_of_week_name_abbr,
            format_date("%B ", cal_date) as cal_month_name,
            format_date("%b ", cal_date) as cal_month_name_abbr,
            --
            -- formatted dates
            format_date("%Y-%m", cal_date) as fmt_year_dash_month_number,
            format_date("%Y %b", cal_date) as fmt_year_space_month_abbr,
            format_date("%Y %B", cal_date) as fmt_year_space_month_name,

            format_date("%m-%Y", cal_date) as fmt_month_number_dash_year,
            format_date("%b %Y", cal_date) as fmt_month_name_abbr_space_year,
            format_date("%b, %Y", cal_date) as fmt_month_name_abbr_comma_year,
            format_date("%B %Y", cal_date) as fmt_month_name_space_year,
            format_date("%B, %Y", cal_date) as fmt_month_name_comma_year,
            --
            -- academic_dates
            {{ getOffsetYear('cal_date', v_academic_year_fall_semester_start_month) }} as academic_year,
            {{ getAcademicYearLabel('cal_date', v_academic_year_fall_semester_start_month) }} as academic_year_label,
            {{ getMonthWithOffset('cal_date', v_academic_year_offset) }} as academic_month,
            --
            -- fiscal_dates
            {{ getOffsetYear('cal_date', v_fiscal_year_start_month) }} as us_fiscal_year,
            {{ getMonthWithOffset('cal_date', v_fiscal_year_offset) }} as us_fiscal_period,
            
            -- broadcast_dates
            date_add(date_trunc(cal_date, week), interval 1 day) as first_day_of_broadcast_week,
            date_add(last_day(cal_date, week), interval 1 day) as last_day_of_broadcast_week
        from
            unnest(generate_date_array(date('1899-12-31'), '2099-12-31', interval 1 day) ) as cal_date
    )

select * from qryDates