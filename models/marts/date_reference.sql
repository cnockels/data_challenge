with 
    qryDates as (
        select * from {{ ref('stg_date_reference') }}
    ),

    qryFinal as (
        select
            cast(format_date('%Y%m%d', calendar_date) as INT64) as date_wid,
            calendar_date,
            day_of_month,
            day_of_week,
            day_of_year,

            day_of_month_string,
            calendar_month_string,
            calendar_year_string,

            calendar_month,
            calendar_year,
            --
            day_of_week_name,
            day_of_week_name_abbr,
            cal_month_name,
            cal_month_name_abbr,
            --
            week_of_year,
            quarter_of_year,
            year_iso,
            week_of_year_iso,
            --
            prev_day,
            next_day,
            prev_week,
            next_week,
            prev_month,
            next_month,
            prev_year,
            next_year,
            prev_30_days,
            next_30_days,
            prev_180_days,
            next_180_days,
            --
            first_day_of_week,
            last_day_of_week,
            first_day_of_week_iso,
            last_day_of_week_iso,
            first_day_of_month,
            last_day_of_month,
            first_day_of_quarter,
            last_day_of_quarter,
            first_day_of_year,
            last_day_of_year,
            first_day_of_year_iso,
            last_day_of_year_iso,
            --
            calendar_date = first_day_of_week as is_first_day_of_week,
            calendar_date = last_day_of_week as is_last_day_of_week,
            calendar_date = first_day_of_week_iso as is_first_day_of_week_iso,
            calendar_date = last_day_of_week_iso as is_last_day_of_week_iso,
            calendar_date = first_day_of_month as is_first_day_of_month,
            calendar_date = last_day_of_month as is_last_day_of_month,
            calendar_date = first_day_of_quarter as is_first_day_of_quarter,
            calendar_date = last_day_of_quarter as is_last_day_of_quarter,
            calendar_date = first_day_of_year as is_first_day_of_year,
            calendar_date = last_day_of_year as is_last_day_of_year,
            calendar_date = first_day_of_year_iso as is_first_day_of_year_iso,
            calendar_date = last_day_of_year_iso as is_last_day_of_year_iso,
            --
            -- formatted dates
            fmt_year_dash_month_number,
            fmt_year_space_month_abbr,
            fmt_year_space_month_name,
            fmt_month_number_dash_year,
            fmt_month_name_abbr_space_year,
            fmt_month_name_abbr_comma_year,
            fmt_month_name_space_year,
            fmt_month_name_comma_year,
            --
            -- academic_dates
            academic_year,
            academic_year_label,
            academic_month,
            {{ getAcademicSemester('academic_month') }} as academic_semester,
            concat({{ getAcademicSemester('academic_month') }}, " '",  format_date('%y', calendar_date)) as academic_semester_year,
            --
            -- US Fiscal Dates
            us_fiscal_year,
            us_fiscal_period,
            {{ getQuarter('calendar_month') }} as us_fiscal_quarter,
            {{ getTrimester('us_fiscal_period') }} as us_fiscal_trimester,
            {{ getSemester('us_fiscal_period') }} as us_fiscal_semester,
        from
            qryDates
    )

 select * from qryFinal