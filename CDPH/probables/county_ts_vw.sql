create or replace view COVID.PRODUCTION.VW_CDPH_COUNTY_AND_STATE_TIMESERIES_METRICS(
    DATE,
    AREA,
    AREA_TYPE,
    POPULATION,
    CASES,
    DEATHS,
    TOTAL_TESTS,
    POSITIVE_TESTS,
    REPORTED_CASES,
    REPORTED_DEATHS,
    REPORTED_TESTS,
    DAILY_TEST_POSITIVITY_RATE,
    MAX_CD_DATE,
    MAX_TESTING_DATE,
    MAX_RT_DATE,
    EPISODE_UNCERTAINTY_PERIOD,
    DEATH_UNCERTAINTY_PERIOD,
    TESTING_UNCERTAINTY_PERIOD,
    IS_MAX_CONFIDENT_EPISODE_DATE,
    IS_MAX_CONFIDENT_DEATH_DATE,
    IS_MAX_CONFIDENT_TESTING_DATE,
    IS_MAX_EPISODE_DATE,
    IS_MAX_DEATH_DATE,
    IS_MAX_TESTING_DATE,
    IS_MAX_CASE_REPORTED_DATE,
    IS_MAX_DEATH_REPORTED_DATE,
    IS_MAX_TEST_REPORTED_DATE,
    CUMULATIVE_REPORTED_CASES,
    CUMULATIVE_REPORTED_DEATHS,
    AVG_CASES_7_DAYS,
    AVG_DEATHS_7_DAYS,
    AVG_TOTAL_TESTS_7_DAYS,
    AVG_POSITIVE_TESTS_7_DAYS,
    AVG_CASES_REPORTED_7_DAYS,
    AVG_DEATHS_REPORTED_7_DAYS,
    AVG_TESTS_REPORTED_7_DAYS,
    SUM_CASES_REPORTED_7_DAYS,
    SUM_DEATHS_REPORTED_7_DAYS,
    SUM_TOTAL_TESTS_7_DAYS,
    SUM_POSITIVE_TESTS_7_DAYS,
    SUM_TESTS_REPORTED_7_DAYS,
    SUM_POSITIVE_TESTS_14_DAYS,
    SUM_TOTAL_TESTS_14_DAYS,
    CUMULATIVE_CASES_PER_100K,
    CUMULATIVE_DEATHS_PER_100K,
    AVG_CASE_RATE_PER_100K_7_DAYS,
    AVG_DEATH_RATE_PER_100K_7_DAYS,
    AVG_TEST_RATE_PER_100K_7_DAYS,
    AVG_CASE_REPORT_RATE_PER_100K_7_DAYS,
    AVG_DEATH_REPORT_RATE_PER_100K_7_DAYS,
    AVG_TEST_REPORT_RATE_PER_100K_7_DAYS,
    TEST_POSITIVITY_RATE_7_DAYS,
    TEST_POSITIVITY_RATE_14_DAYS,
    PCT_CH_CASES_REPORTED_1_DAY,
    PCT_CH_DEATHS_REPORTED_1_DAY,
    STATEWIDE_AVG_CASE_RATE_PER_100K_7_DAYS,
    STATEWIDE_AVG_DEATH_RATE_PER_100K_7_DAYS,
    STATEWIDE_AVG_TEST_RATE_PER_100K_7_DAYS,
    STATEWIDE_AVG_CASE_REPORT_RATE_PER_100K_7_DAYS,
    STATEWIDE_AVG_DEATH_REPORT_RATE_PER_100K_7_DAYS,
    STATEWIDE_AVG_TEST_REPORT_RATE_PER_100K_7_DAYS,
    STATEWIDE_TEST_POSITIVITY_RATE_7_DAYS,
    STATEWIDE_TEST_POSITIVITY_RATE_14_DAYS,
    HYPOTHETICAL_AVG_CASES_7_DAYS,
    HYPOTHETICAL_AVG_DEATHS_7_DAYS,
    HYPOTHETICAL_AVG_TESTS_7_DAYS,
    HYPOTHETICAL_AVG_CASES_REPORTED_7_DAYS,
    HYPOTHETICAL_AVG_DEATHS_REPORTED_7_DAYS,
    HYPOTHETICAL_AVG_TESTS_REPORTED_7_DAYS,
    INCREASE_CASE_RATE_PER_100K_7_DAYS,
    INCREASE_DEATH_RATE_PER_100K_7_DAYS,
    INCREASE_TEST_POSITIVITY_RATE_7_DAYS,
    INCREASE_TEST_POSITIVITY_RATE_14_DAYS,
    LATEST_CONFIDENT_AVG_CASE_RATE_PER_100K_7_DAYS,
    LATEST_CONFIDENT_AVG_DEATH_RATE_PER_100K_7_DAYS,
    LATEST_CONFIDENT_POSITIVITY_RATE_7_DAYS,
    LATEST_CONFIDENT_AVG_TOTAL_TESTS_7_DAYS,
    LATEST_CONFIDENT_AVG_TEST_RATE_PER_100K_7_DAYS,
    LATEST_CONFIDENT_INCREASE_CASE_RATE_PER_100K_7_DAYS,
    LATEST_CONFIDENT_INCREASE_DEATH_RATE_PER_100K_7_DAYS,
    LATEST_CONFIDENT_INCREASE_POSITIVITY_RATE_7_DAYS,
    LATEST_TOTAL_CONFIRMED_CASES,
    LATEST_TOTAL_CONFIRMED_DEATHS,
    LATEST_TOTAL_TESTS_PERFORMED,
    NEWLY_REPORTED_CASES,
    NEWLY_REPORTED_DEATHS,
    NEWLY_REPORTED_TESTS,
    NEWLY_REPORTED_CASES_LAST_7_DAYS,
    NEWLY_REPORTED_DEATHS_LAST_7_DAYS,
    NEWLY_REPORTED_TESTS_LAST_7_DAYS,
    LATEST_PCT_CH_CASES_REPORTED_1_DAY,
    LATEST_PCT_CH_DEATHS_REPORTED_1_DAY,
    LATEST_PCT_CH_TOTAL_TESTS_REPORTED_1_DAY,
    LATEST_PCT_CH_TOTAL_TESTS_REPORTED_7_DAYS,
    LATEST_POSITIVITY_RATE_7_DAYS,
    LATEST_INCREASE_POSITIVITY_RATE_7_DAYS,
    LATEST_POSITIVITY_RATE_14_DAYS,
    LATEST_INCREASE_POSITIVITY_RATE_14_DAYS
) COMMENT='combined table of cases, deaths, and testing data for use with v2.0 of Cases and State Dashes -djmolitor'
 as --*/
with 
cases_deaths_data as (
    select *
      ,max(sf_load_timestamp) over (partition by '1') as last_cd_load_timestamp
    from PRODUCTION.CDPH_GOOD_CASES_DEATHS_COUNTY_STAGE
)
,cases_deaths as (
   select
      initcap(county) as area
      ,date
      ,sum(cases_epdate)   as cases
      ,sum(deaths_dod)  as deaths
      ,sum(cases_repdate)  as reported_cases
      ,sum(deaths_repdate) as reported_deaths
      ,sum(deaths_epdate)  as fatal_cases
  from cases_deaths_data
    where sf_load_timestamp=last_cd_load_timestamp
  group by 1,2
  UNION
  select 
      'California'         as area
      ,date
      ,sum(cases_epdate)   as cases
      ,sum(deaths_dod)  as deaths
      ,sum(cases_repdate)  as reported_cases
      ,sum(deaths_repdate) as reported_deaths
      ,sum(deaths_epdate)  as fatal_cases
  from cases_deaths_data
    where sf_load_timestamp=last_cd_load_timestamp
  group by 1,2
)
,tests_data as (
    select * 
      ,max(sf_load_timestamp) over (partition by '1') as max_tt_load_timestamp
    from PRODUCTION.CDPH_GOOD_TESTS_COUNTY_COLLECTION_DATE_ALL
    order by report_date desc, date desc, county
)
,tests as (
  select 
    initcap(county)      as area
    ,date
    ,sum(TOTALTESTS_EARLYSPECDATE)  as total_tests
    ,sum(POSTESTS_EARLYSPECDATE)    as positive_tests
  from tests_data
    where sf_load_timestamp=max_tt_load_timestamp
  group by 1,2
  union
  select 
    'California'         as area
    ,date
    ,sum(TOTALTESTS_EARLYSPECDATE)  as total_tests
    ,sum(POSTESTS_EARLYSPECDATE)    as positive_tests
  from tests_data
    where sf_load_timestamp=max_tt_load_timestamp
  group by 1,2
)   
-- REFERENCE TO v1.0 TESTING DATA
,tests_by_repdate_data_1 as (
  select 
    county
    ,dateadd('d',2,date) as date        
    ,sum(floor(tests)) as reported_tests   -- Floor function: per Brooke Bregman, 1/23/2021: "Back in [May] when we added some estimates on top of the ELR data, it was in partial numbers before someone realized and started rounding down... round down for these"
  from public.COUNTY_TESTSREPORTED_TALL 
  group by 1,2
  order by date desc
)
,tests_by_repdate_data_2 as (
  select 
    county
    ,report_date as date
    ,sum(floor(pcr_tests)) as reported_tests   -- Floor function: per Brooke Bregman, 1/23/2021: "Back in [May] when we added some estimates on top of the ELR data, it was in partial numbers before someone realized and started rounding down... round down for these"
  from PRODUCTION.CDPH_GOOD_TESTS_REPORT_DATE_ELR 
  group by 1,2
  order by 1,2
)
,cutoff as (select '2021-01-26'::date as cutoff_date)
,tests_by_repdate_data as (
  select *
  from tests_by_repdate_data_1
    where date<(select cutoff_date from cutoff)
  UNION
  select *
  from tests_by_repdate_data_2
    where date>=(select cutoff_date from cutoff)
    and lower(county)!='total'
) 
,tests_by_repdate as (      -- XXX2 needs to include get pre-May statewide data included
  select 
      county as area
      ,date
      ,sum(reported_tests) as reported_tests
  from tests_by_repdate_data
  group by 1,2
  UNION
  select 
      'California' as area
      ,date
      ,sum(reported_tests) as reported_tests
  from tests_by_repdate_data
  group by 1,2
)
,county_pops_data as (
  select * from PRODUCTION.CDPH_STATIC_DEMOGRAPHICS_BY_COUNTY
    where demographic_set in 
      (select distinct demographic_set from PRODUCTION.CDPH_STATIC_DEMOGRAPHICS_BY_COUNTY order by 1 limit 1)
    and sf_load_timestamp=(select max(sf_load_timestamp) from PRODUCTION.CDPH_STATIC_DEMOGRAPHICS_BY_COUNTY)
)
,dates as (
  select 
    date
    -- written like this here so they populate ALL rows even where there's no data (so uncertainty period calcs dont break after max dates)
    ,(select max(date) from cases_deaths)       as max_cd_date
    ,(select max(date) from tests)              as max_testing_date
    ,(select max(date) from tests_by_repdate)   as max_rt_date
  from PRODUCTION.dim_date
  UNION
  select 
    NULL as date
    ,(select max(date) from cases_deaths)       as max_cd_date
    ,(select max(date) from tests)              as max_testing_date
    ,(select max(date) from tests_by_repdate)   as max_rt_date
)
,county_pops as (
  select 
    initcap(county)  as area
    ,case county when 'California' then 'State' else 'County'
        end          as area_type
    ,sum(population) as population
  from county_pops_data
  group by 1,2
  UNION
    select
    'Unknown'        as area
    ,'County'        as area_type
    ,NULL            as population
  UNION
    select
    'Out of state'   as area
    ,'County'        as area_type
    ,NULL            as population
)
,backbone as (
   select 
     d.date
     ,d.max_cd_date
     ,d.max_testing_date
     ,d.max_rt_date
     ,cpops.area
     ,cpops.area_type
     ,cpops.population
   from dates d
   cross join county_pops cpops
   where d.date<=greatest(d.max_cd_date,d.max_testing_date,d.max_rt_date) OR d.date is NULL
) 
,core as (
  select
    bb.date
    ,bb.area
    ,bb.area_type
    ,bb.population
    ,cd.cases
    ,cd.deaths
    ,t.total_tests              as total_tests              
    ,t.positive_tests           as positive_tests           
    ,cd.reported_cases
    ,cd.reported_deaths
    ,rt.reported_tests          as reported_tests            
    ,bb.max_cd_date          
    ,bb.max_testing_date
    ,bb.max_rt_date    
    ,sum(cd.reported_cases)  over (partition BY cd.area ORDER BY cd.date ASC rows UNBOUNDED PRECEDING) as cumulative_reported_cases  
    ,sum(cd.reported_deaths) over (partition by cd.area order by cd.date ASC rows UNBOUNDED PRECEDING) as cumulative_reported_deaths 
    --,sum(rt.reported_tests)  over (partition by rt.area order by rt.date ASC rows UNBOUNDED PRECEDING) as cumulative_reported_tests 
  from backbone bb
  left join tests             t  on  (bb.date=t.date  OR (bb.date is NULL AND t.date  is NULL)) and lower(bb.area)=lower(t.area) 
  left join cases_deaths      cd on  (bb.date=cd.date OR (bb.date is NULL AND cd.date is NULL)) and lower(bb.area)=lower(cd.area)      
  left join tests_by_repdate  rt on  (bb.date=rt.date OR (bb.date is NULL AND rt.date is NULL)) and lower(bb.area)=lower(rt.area)          
)
,windows as (
  select
    date
    ,area
    ,area_type
    ,population
    ,cases
    ,deaths
    ,total_tests
    ,positive_tests
    ,reported_cases
    ,reported_deaths
    ,reported_tests
    -- calcs
    ,iff(total_tests=0,0,positive_tests/total_tests) as daily_test_positivity_rate
    ,max_cd_date,max_testing_date,max_rt_date
    ,iff(date>(max_cd_date-7),TRUE,FALSE)      as episode_uncertainty_period
    ,iff(date>(max_cd_date-21),TRUE,FALSE)     as death_uncertainty_period    
    ,iff(date>(max_testing_date-7),TRUE,FALSE) as testing_uncertainty_period
    ,iff(date=(max_cd_date-7),TRUE,FALSE)      as is_max_confident_episode_date
    ,iff(date=(max_cd_date-21),TRUE,FALSE)     as is_max_confident_death_date
    ,iff(date=(max_testing_date-7),TRUE,FALSE) as is_max_confident_testing_date
    ,iff(date=max_cd_date,TRUE,FALSE)          as is_max_episode_date
    ,iff(date=max_cd_date,TRUE,FALSE)          as is_max_death_date
    ,iff(date=max_testing_date,TRUE,FALSE)     as is_max_testing_date
    ,iff(date=max_cd_date,TRUE,FALSE)          as is_max_case_reported_date
    ,iff(date=max_cd_date,TRUE,FALSE)          as is_max_death_reported_date
    ,iff(date=max_rt_date,TRUE,FALSE)          as is_max_test_reported_date
    ,cumulative_reported_cases
    ,cumulative_reported_deaths
    --,cumulative_reported_tests
    --,cumulative_reported_positive_tests
    -- note: expressed as average of daily values in the last 7 days
    ,iff(date<=max_cd_date,      AVG(core.cases)           OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_cases_7_days                   -- 7 days of episode date
    ,iff(date<=max_cd_date,      AVG(core.deaths)          OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_deaths_7_days                  -- 7 days of death date
    ,iff(date<=max_testing_date, AVG(core.total_tests)     OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_total_tests_7_days             -- 7 days of test date
    ,iff(date<=max_testing_date, AVG(core.positive_tests)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_positive_tests_7_days          -- 7 days of test date
    ,iff(date<=max_cd_date,      AVG(core.reported_cases)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_cases_reported_7_days          -- 7 days of reported date
    ,iff(date<=max_cd_date,      AVG(core.reported_deaths) OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_deaths_reported_7_days         -- 7 days of reported date
    ,iff(date<=max_rt_date,      AVG(core.reported_tests)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as avg_tests_reported_7_days          -- 7 days of reported date
    ,iff(date<=max_cd_date,      SUM(core.reported_cases)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as sum_cases_reported_7_days          -- 7 days of reported date
    ,iff(date<=max_cd_date,      SUM(core.reported_deaths) OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as sum_deaths_reported_7_days         -- 7 days of reported date
    ,iff(date<=max_testing_date, SUM(core.total_tests)     OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as sum_total_tests_7_days             -- 7 days of test date
    ,iff(date<=max_testing_date, SUM(core.positive_tests)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as sum_positive_tests_7_days          -- 7 days of test date
    ,iff(date<=max_rt_date,      SUM(core.reported_tests)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),NULL)  as sum_tests_reported_7_days          -- 7 days of reported date
    ,iff(date<=max_testing_date, SUM(core.positive_tests)  OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),NULL) as sum_positive_tests_14_days         -- 14 days of test date
    ,iff(date<=max_testing_date, SUM(core.total_tests)     OVER (PARTITION BY core.area ORDER BY core.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),NULL) as sum_total_tests_14_days            -- 14 days of test date
  from core
  order by 1,2
)  
,rates as (
  select * 
    -- note: not rates, but a cumulative measure of prevalence; no time in denominator
      ,cumulative_reported_cases   /(population/100000) as cumulative_cases_per_100k 
      ,cumulative_reported_deaths  /(population/100000) as cumulative_deaths_per_100k
      --,cumulative_reported_tests   /(population/100000) as cumulative_total_tests_per_100k
    -- note: expressed as average of daily case rates
      ,avg_cases_7_days            /(population/100000) as avg_case_rate_per_100k_7_days    
      ,avg_deaths_7_days           /(population/100000) as avg_death_rate_per_100k_7_days
      ,avg_total_tests_7_days      /(population/100000) as avg_test_rate_per_100k_7_days
      ,avg_cases_reported_7_days   /(population/100000) as avg_case_report_rate_per_100k_7_days
      ,avg_deaths_reported_7_days  /(population/100000) as avg_death_report_rate_per_100k_7_days
      ,avg_tests_reported_7_days   /(population/100000) as avg_test_report_rate_per_100k_7_days
    -- include logic to avoid dividing by 0
      ,iff(sum_total_tests_7_days=0         ,0,sum_positive_tests_7_days /sum_total_tests_7_days)                          as test_positivity_rate_7_days
      ,iff(sum_total_tests_14_days=0        ,0,sum_positive_tests_14_days/sum_total_tests_14_days)                         as test_positivity_rate_14_days
      ,iff(reported_cases=0   ,0, reported_cases  / greatest(cumulative_reported_cases-reported_cases,  reported_cases)  ) as pct_ch_cases_reported_1_day
      ,iff(reported_deaths=0  ,0, reported_deaths / greatest(cumulative_reported_deaths-reported_deaths,reported_deaths) ) as pct_ch_deaths_reported_1_day
     -- ,iff(reported_tests=0   ,0, reported_tests  / greatest(cumulative_reported_tests-reported_tests,  reported_tests)  ) as pct_ch_tests_reported_1_day
  from windows
)
,rates_w_statewides as (
  select county_rates.*
    ,state_rates.avg_case_rate_per_100k_7_days   as statewide_avg_case_rate_per_100k_7_days
    ,state_rates.avg_death_rate_per_100k_7_days  as statewide_avg_death_rate_per_100k_7_days
    ,state_rates.avg_test_rate_per_100k_7_days   as statewide_avg_test_rate_per_100k_7_days
    ,state_rates.avg_case_report_rate_per_100k_7_days   as statewide_avg_case_report_rate_per_100k_7_days
    ,state_rates.avg_death_report_rate_per_100k_7_days  as statewide_avg_death_report_rate_per_100k_7_days
    ,state_rates.avg_test_report_rate_per_100k_7_days   as statewide_avg_test_report_rate_per_100k_7_days
    ,state_rates.test_positivity_rate_7_days            as statewide_test_positivity_rate_7_days
    ,state_rates.test_positivity_rate_14_days           as statewide_test_positivity_rate_14_days
  from rates        county_rates
  left join rates   state_rates
    on (county_rates.date=state_rates.date  OR (county_rates.date is NULL AND state_rates.date  is NULL)) 
  where state_rates.area='California'
)
,comparisons as (
  select *
        -- (statewide 7-day avg [metric] per 100k rates) * (county population in 100k) = (hypothetical 7-day avg [metric])
      ,statewide_avg_case_rate_per_100k_7_days  *(POPULATION/100000)  as hypothetical_avg_cases_7_days
      ,statewide_avg_death_rate_per_100k_7_days *(POPULATION/100000)  as hypothetical_avg_deaths_7_days
      ,statewide_avg_test_rate_per_100k_7_days  *(POPULATION/100000)  as hypothetical_avg_tests_7_days
      ,statewide_avg_case_report_rate_per_100k_7_days  *(POPULATION/100000)  as hypothetical_avg_cases_reported_7_days
      ,statewide_avg_death_report_rate_per_100k_7_days *(POPULATION/100000)  as hypothetical_avg_deaths_reported_7_days
      ,statewide_avg_test_report_rate_per_100k_7_days  *(POPULATION/100000)  as hypothetical_avg_tests_reported_7_days
  from rates_w_statewides
)
,trends as (
  select *
    ,avg_case_rate_per_100k_7_days 
        - LAG(avg_case_rate_per_100k_7_days,7)    OVER (PARTITION BY area ORDER BY date) 
              as increase_case_rate_per_100k_7_days
    ,avg_death_rate_per_100k_7_days 
        - LAG(avg_death_rate_per_100k_7_days,7)    OVER (PARTITION BY area ORDER BY date) 
              as increase_death_rate_per_100k_7_days
    ,test_positivity_rate_7_days 
        - LAG(test_positivity_rate_7_days,7)    OVER (PARTITION BY area ORDER BY date) 
              as increase_test_positivity_rate_7_days
    ,test_positivity_rate_14_days 
        - LAG(test_positivity_rate_14_days,7)    OVER (PARTITION BY area ORDER BY date) 
              as increase_test_positivity_rate_14_days
  from comparisons
)
,display_metrics as (
 select *
  -- readings on max confident dates
  ,iff(is_max_confident_episode_date,avg_case_rate_per_100k_7_days,NULL)        as latest_confident_avg_case_rate_per_100k_7_days
  ,iff(is_max_confident_death_date,avg_death_rate_per_100k_7_days,NULL)         as latest_confident_avg_death_rate_per_100k_7_days
--,iff(is_max_confident_testing_date,test_positivity_rate_7_days,NULL)          as latest_confident_positivity_rate_7_days 
-- removing uncertainty period for positivity calc, leaving word "confident" in for legacy compatibility; this is now a synonym of latest_positivity_rate_7_days  -djm 2021feb17
  ,iff(is_max_testing_date,test_positivity_rate_7_days,NULL)                    as latest_confident_positivity_rate_7_days                  
  ,iff(is_max_confident_testing_date,avg_total_tests_7_days,NULL)               as latest_confident_avg_total_tests_7_days
  ,iff(is_max_confident_testing_date,avg_test_rate_per_100k_7_days,NULL)        as latest_confident_avg_test_rate_per_100k_7_days
  ,iff(is_max_confident_episode_date,increase_case_rate_per_100k_7_days,NULL)   as latest_confident_increase_case_rate_per_100k_7_days
  ,iff(is_max_confident_death_date,increase_death_rate_per_100k_7_days,NULL)    as latest_confident_increase_death_rate_per_100k_7_days
--,iff(is_max_confident_testing_date,increase_test_positivity_rate_7_days,NULL) as latest_confident_increase_positivity_rate_7_days 
-- removing uncertainty period for positivity calc, leaving word "confident" in for legacy compatibility; this is now a synonym of latest_increase_positivity_rate_7_days  -djm 2021feb17
  ,iff(is_max_testing_date,increase_test_positivity_rate_7_days,NULL)           as latest_confident_increase_positivity_rate_7_days         
  -- readings on max dates
  ,iff(is_max_case_reported_date ,cumulative_reported_cases,NULL)         as latest_total_confirmed_cases
  ,iff(is_max_death_reported_date,cumulative_reported_deaths,NULL)        as latest_total_confirmed_deaths
  ,iff(is_max_testing_date, 
       SUM(total_tests)     OVER (PARTITION BY area)
       ,NULL)                                                             as latest_total_tests_performed 
  ,iff(is_max_case_reported_date ,reported_cases,NULL)                    as newly_reported_cases
  ,iff(is_max_death_reported_date,reported_deaths,NULL)                   as newly_reported_deaths
  ,iff(is_max_test_reported_date ,reported_tests,NULL)                    as newly_reported_tests
  ,iff(is_max_case_reported_date ,sum_cases_reported_7_days,NULL)         as newly_reported_cases_last_7_days
  ,iff(is_max_death_reported_date,sum_deaths_reported_7_days,NULL)        as newly_reported_deaths_last_7_days
  ,iff(is_max_test_reported_date ,sum_tests_reported_7_days,NULL)         as newly_reported_tests_last_7_days
  ,iff(is_max_case_reported_date ,pct_ch_cases_reported_1_day,NULL)       as latest_pct_ch_cases_reported_1_day
  ,iff(is_max_death_reported_date,pct_ch_deaths_reported_1_day,NULL)      as latest_pct_ch_deaths_reported_1_day
  ,iff(is_max_test_reported_date ,
       reported_tests               / ((SUM(total_tests) OVER (PARTITION BY area)) - reported_tests)
       ,NULL)                                                             as latest_pct_ch_total_tests_reported_1_day   
  ,iff(is_max_test_reported_date ,
       sum_tests_reported_7_days    / ((SUM(total_tests) OVER (PARTITION BY area)) - sum_tests_reported_7_days)
       ,NULL)                                                             as latest_pct_ch_total_tests_reported_7_days  
  ,iff(is_max_testing_date,test_positivity_rate_7_days,NULL)              as latest_positivity_rate_7_days                  
  ,iff(is_max_testing_date,increase_test_positivity_rate_7_days,NULL)     as latest_increase_positivity_rate_7_days     
  ,iff(is_max_testing_date,test_positivity_rate_14_days,NULL)             as latest_positivity_rate_14_days                  
  ,iff(is_max_testing_date,increase_test_positivity_rate_14_days,NULL)    as latest_increase_positivity_rate_14_days         
  from trends
)
select *
from display_metrics
order by date desc,area
;
