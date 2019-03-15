--This table provides the comparison between listing "Crying" as the first symptom or the second per year
--A limit of 40 is set so only years that crying was reported more than 40 times are listed

select year, count( distinct symptom1) as crying_symptom1, count(distinct symptom2) as crying_symptom2
from dataset1clean.symptom_PK1 s 
where symptom1 = 'Crying' or symptom2 = 'Crying'
group by year
having count(distinct symptom1) > 40

--This table provides a view of less commonly made vaccines and in which quantities they were made by year
--A limit of 50 is set so only those vaccines manufactured less than 50 times per year are listed
select year, vax_name, count(vax_name) as vax_count
from dataset1clean.vaccine_PK1 s  
group by vax_name, year
having vax_count < 50
order by year

--This table provides a view of which vax_routes cause how many complaints 

select vax_route, count(vax_route) as num_vaccines_total, count(distinct v.vaers_id) as patients_reporting_total
from dataset1clean.vaccine_PK1 v join dataset1clean.symptom_PK1 s on v.vaers_id = s.vaers_id
group by vax_route
having num_vaccines_total > 5000
order by num_vaccines_total

--This table provides the number of distinct symptom1 and symptom2 records per year
--This provides information on the variety of symptom1 and symptom2 reported by the years

create table dataset1clean.num_vaccines_by_years as
select year, count(DISTINCT symptom1) as count_symptom1, count(DISTINCT symptom2) as count_symptom2
from dataset1clean.symptom_PK1 s 
where year >= '2006'
group by year
order by year

--This uses the sum function after creating a table from last query. 
--Creates total number of symptoms

select year, sum(count_symptom1 + count_symptom2) AS Total_Symptoms
from `causal-medium-230423.dataset1clean.num_vaccines_by_years` s 
where year >= '2006'
group by year
order by year

-- Lists the top 15 occurring unique symptoms that occurred and how many different vaccines they were caused by, in order of greatest occurrence to least. 
create view `causal-medium-230423.dataset1clean.symptoms_numvaccines` as
select s.symptom1, count(distinct v.vax_name) as CausedByNumVaxes
from `causal-medium-230423.dataset1clean.vaccine_PK1` v right join `causal-medium-230423.dataset1clean.symptom_PK1` s on v.vaers_id = s.vaers_id
group by s.symptom1
order by count(v.vax_name) desc
limit 15

-- number of specific vaccines that caused this specific symptom as the primary symptom
select s.symptom1, count(distinct v.vax_name) as CausedByNumVaxes
from dataset1clean.vaccine_PK1 v right join `dataset1clean.symptom_PK1` s on v.vaers_id = s.vaers_id
group by s.symptom1
order by count(v.vax_name) desc

-- number of deaths attributed to each manufacturer caused over the 11 year span
create view `causal-medium-230423.dataset1clean.manufacturer_deaths` as
select v.vax_manu, count(s.symptom1="Death") as resulting_deaths
from `causal-medium-230423.dataset1clean.vaccine_PK1` v right join `causal-medium-230423.dataset1clean.symptom_PK1` s on v.vaers_id = s.vaers_id
where s.symptom1="Death" 
group by v.vax_manu
order by count(s.symptom1="Death") desc

-- returns the specific symptom and how often that symptom is listed as both symptom1 and symptom2
select symptom1 as symptom, count(symptom1=symptom2) as num_repeates
from dataset1clean.symptom_PK1 
group by symptom1
order by count(symptom1=symptom2) desc