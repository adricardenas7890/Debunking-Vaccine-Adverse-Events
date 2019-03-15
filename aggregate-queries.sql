--This table provides the number of distinct symptom1 and symptom2 records per year
--This provides information on the variety of symptoms reported by the years
--Findings: around the year 2007

select year, count(DISTINCT symptom1) as count_symptom1, count(DISTINCT symptom2) as count_symptom2
from dataset1clean.symptom_PK1 s 
where year >= '2006'
group by year
order by year

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