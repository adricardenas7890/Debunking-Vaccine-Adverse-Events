
— “NOT IN” 
—display only the symptoms that do not render as null. somehow transforms.sql did not catch these mistakes

select vaers_id, symptom from dataset1clean.vaersid_symptom where vaers_id NOT IN
(select vaers_id from dataset1clean.vaersid_symptom where symptom is null)

— “IN”, “COUNT()”, GROUP BY
— displays the number of crying reports by each vaccine 

select vax_name, count(vax_name) as amount_of_crying_patients from dataset1clean.vaccine_PK1 where vaers_id IN
(select vaers_id from dataset1clean.vaersid_symptom where symptom = 'Crying')
group by vax_name 


—“IN”, “COUNT()”, GROUP BY
— displays the number of deaths by each vaccine and which year they occured

select v.vax_name, s.symptom, v.year, count(v.vax_name) as amount_of_deaths from dataset1clean.vaccine_PK1 v join dataset1clean.symptom_PK1 s on where vaers_id IN
(select vaers_id from dataset1clean.vaersid_symptom where symptom = 'Death')
group by year, vax_name
order by vax_name

— This query returns information for vaers_id where vax_name is HIBV

select v.vax_name, s.vaers_id, v.year,s.symptom1, s.symptom2
from dataset1clean.symptom_PK1 s join dataset1clean.vaccine_PK1 v on s.vaers_id = v.vaers_id 
where v.vax_manu in (select vax_manu from dataset1clean.vaccine_PK1 where vax_name = “HIBV”)


-- This returns the number of cases that were assigned a vaers_id in the vaccine table but did not have a matching one in the symptom table - indicating that they did not have associated reported symptoms.

select vax_type, count(*) as num_cases_no_symptoms from dataset1clean.vaccine_PK1 where vaers_id not in
(select vaers_id from dataset1clean.symptom_PK1) 
group by vax_type

-- All years that have distinct symptoms as symptom1 that are greater than the average number of distinct symptoms 

select distinct year, count(*) as distinct_symptoms
from dataset1clean.symptom_PK1
group by year 
having count(distinct symptom1) > 
(select avg(symptom_count) from 
(select year, count(distinct symptom1) as symptom_count from dataset1clean.symptom_PK1
group by year))

-- All vaers_id's and associated symptoms from cases from Merck & Co. Inc.

select v.vax_manu, s.vaers_id, v.year,s.symptom1, s.symptom2
from dataset1clean.symptom_PK1 s join dataset1clean.vaccine_PK1 v on s.vaers_id = v.vaers_id 
where v.vax_manu in (select vax_manu from dataset1clean.vaccine_PK1 where vax_manu = "MERCK & CO. INC.")

-- This returns the years that had the PPV vaccine administered more often than the average and where erythema was listed as symptom1 more often than the average

select v.year, count(*) as PPV_Erythema_Count
from dataset1clean.symptom_PK1 s join dataset1clean.vaccine_PK1 v on s.vaers_id = v.vaers_id
group by year
having count(vax_type="PPV") > 
(select avg(PPV_count) from
(select year, count(vax_type="PPV") as PPV_count from dataset1clean.vaccine_PK1 
group by year)) 
and count(symptom1 ="Erythema") > 
(select avg(erythema_count) from
(select year, count(symptom1 ="PPV") as erythema_count from dataset1clean.symptom_PK1 
group by year))


