--Identify which vaccines are administered to patients older than 18 and the prevalence of each.

create view `causal-medium-230423.dataset2.adult_vaccinations` as 
select vax_type, count(age) as numVaccinestoAdults
from `causal-medium-230423.dataset2.primaryinfo_4` pi join `causal-medium-230423.dataset1clean.vaccine_PK1` v 
on pi.vaers_id = v.vaers_id
where age >= 18.0 
group by v.vax_type
order by numVaccinestoAdults desc

---Find number of cases per year reported with adverse symptoms concerning the antivaccination movement. Possibly compare the highs and lows with real life events played out in the media. 

create view `causal-medium-230423.dataset2.adverse_cases_by_year` as 
select pi.year, count(*) as adverse_cases
from `causal-medium-230423.dataset2.primaryinfo_4` pi left join `causal-medium-230423.dataset1clean.symptom_PK1` s 
on pi.vaers_id = s.vaers_id 
left join `causal-medium-230423.dataset1clean.vaccine_PK1` v on s.vaers_id = v.vaers_id
where s.symptom1 = "Autism" or s.symptom2 = "Autism" or pi.disabled = "True" or pi.recovered = "N" or pi.died = "True"
group by year

--Identify how many deaths occcured in at risk populations as a result of the vaccine. Example: over 65, under 2. Compare to number of deaths in other age categories. 

create view `causal-medium-230423.dataset2.age_category_deaths` as 
select count(*) as num_deaths,
case
  when age <= 2 then "Infant"
  when age between 2.1 and 13 then "Child"
  when age between 13.1 and 18 then "Adolescent"
  when age between 18.1 and 64.9 then "Adult"
  when age >= 65 then "Elderly"
  else "Invalid age" 
end as age_group
from `causal-medium-230423.dataset2.primaryinfo_4`
where died = "True" 
group by age_group


--Investigate average amount of deaths per state over the decade 2006-2016. Group by year.

select count(*) as deaths, state, year
from dataset2.primaryinfo_3
where died = "True"
group by state, year
order by state, year

-- Investigate the average amount of patients disabled as a cause of the vaccine by manufacturer. Group by manufacturer.

select count(*) disabled, v.vax_manu
from dataset2.primaryinfo_4 p right join dataset1clean.vaccine v on p.vaers_id = v.vaers_id
where p.disabled = "True"
group by v.vax_manu
order by disabled desc

--Investigate which vaccines caused Death or Disabled field to report yes. Group by vaccines.

create view `causal-medium-230423.dataset2.disabled_or_died` as
select count(*) disabled_or_died, v.vax_type
from `causal-medium-230423.dataset2.primaryinfo_4` p right join `causal-medium-230423.dataset1clean.vaccine` v on p.vaers_id = v.vaers_id
where p.disabled = "True" or p.disabled = "True"
group by v.vax_type
order by disabled_or_died desc