-- creating new table "Symptoms" in the dataset1clean folder

create table dataset1clean.Symptoms as
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2006 
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2007
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2008
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2009
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2010
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2011
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2012
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2013
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2014
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2015
where symptom1 is not null and symptom2 is not null
union distinct
select distinct serialid,vaers_id, symptom1, symptom2
from dataset1.symptoms2016
where symptom1 is not null and symptom2 is not null


Redo join queries (eliminate year from old queries until we can get a year column in and eliminate conditions that rely on fields we chose to leave out, ie. dosage)

-- 1.  This query returns the vaccine manufacturer of all HIBV vaccines that caused "screaming" in and any additional symptoms. 

select v.vax_manu, s.*
from dataset1clean.Symptoms s left join dataset1clean.Vaccines v on s.vaers_id = v.vaers_id
where (v.vax_type = 'HIBV') and (s.symptom1 = 'Screaming')

-- 2.  This query produces all the drugs manufactured by Aventis Pasteur in 2007 that resulted in adverse symptoms that were not caused by the wrong drug being administered.

select s.*, v.vax_type
from dataset1clean.Symptoms s left join dataset1clean.Vaccines v on s.vaers_id = v.vaers_id 
where v.vax_manu = 'AVENTIS PASTEUR' and s.symptom1 != 'Wrong drug administered'

-- 3. This query gives all symptoms where patients had multiple symptoms (symptom2 is NOT null). 

select s.*
from dataset1clean.Symptoms s join dataset1clean.Vaccines v on s.vaers_id = v.vaers_id 
where s.symptom1 != 'Null' and s.symptom2 != 'Null'

-- 4.  This query returns the number of HIBV vaccines that had adverse events that with symptoms that were not null.

select v.vaers_id, v.vax_type, s.symptom1, s.symptom2
from dataset1clean.Symptoms s join dataset1clean.Vaccines v on s.vaers_id = v.vaers_id 
where v.vax_type = 'HIBV' and s.symptom1 != 'Null'
order by v.vaers_id

-- 5. This query returns vaccine information from vaccines that resulted in death and other not-null symptoms.
  
select s.symptom1,s.symptom2, v.*
from dataset1clean.Symptoms s right join dataset1clean.Vaccines v on s.vaers_id = v.vaers_id
where s.symptom1 = 'Death' and s.symptom2 != 'Null'

-- 6. This query returns all the symptoms information about adverse effects resulting from the MMRV vaccine that was manufactured by Merck & Co. Inc. 

select s.*
from dataset1clean.Symptoms s join dataset1clean.Vaccines v on s.vaers_id = v.vaers_id
where v.vax_manu = 'MERCK & CO. INC.' and v.vax_type = 'MMRV' and s.symptom1 != 'Null'





