--VACCINES QUERIES
--Milestone #2

--vaccines2006.csv
--investigating which types of vax Organon Teknika produced in 2006 // only BCG (TB)

select *
from dataset1.vaccine2006
where UPPER(vax_manu) = 'ORGANON-TEKNIKA'
order by vax_type

--vaccines2007.csv
--investigating which types of vax Sanofi Pasteur produced in 2007 // there are many

select *
from dataset1.vaccine2007
where UPPER(vax_manu) = 'SANOFI PASTEUR'
order by vax_type

--vaccines2008.csv
-- investigate how many vax Sanofi Pasteur produced in 2008 // 100

select *
from dataset1.vaccine2008
where UPPER(vax_manu) = 'SANOFI PASTEUR'
order by vax_type

--vaccines2009.csv
--investigate vax site for all HIBV vaccines

select *
from dataset1.vaccine2009
where UPPER(vax_type) = 'HIBV'
order by vax_site

--vaccines2010.csv
--how many vax did Sanofi Pasteur produce in 2009 // ~13,000

select *
from dataset1.vaccine2010
where UPPER(vax_manu) = 'SANOFI PASTEUR'
order by vax_type

--vaccines2011.csv
-- how many vaccines by Sanofi have a vaccination route of UN

select *
from dataset1.vaccine2011
where UPPER(vax_manu) = 'SANOFI PASTEUR' and UPPER(vax_route) = 'UN'
order by vax_site

--vaccines2012.csv
--investigate TDAP vaccines that have a vax_route of 'UN'

select *
from dataset1.vaccine2012
where UPPER(vax_type) = 'TDAP' and UPPER(vax_route) = 'UN'
order by vax_site

--vaccines2013.csv
--investigate Merck vax with IM vax route

select *
from dataset1.vaccine2013
where UPPER(vax_manu) = 'MERCK & CO. INC.' and UPPER(vax_route) = 'IM'
order by vax_site

--vaccines2014.csv
--how many HEPA vaccines did Merck output in 2014 VS. 2015? (part a)

select *
from dataset1.vaccine2014
where UPPER(vax_manu) = 'MERCK & CO. INC.' and UPPER(vax_type) = 'HEPA'
order by vax_site

--vaccines2015.csv
-- how many HEPA vaccines did Merck output in 2014 VS. 2015? (part b)

select *
from dataset1.vaccine2015
where UPPER(vax_manu) = 'MERCK & CO. INC.' and UPPER(vax_type) = 'HEPA'
order by vax_site

--vaccines2016.csv
-- how many state required zoster vaccines did Merck output in 2016?

select *
from dataset1.vaccine2016
where UPPER(vax_manu) = 'MERCK & CO. INC.' and UPPER(vax_name) = 'ZOSTER (ZOSTAVAX)'
order by vax_site

--VACCINES SYMPTOMS


--symptoms2006.csv
-- This query selects all the vaccine events in 2006 that resulted in pain as the primary -- symptoms and is ordered by the id of that event (vaers_id).
select * 
from dataset1.symptoms2006
where UPPER(symptom1) = 'PAIN'
order by vaers_id
 
--symptoms2007.csv
-- This query identifies all the vaccine events in 2007 that simply had the wrong drug
-- administered and caused the vaccine averse event. Each row returned had all "null" 
-- values as the symptoms2 and symptoms3 fields.
select * 
from dataset1.symptoms2007
where UPPER(symptom1) = 'WRONG DRUG ADMINISTERED'
order by vaers_id

--symptoms2008.csv
-- This query returns all vaccine events in 2008 that only had one symptom reported in the -- symptom1 field. It is ordered by the serial id of the specific event.
select * 
from dataset1.symptoms2008
where NOT(symptom2) = 'Null'
order by serialid

--symptoms2009.csv
-- This query identifies all vaccine events that had the terribly misspelled "diarrhoea"
-- listed as a symptom and orders each event by the associated ID.
select * 
from dataset1.symptoms2009
where UPPER(symptom1) = 'DIARRHOEA'
order by serialid

--symptoms2010.csv
-- This query selects all vaccine events that listed "crying" as the primary symptom. The --list is ordered by the second symptom, most of which were similar symptoms such as      -- "distress".
select * 
from dataset1.symptoms2010
where UPPER(symptom1) = 'CRYING'
order by symptom2


--symptoms2011.csv
-- This is a more specific query than the one for 2010, and lists events that have "crying -- and "feeling abnormal" as primary and secondary, respectively, symptoms. It is ordered 
-- by the vaers_id to identify if these patterns occur with any specific drugs.
select * 
from dataset1.symptoms2011
where UPPER(symptom1) = 'CRYING' 
and UPPER(symptom2) = 'FEELING ABNORMAL'
order by vaers_id

--symptoms2012.csv
-- This query identifies all the entries in 2012 that resulted in paralysis as the only
- symptom. It is organized by the ID number of each specific event.
select * 
from dataset1.symptoms2012
where UPPER(symptom1) = 'PARALYSIS' 
and not(symptom2) = 'null'
order by serialid


--symptoms2013.csv
-- This query selects all entries from 2013 that resulted in a loss of consciousness as   -- any symptom.  It is ordered by the vaers_id in order to trace this back to a specific -- drug.
select * 
from dataset1.symptoms2013
where UPPER(symptom1) = 'LOSS OF CONSCIOUSNESS' 
or UPPER(symptom2) = 'LOSS OF CONSCIOUSNESS'
or UPPER(symptom3) = 'LOSS OF CONSCIOUSNESS'
order by vaers_id

--symptoms2014.csv
-- This query identifies all cases of incorrect drug storage in 2014 as the only "symptom" -- in each case. It is ordered by the event ID number (serialid). 
select * 
from dataset1.symptoms2013
where UPPER(symptom1) = 'INCORRECT STORAGE OF DRUG' 
and not(symptom2) = 'null' 
order by serialid

--symptoms2015.csv
-- This query returns all entries that resulted in "herpes zoster" as the primary symptom -- and a rash as the secondary symptom, which is likely caused by the herpes. It is
-- ordered by the vaers_id to identify any specific drugs that could cause herpes. 
select * 
from dataset1.symptoms2015
where UPPER(symptom1) = 'HERPES ZOSTER'
and UPPER(symptom2) = 'RASH'
order by vaers_id

--symptoms2016.csv
-- This query selects all the cases that resulted in vomiting projectiles as the primary 
-- and orders it by the symptom version (numerical value). This is interesting because
-- all the "vomiting projectile" symptoms did not have the same symptom version number.
select *
from dataset1.symptoms2015
where UPPER(symptom1) = 'VOMITING PROJECTILE'
order by symptomversion1

