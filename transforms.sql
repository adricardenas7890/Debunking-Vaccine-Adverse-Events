--Section 1: Create new modeled tables

--*** ADD YEAR AS A FIELD FOR EACH TABLE BEFORE UNION

--
--Symptoms 2006-2016
--CLEAN DATA FORMAT
--This will return the columns from each table that will be needed for analysis, remove duplicates, and remove null data. This is the format for the union
--Fields: serialid, vaers_id, symptom1, symptom2

select distinct serialid, vaers_id, symptom1, symptom2
from dataset1.symptoms2006
where symptom1 is not null and symptom2 is not null
order by serialid

--TABLE VaersID_Symptom
--junction table with vaers_id, symptom1, symptom2

select distinct vaers_id, symptom1, symptom2, "2006" as year
from dataset1.symptoms2006
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2007" as year
from dataset1.symptoms2007
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2008" as year
from dataset1.symptoms2008
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2009" as year
from dataset1.symptoms2009
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2010" as year
from dataset1.symptoms2010
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2011" as year
from dataset1.symptoms2011
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2012" as year
from dataset1.symptoms2012
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2013" as year
from dataset1.symptoms2013
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2014" as year
from dataset1.symptoms2014
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2015" as year
from dataset1.symptoms2015
where symptom1 is not null and symptom2 is not null
union distinct
select distinct vaers_id, symptom1, symptom2, "2016" as year
from dataset1.symptoms2016
where symptom1 is not null and symptom2 is not null

--Table Symptom
--union of all unique symptoms for each vaers_id
create table dataset1clean.vaersid_symptom as
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2006
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2006
where symptom2 is not null
union distinct 
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2007
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2007
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2008
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2008
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2009
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2009
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2010
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2010
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2011
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2011
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2012
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2012
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2013
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2013
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2014
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2014
where symptom2 is not null
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2015
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2015
union distinct
select distinct vaers_id, symptom1 symptom
from dataset1.symptoms2016
where symptom1 is not null
union distinct
select distinct vaers_id, symptom2 symptom
from dataset1.symptoms2016
where symptom2 is not null

--
--Vaccines 2006-2016
--CLEAN DATA FORMAT
--This will return the columns from each table that will be need for analysis, remove duplicates, and remove null data. This is the format for the union
--Fields: serialid, vaers_id, vax_name, vax_type, vax_manu, vax_route

select distinct serialid, vaers_id, vax_name, vax_type, vax_manu, vax_route
from dataset1.vaccine2006
where vax_route is not null and vax_name is not null and vax_manu is not null
order by serialid

--UNION QUERY

select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2006" as year
from dataset1.vaccine2006
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2007" as year
from dataset1.vaccine2007
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2008" as year
from dataset1.vaccine2008
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2009" as year
from dataset1.vaccine2009
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2010" as year
from dataset1.vaccine2010
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2011" as year
from dataset1.vaccine2011
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2012" as year
from dataset1.vaccine2012
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2013" as year
from dataset1.vaccine2013
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2014" as year
from dataset1.vaccine2014
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2015" as year
from dataset1.vaccine2015
where vax_route is not null and vax_name is not null and vax_manu is not null
union distinct
select distinct vaers_id, vax_name, vax_type, vax_manu, vax_route, "2016" as year
from dataset1.vaccine2016
where vax_route is not null and vax_name is not null and vax_manu is not null


--3. Choose primitive data type most precise for domain
--Symptom
--All fields are ideal primitive data type and do not have to be cast
--Fields: symptom_id(Int), vaers_id(Int), symptom1(Str), symptom2(Str)

-- Vaccine
-- All fields are ideal primitive data type and do not have to be cast
--Fields: vaccine_id(Int), vaers_id(Int), vax_name(Str), vax_type(Str), vax_manu(Str), vax_route(Str)



--Milestone #6
--Adding primary key to transformed symptom and vaccine table

-- this query builds in a primary key called row into the symptom table and orders it by the row number 
create table dataset1clean.symptom_PK as
select row_number() over (order by vaers_id asc) as row,
vaers_id, symptom1, symptom2, year from dataset1clean.symptom
order by row

-- this query does the same as above but for the vaccine table
create table dataset1clean.vaccine_PK as
select row_number() over (order by vaers_id asc) as row,
vaers_id, vax_name, vax_type, vax_manu, vax_route, year from dataset1clean.vaccine
order by row
