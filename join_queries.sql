-- 1.  This query returns the vaccine manufacturer of all HIBV vaccines that caused "screaming" in 2006 and any additional symptoms. 

select v6.vax_manu, s6.*
from dataset1.symptoms2006 s6 left join dataset1.vaccine2006 v6 on s6.vaers_id = v6.vaers_id
where (v6.vax_type = 'HIBV') and (s6.symptom1 = 'Screaming')

-- 2.  This query produces all the drugs manufactured by Aventis Pasteur in 2007 that resulted in adverse symptoms that were not caused by the wrong drug being administered.

select s7.*, v7.vax_type
from dataset1.symptoms2007 s7 left join dataset1.vaccine2007 v7 on s7.vaers_id = v7.vaers_id 
where v7.vax_manu = 'AVENTIS PASTEUR' and s7.symptom1 != 'Wrong drug administered	'


-- 3. This query gives all symptoms where patients had multiple symptoms (symptom2 and symptom 3 NOT null) and the dosage is at least 2. 

select s8.*
from dataset1.symptoms2007 s8 join dataset1.vaccine2007 v8 on s8.vaers_id = v8.vaers_id 
where v8.vax_dose > 1 and s8.symptom1 != 'Null' and s8.symptom2 != 'Null' and s8.symptom3 != 'Null'  

-- 4.  This query returns the number of HIBV vaccines that had adverse events in 2009 that with symptoms that were not null.

select v9.vaers_id, v9.vax_type, s9.symptom1, s9.symptom2, s9.symptom3
from dataset1.symptoms2009 s9 join dataset1.vaccine2009 v9 on s9.vaers_id = v9.vaers_id 
where v9.vax_type = 'HIBV' and s9.symptom1 != 'Null'
order by v9.vaers_id

-- 5. This query returns vaccine information from vaccines that resulted in death and other not-null symptoms.
  
select s10.symptom1,s10.symptom2,s10.symptom3, v10.*
from dataset1.symptoms2010 s10 right join dataset1.vaccine2010 v10 on s10.vaers_id = v10.vaers_id
where s10.symptom1 = 'Death' and s10.symptom2 != 'Null'

-- 6. This query returns all the symptoms information about adverse effects resulting from the MMRV vaccine that was manufactured by Merck & Co. Inc in 2011. 

select s11.*
from dataset1.symptoms2011 s11 join dataset1.vaccine2011 v11 on s11.vaers_id = v11.vaers_id
where v11.vax_manu = 'MERCK & CO. INC.' and v11.vax_type = 'MMRV' and s11.symptom1 != 'Null'

