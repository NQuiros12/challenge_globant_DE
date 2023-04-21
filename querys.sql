--Number of employees hired for each job and department in 2021 divided by quarter.
--The table must be ordered alphabetically by department and job.
select  d.department,
		j.job,
        QUARTER(h.`datetime`) as quarter_id,
        count(0) as hires
from hired_employees h
inner join jobs j 
    on h.job_id = j.job_id
inner join departments d
    on h.department_id = d.department_id
where year(h.datetime) = 2021
group by d.department,j.job,  QUARTER(h.`datetime`)
order by d.department,d.department ASC ;
--
