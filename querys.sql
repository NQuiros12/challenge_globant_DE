select count(0)
from hired_employees h
inner join jobs j 
on h.job_id = j.job_id
inner join departments d
on h.department_id = d.department_id
where year(h.datetime) in (2021)
group by j.job,d.department;
--Number of employees hired for each job and department in 2021 divided by quarter.
-- The table must be ordered alphabetically by department and job.
