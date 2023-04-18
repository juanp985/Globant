## Challange #2

## a. Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job

SELECT
    d.department_name,
    j.job_name,
    QUARTER(he.datetime) AS quarter,
    COUNT(*) AS hired_employee_count
FROM hired_employees he
	INNER JOIN departments d ON 
		he.department_id = d.department_id
	INNER JOIN Jobs j ON 
		he.job_id = j.job_id
WHERE YEAR(he.datetime) = 2021
GROUP BY
    d.department_name,
    j.job_name,
    QUARTER(he.datetime)
ORDER BY
    d.department_name,
    j.job_name;


## b. List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered
## by the number of employees hired (descending).

SELECT
    d.department_id,
    d.department_name,
    COUNT(he.employee_id) AS hired_employee_count
FROM hired_employees he
	INNER JOIN departments d ON 
		he.department_id = d.department_id
WHERE
    YEAR(he.datetime) = 2021
GROUP BY
    d.department_id,
    d.department_name
HAVING
    COUNT(he.employee_id) > (SELECT AVG(sub.total_hired) FROM (
                               SELECT COUNT(*) AS total_hired
                               FROM HiredEmployees
                               WHERE YEAR(datetime) = 2021
                               GROUP BY department_id
                             ) AS sub)
ORDER BY
    hired_employee_count DESC;
