--creating a table employee_details
create table employee_details (emp_name varchar(20),Department varchar(20),salary int)

---inserting values into the table employee_details
insert into employee_details values('James','Sales',3000)

,('Michael','Sales',4600)

,('Robert','Sales',4100)

,('Maria','Finance',3000)

,('Raman','Finance',3000)

,('Scott','Finance',3300)

,('Jen','Finance',3900)

,('Jeff','Marketting',3000)

,('Kumar','Marketting',2000)

select * from employee_details

--selecting first row from each department which contains all details of particular employee
SELECT emp_name,department,salary FROM (
  SELECT emp_name, department, salary,
         ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary asc) AS row_num
  FROM employee_details
) subquery
WHERE row_num = 1;

---select * from employee_details where limit=1;
--selecting employee's who get highest salary from each department
SELECT emp_name,department,salary FROM (
  SELECT emp_name, department, salary,
         ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employee_details
) subquery
WHERE row_num = 1;

--Displaying avg salary in each department
SELECT department, avg(salary) as avg_salary
FROM employee_details
GROUP BY department;

--Displaying the total of all salaries in each department
SELECT department, sum(salary) as total_salary
FROM employee_details
GROUP BY department;

--Displaying minimum salary in each department
SELECT department, min(salary) as min_salary
FROM employee_details
GROUP BY department;

--Displaying maximum salary in each department
SELECT department, max(salary) as max_salary
FROM employee_details
GROUP BY department;