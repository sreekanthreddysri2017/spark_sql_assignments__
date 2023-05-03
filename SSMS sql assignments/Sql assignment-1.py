/*creating a table called emp*/
create table emp(firstName varchar(30),middleName varchar(30),lastName varchar(30),dob int, gender char(1),salary int )

/*inserting values into the table*/
insert into emp values('James','','Smith',03011998,'M',3000),
('Michael','Rose','',10011998,'M',20000),
('Robert','','Williams',02012000,'M',3000),
('Maria','Annie','Jones',03011998,'F',11000),
('Jen','Mary','Brown',04101998,'F',10000);

select * from emp order by salary desc

--selecting firstName,lastName,Salary column from the table emp

select firstName,lastName,salary from emp;

--adding new column Country,Department,Age to the table emp
alter table emp add Department varchar(30),age int;
select * from emp

--changing the datatype of dob and salary column to string
alter table emp alter column dob varchar(10)
alter table emp alter column salary varchar(10)

alter table emp alter column salary int

--changing name of the column

exec sp_RENAME 'emp.firstName','firstPosition','column'
exec sp_RENAME 'emp.middleName','secondPosition','column'
exec sp_RENAME 'emp.lastName','thirdPosition','column'

SELECT firstname
FROM emp
WHERE salary = (
  SELECT Max(salary) FROM emp
);
--distinct values in dob,salary
select distinct dob from emp;
select distinct salary from emp;
---dropping department,age columns
alter table emp drop column Department;
alter table emp drop column age;

--printing the firstName,middleName,lastName of the emp who is getting the highest salary


select firstPosition,secondPosition,thirdPosition from emp where salary in (select max(salary) from emp)
select * from information_schema.columns where table_name='emp'