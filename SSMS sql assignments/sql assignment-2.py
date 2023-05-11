--creating table fruit_details
create table fruit_details(product varchar(20),amount int, Country varchar(20) )

--inserting values into the fruit_details table
insert into fruit_details values('Banana', 1000, 'USA')

,('Carrots', 1500, 'India')

,('Beans', 1600, 'Sweden'),

('Orange', 2000, 'UK')

,('Orange', 2000, 'UAE')

,('Banana', 400, 'China')

,('Carrots', 1200, 'China')

--pivot columns operation on the table fruit_table
SELECT * from fruit_details
pivot( sum(amount) for Country in (USA,India,Sweden,UK,UAE,China)) as PivotTable