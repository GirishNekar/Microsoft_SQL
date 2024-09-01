/*

@Author: Girish
@Date: 2024-08-22
@Last Modified by: Girish
@Last Modified time: 2024-08-22
@Title: CRUD Operations Of DataBase and its tables

*/


-- Creating a Databse

create database StudentPerformance;


-- Selecting the DataBase

use StudentPerformance;


-- Creating the table

create table Students(
	student_id int primary key,
	gender varchar(6) not null,
	age int not null,
	study_hour_per_day int not null

);


create table Students_info(
	std_usn int primary key,
	height int not null ,
	weight int not  null,
	student_id int ,
	foreign key (student_id) references Students(student_id)

);

insert into Students_info(std_usn,height,weight,student_id) 
values (1000,156,56,2),
	   (1001,154,52,2),
	   (1002,152,57,2),
	   (1003,156,47,2),
	   (1004,160,56,2);


-- Inserting One row to the Table we created

insert into Students(student_id,gender,age,study_hour_per_day) 
values (1,'male',16,5);


-- inserting multiple rows to the table

insert into Students(student_id,gender,age,study_hour_per_day) 
values (2,'male',16,7),
	   (3,'male',16,5),
	   (4,'male',16,8),
	   (5,'male',16,2),
	   (6,'male',17,4),
	   (7,'male',17,7),
	   (8,'male',17,2);


insert into Students(student_id,gender,age,study_hour_per_day) 
values (9,'female',16,7),
	   (10,'female',16,5),
	   (11,'female',15,8),
	   (12,'female',14,2),
	   (13,'female',17,4),
	   (14,'female',15,7),
	   (15,'female',17,2);



-- Selecting all the Columns to see them

select * from Students;

-- DML Operatoions
-- Updating the table 

update Students 
set study_hour_per_day = 10 where (student_id % 2 = 0);


--deleting the rows

delete from Students where student_id = 11;


-- deleting Column

alter table Students drop column gender;

-- adding the column

alter table Students add gender varchar(6);


-- setting up values to the gender

update Students 
set gender = case 
				when student_id % 2 = 0 then 'male'
				else 'female'
				end;
	
-- TCL Statements	
begin transaction;

insert into Students(student_id,gender,age,study_hour_per_day) 
values (28,'female',15,4);

commit;
select * from Students;
rollback;

-- truncate the table
truncate table Sudents;


-- deleting the table

drop table Students;


-- deleting the database

drop database StudentPerformance;