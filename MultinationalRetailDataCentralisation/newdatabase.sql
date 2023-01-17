
CREATE TABLE movies (
 employee_id INT UNIQUE NOT NULL ,
 employee_name  varchar(20) UNIQUE NOT NULL
);


CREATE TABLE movies (
 employee_id INT UNIQUE NOT NULL ,
 employee_name  varchar(20) UNIQUE NOT NULL
);

SELECT * FROM movies

INSERT INTO movies (employee_id, employee_name)
VALUES (1,'Mr.Pink'),
       (2,'Mr.Blonde'),
	   (3,'Mr.Orange'),
	   (4,'Mr.White'),
	   (5,'Mr.Brown'),
	   (6,'Eddie'),
	   (7,'Joe');
	   
	   
DELETE FROM movies
WHERE employee_id IN (4,5);


INSERT INTO movies (employee_id, employee_name)
VALUES (8, 'Mr.Blue');



ALTER TABLE movies
 ADD COLUMN salary INT ;

INSERT INTO movies (salary)
VALUES (50000),
	   (48000),
	   (65000),
	   (90000),
	   (120000),
	   (30000);










SELECT * FROM movies



UPDATE movies
SET salary = 50000
WHERE employee_id=1;

UPDATE movies
SET salary = 49000
WHERE employee_id=2;
