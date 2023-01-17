SELECT title, release_year, description,length FROM film;
SELECT * FROM customer
LIMIT 30;

SELECT first_name, last_name, email FROM customer
LIMIT 30;

SELECT * FROM payment
ORDER BY amount DESC
LIMIT 10;

SELECT title FROM film
ORDER BY length 
LIMIT 15;

SELECT title FROM film
ORDER BY title, rental_rate DESC 
LIMIT 100;


SELECT title FROM film
ORDER BY rental_rate DESC ,title
LIMIT 100;


SELECT * FROM film
WHERE length < 120 
LIMIT 10;

SELECT * FROM film 
WHERE rating ='G'
ORDER BY length DESC 
LIMIT 10;

SELECT * FROM payment
WHERE amount > 10;

SELECT title, (replacement_cost/length) AS rental_rate_per_day
FROM film;

SELECT title, (rental_rate/rental_duration) AS rental_rate_per_hour
FROM film
ORDER BY rental_rate_per_hour DESC
LIMIT 10;

SELECT * FROM city 
WHERE city LIKE 'al%'
OR city LIKE 'Al%';


SELECT first_name FROM actor 
WHERE first_name NOT LIKE '%EN';



SELECT first_name FROM actor 
WHERE first_name NOT LIKE '%EN'
AND actor_id <100;


SELECT * FROM actor 
WHERE first_name NOT LIKE '%EN'
AND actor_id <100
AND last_name NOT LIKE '%D'

