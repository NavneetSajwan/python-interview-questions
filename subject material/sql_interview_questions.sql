/*markdown
## Nth highest salary
*/

/*markdown

*/

show DATABASES;

create database interview;

use interview

/*markdown
## Question 1
*/

/*markdown
Given a table of candidates and their skills, you're tasked with finding the candidates best suited for an open Data Science job. You want to find candidates who are proficient in Python, Tableau, and PostgreSQL.
Write a SQL query to list the candidates who possess all of the required skills for the job. Sort the the output by candidate ID in ascending order.
Assumption:
There are no duplicates in the candidates table.
*/

-- drop table
drop table interview.candidates;

-- Create candidates table
CREATE TABLE interview.candidates (
    candidate_id INTEGER,
    skill VARCHAR(50)
);

-- Insert dummy data
INSERT INTO candidates (candidate_id, skill)
VALUES
    (123, 'Python'),
    (123, 'Tableau'),
    (123, 'PostgreSQL'),
    (234, 'R'),
    (234, 'PowerBI'),
    (234, 'SQL Server'),
    (345, 'Python'),
    (345, 'Tableau');


select * from interview.candidates;

use interview;

-- solution 1;

select  
candidate_id
from interview.candidates
where skill in ('Python', 'Tableau', 'PostgreSQL')
group by candidate_id
having count(skill)>=3

/*markdown
## Question 2:
*/

/*markdown
Tesla is investigating bottlenecks in their production, and they need your help to extract the relevant data. Write a SQL query that determines which parts have begun the assembly process but are not yet finished.
Assumption
Table `parts_assembly` contains all parts in production.
*/

drop table interview.parts_assembly;

create table interview.parts_assembly (
    part VARCHAR(50),
    finish_date datetime,
    assembly_step INTEGER
);

insert into interview.parts_assembly (part, finish_date, assembly_step) values ('battery', STR_TO_DATE('01/22/2022 00:00:00', '%m/%d/%Y %H:%i:%s'), 1);
insert into interview.parts_assembly (part, finish_date, assembly_step) values ('battery', STR_TO_DATE('01/22/2022 00:00:00', '%m/%d/%Y %H:%i:%s'), 2);
insert into interview.parts_assembly (part, finish_date, assembly_step) values ('battery', STR_TO_DATE('01/22/2022 00:00:00', '%m/%d/%Y %H:%i:%s'), 3);
insert into interview.parts_assembly (part, finish_date, assembly_step) values ('bumper', STR_TO_DATE('01/22/2022 00:00:00', '%m/%d/%Y %H:%i:%s'), 1);
insert into interview.parts_assembly (part, finish_date, assembly_step) values ('bumper', STR_TO_DATE('01/22/2022 00:00:00', '%m/%d/%Y %H:%i:%s'), 2);
insert into interview.parts_assembly (part,  assembly_step) values ('bumper', 3);
insert into interview.parts_assembly (part,  assembly_step) values ('bumper', 4);


select * from interview.parts_assembly

with cte as (
    select part, max(assembly_step)
from interview.parts_assembly
group by part
)

select distinct(part) from interview.parts_assembly
where finish_date is NULL
and (part, assembly_step) in (select part, assembly_step from cte)

-- Solution 2:
SELECT *
FROM interview.parts_assembly
WHERE (part, assembly_step) IN (
    SELECT part, MAX(assembly_step) AS max_column
    FROM interview.parts_assembly
    GROUP BY part
)
and finish_date is null
-- Learning:

-- Trick to get an entire row of a particular group

/*markdown
## Question 3:
*/

/*markdown
Assume that you are given the table below containing information on viewership by device type (where the three types are laptop, tablet, and phone). Define “mobile” as the sum of tablet and phone viewership numbers. Write a query to compare the viewership on laptops versus mobile devices.
Output the total viewership for laptop and mobile devices in the format of "laptop_views" and "mobile_views".
*/

-- Create viewership table
CREATE TABLE interview.viewership (
    user_id INT,
    device_type VARCHAR(20),
    view_time TIMESTAMP
);

-- Insert dummy data
INSERT INTO interview.viewership (user_id, device_type, view_time) VALUES
(123, 'tablet', '2022-01-02 00:00:00'),
(125, 'laptop', '2022-01-07 00:00:00'),
(128, 'laptop', '2022-02-09 00:00:00'),
(129, 'phone', '2022-02-09 00:00:00'),
(145, 'tablet', '2022-02-24 00:00:00');


select * from interview.viewership;

-- How to split a single col into multiple cols

SELECT
sum(case when device_type="laptop" then 1 else 0 end) as laptop_views,
sum(case when device_type="tablet" or device_type="phone" then 1 else 0 end) as mobile_views
from 
interview.viewership;

/*markdown
## Question 4:
*/

/*markdown

You are given the tables below containing information on Robinhood trades and users. Write a query to list the top three cities that have the most completed trade orders in descending order.
*/

-- Create users table
CREATE TABLE users (
    user_id INT,
    city VARCHAR(50),
    email VARCHAR(255),
    signup_date DATETIME
);

-- Insert dummy data into users table
INSERT INTO users (user_id, city, email, signup_date) VALUES
(111, 'San Francisco', 'rrok10@gmail.com', '2021-08-03 12:00:00'),
(148, 'Boston', 'sailor9820@gmail.com', '2021-08-20 12:00:00'),
(178, 'San Francisco', 'harrypotterfan182@gmail.com', '2022-01-05 12:00:00'),
(265, 'Denver', 'shadower_@hotmail.com', '2022-02-26 12:00:00'),
(300, 'San Francisco', 'houstoncowboy1122@hotmail.com', '2022-06-30 12:00:00');

-- Create trades table
CREATE TABLE trades (
    order_id INT,
    user_id INT,
    price DECIMAL(10, 2),
    quantity INT,
    status VARCHAR(20),
    timestamp DATETIME
);

-- Insert dummy data into trades table
INSERT INTO trades (order_id, user_id, price, quantity, status, timestamp) VALUES
(100101, 111, 9.80, 10, 'Cancelled', '2022-08-17 12:00:00'),
(100102, 111, 10.00, 10, 'Completed', '2022-08-17 12:00:00'),
(100259, 148, 5.10, 35, 'Completed', '2022-08-25 12:00:00'),
(100264, 148, 4.80, 40, 'Completed', '2022-08-26 12:00:00'),
(100305, 300, 10.00, 15, 'Completed', '2022-09-05 12:00:00'),
(100400, 178, 9.90, 15, 'Completed', '2022-09-09 12:00:00'),
(100565, 265, 25.60, 5, 'Completed', '2022-12-19 12:00:00');


select * from interview.trades;

select * from interview.users

/*markdown
#### Solution 4:
*/

-- Optionn1:
select city,
count(order_id) as total_orders
from
(select 
ct.order_id,
ct.user_id,
u.city
from
(
    select * from trades where status="Completed"
) ct
JOIN
users u
on
ct.user_id=u.user_id
) sub
group by city
order by total_orders desc
limit 3

-- Option 2:

SELECT
    u.city,
    COUNT(t.order_id) AS total_orders
FROM
    users u
JOIN
    trades t ON u.user_id = t.user_id
WHERE
    t.status = 'Completed'
GROUP BY
    u.city
ORDER BY
    total_orders DESC
LIMIT 3;

/*markdown
## Question 5:
*/

/*markdown
Assume you are given the table below that shows job postings for all companies on the LinkedIn platform. Write a query to get the number of companies that have posted duplicate job listings (two jobs at the same company with the same title and description).
*/

-- drop table job_listings

-- Create job_listings table
CREATE TABLE job_listings (
    job_id INT,
    company_id INT,
    title VARCHAR(255),
    description TEXT
);

-- Insert example data into job_listings table
INSERT INTO job_listings (job_id, company_id, title, description) VALUES
(248, 827, 'Business Analyst', 'Business analyst evaluates past and current business data with the primary goal of improving decision-making processes within organizations.'),
(149, 845, 'Business Analyst', 'Business analyst evaluates past and current business data with the primary goal of improving decision-making processes within organizations.'),
(945, 345, 'Data Analyst', 'Data analyst reviews data to identify key insights into a businesss customers and ways the data can be used to solve problems.'),
(164, 345, 'Data Analyst', 'Data analyst reviews data to identify key insights into a businesss customers and ways the data can be used to solve problems.'),
(172, 244, 'Data Engineer', 'Data engineer works in a variety of settings to build systems that collect, manage, and convert raw data into usable information for data scientists and business analysts to interpret.'),
(190, 244, 'Data Engineer', 'Data engineer works in a variety of settings to build systems that collect, manage, and convert raw data into usable information for data scientists and business analysts to interpret.');

select * from interview.job_listings;

select
count(*)
from
(select 
company_id,
title
FROM
interview.job_listings
group by company_id, title
having count(*)> 1) sub

-- Solution 5:
SELECT count(DISTINCT company_id) AS duplicate_companies
FROM job_listings
WHERE (company_id, title, description) IN (
    SELECT company_id, title, description
    FROM job_listings
    GROUP BY company_id, title, description
    HAVING COUNT(*) > 1
);

/*markdown
## Question 6:
*/

/*markdown
Given a table of bank deposits and withdrawals, return the final balance for each account.
Assumption:
All the transactions performed for each account are present in the table; no transactions are missing.
*/

CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    account_id INT,
    transaction_type VARCHAR(255),
    amount DECIMAL(10, 2)
);

-- Inserting example data
INSERT INTO transactions (transaction_id, account_id, transaction_type, amount) VALUES
(123, 101, 'Deposit', 10.00),
(124, 101, 'Deposit', 20.00),
(125, 101, 'Withdrawal', 5.00),
(126, 201, 'Deposit', 20.00),
(128, 201, 'Withdrawal', 10.00);


select * from interview.transactions;

SELECT
account_id,
sum(case when transaction_type="Deposit" then amount else amount * -1 end ) as balance
from transactions
group by account_id

/*markdown
## Questions 9:
*/

/*markdown
The LinkedIn Creator team is looking for power creators who use their personal profile as a company or influencer page. If someone's LinkedIn page has more followers than the company they work for, we can safely assume that person is a power creator.
Write a query to return the IDs of these LinkedIn power creators ordered by the IDs.
Assumption:
Each person with a LinkedIn profile in this database works at one company only.
*/

-- Creating personal_profiles table
CREATE TABLE personal_profiles (
    profile_id INT PRIMARY KEY,
    name VARCHAR(255),
    followers INT,
    employer_id INT
);

-- Inserting example data into personal_profiles table
INSERT INTO personal_profiles (profile_id, name, followers, employer_id) VALUES
(1, 'Nick Singh', 92000, 4),
(2, 'Zach Wilson', 199000, 2),
(3, 'Daliana Liu', 171000, 1),
(4, 'Ravit Jain', 107000, 3),
(5, 'Vin Vashishta', 139000, 6),
(6, 'Susan Wojcicki', 39000, 5);

-- Creating company_pages table
CREATE TABLE company_pages (
    company_id INT PRIMARY KEY,
    name VARCHAR(255),
    followers INT
);

-- Inserting example data into company_pages table
INSERT INTO company_pages (company_id, name, followers) VALUES
(1, 'The Data Science Podcast', 8000),
(2, 'Airbnb', 700000),
(3, 'The Ravit Show', 6000),
(4, 'DataLemur', 200),
(5, 'YouTube', 16000000),
(6, 'DataScience.Vin', 4500);


select * from personal_profiles;

select * from company_pages;

-- Solution 9:

SELECT
p.profile_id
FROM
personal_profiles p 
JOIN
company_pages c
on p.employer_id=c.company_id
where p.followers > c.followers

/*markdown
## Question 10:
*/

/*markdown
Assume that you are given the table below containing information on various orders made by eBay customers. Write a query to obtain the user IDs and number of products purchased by the top 3 customers; these customers must have spent at least $1,000 in total.
Output the user id and number of products in descending order. To break ties (i.e., if 2 customers both bought 10 products), the user who spent more should take precedence.
*/

drop table interview.user_transactions;

CREATE TABLE interview.user_transactions (
    transaction_id INT,
    product_id INT,
    user_id INT,
    spend DECIMAL(10, 2)
);

INSERT INTO interview.user_transactions (transaction_id, product_id, user_id, spend)
VALUES
    (131432, 1324, 128, 699.78),
    (131433, 1313, 128, 501.00),
    (153853, 2134, 102, 1001.20),
    (247826, 8476, 133, 1051.00),
    (247265, 3255, 133, 1474.00),
    (136496, 3677, 133, 247.56),
    (136497, 3678, 134, 247.56),
    (136498, 3679, 134, 247.56);
    

select * from interview.user_transactions;

SELECT
user_id,
sum(spend) total_spend,
count(transaction_id) total_transactions
from interview.user_transactions
group by user_id
having total_spend>1000
order by total_spend desc, total_transactions DESC
limit 3


-- Solution 10:
select 
user_id,
count(user_id) product_sum
from user_transactions
group by user_id
having sum(spend)>1000
order by product_sum DESC

/*markdown
## Question 12
*/

/*markdown
Microsoft Azure's capacity planning team wants to understand how much data its customers are using, and how much spare capacity is left in each of it's data centers. You’re given three tables: customers, datacenters, and forecasted_demand.
Write a query to find the total monthly unused server capacity for each data center. Output the data center id in ascending order and the total spare capacity.
P.S. If you've read the Ace the Data Science Interview and liked it, consider writing us a review?
*/

-- Create customers table
CREATE TABLE customers (
    customer_id INT,
    name VARCHAR(255)
);

-- Insert example data into customers table
INSERT INTO customers (customer_id, name)
VALUES
    (144, 'Florian Simran'),
    (109, 'Esperanza A. Luna'),
    (852, 'Garland Acacia');

-- Create datacenters table
CREATE TABLE datacenters (
    datacenter_id INT,
    name VARCHAR(255),
    monthly_capacity INT
);

-- Insert example data into datacenters table
INSERT INTO datacenters (datacenter_id, name, monthly_capacity)
VALUES
    (1, 'London', 100),
    (3, 'Amsterdam', 250),
    (4, 'Hong Kong', 400);

-- Create forecasted_demand table
CREATE TABLE forecasted_demand (
    customer_id INT,
    datacenter_id INT,
    monthly_demand INT
);

-- Insert example data into forecasted_demand table
INSERT INTO forecasted_demand (customer_id, datacenter_id, monthly_demand)
VALUES
    (109, 4, 120),
    (144, 3, 60),
    (144, 4, 105),
    (852, 1, 60),
    (852, 3, 178);

select * from interview.customers;

select * from interview.datacenters;

select * from interview.forecasted_demand;


select 
DC.datacenter_id,
DC.monthly_capacity,
sub.md_per_datacenter,
DC.monthly_capacity- sub.md_per_datacenter storage_left
FROM
interview.datacenters DC
JOIN
(
select 
datacenter_id,
sum(monthly_demand) as md_per_datacenter
from interview.forecasted_demand
group by datacenter_id
) sub
on sub.datacenter_id=DC.datacenter_id


select 
f.datacenter_id,
d.monthly_capacity -  f.demand_per_center
from
(
select 
datacenter_id,
sum(monthly_demand) as demand_per_center
from  forecasted_demand
group by datacenter_id
) f
JOIN
datacenters d 
ON f.datacenter_id=d.datacenter_id

/*markdown

## Question 13:
*/

/*markdown
Given a table of Facebook posts, for each user who posted at least twice in 2021, write a query to find the number of days between each user’s first post of the year and last post of the year in the year 2021. Output the user and number of the days between each user's first and last post.
*/

*/

CREATE TABLE posts (
    user_id INT,
    post_id INT,
    post_date TIMESTAMP,
    post_content TEXT
);

INSERT INTO posts (user_id, post_id, post_date, post_content)
VALUES
    (151652, 599415, '2021-07-10 12:00:00', 'Need a hug'),
    (661093, 624356, '2021-07-29 13:00:00', 'Bed. Class 8-12. Work 12-3. Gym 3-5 or 6. Then class 6-10. Another day that\'s gonna fly by. I miss my girlfriend'),
    (004239, 784254, '2021-07-04 11:00:00', 'Happy 4th of July!'),
    (661093, 442560, '2021-07-08 14:00:00', 'Just going to cry myself to sleep after watching Marley and Me.'),
    (151652, 111766, '2021-07-12 19:00:00', 'I\'m so done with covid - need travelling ASAP!');


select * from interview.posts

select
user_id,
DATEDIFF(max(post_date), min(post_date)) as days
FROM
posts
group by user_id
having count(user_id) > 1

/*markdown
## Question 14:
*/

/*markdown
Write a query to find the top 2 power users who sent the most messages on Microsoft Teams in August 2022. Display the IDs of these 2 users along with the total number of messages they sent. Output the results in descending count of the messages.
*/

/*markdown
Assumption:
No two users has sent the same number of messages in August 2022.
*/

CREATE TABLE messages (
    message_id INT,
    sender_id INT,
    receiver_id INT,
    content VARCHAR(255),
    sent_date DATETIME
);

INSERT INTO messages (message_id, sender_id, receiver_id, content, sent_date)
VALUES
    (901, 3601, 4500, 'You up?', '2022-08-03 00:00:00'),
    (902, 4500, 3601, 'Only if youre buying', '2022-08-03 00:00:00'),
    (743, 3601, 8752, 'Lets take this offline', '2022-06-14 00:00:00'),
    (922, 3601, 4500, 'Get on the call', '2022-08-10 00:00:00');


select * from interview.messages;

select 
sender_id,
count(message_id) as total
from messages
where month(sent_date)=8 and year(sent_date)=2022
group by sender_id

/*markdown

## Question 15: Two way relatinship
*/

/*markdown
You are given a table of PayPal payments showing the payer, the recipient, and the amount paid. A two-way unique relationship is established when two people send money back and forth. Write a query to find the number of two-way unique relationships in this data.
*/

*/

CREATE TABLE payments (
    payer_id INT,
    recipient_id INT,
    amount INT
);


-- Inserting example data into the payments table
INSERT INTO payments (payer_id, recipient_id, amount) VALUES
(101, 201, 30),
(201, 101, 10),
(101, 301, 20),
(301, 101, 80),
(201, 301, 70);


select * from interview.payments;

select 
lp.payer_id,
lp.recipient_id
FROM 
interview.payments lp 
join 
interview.payments rp 
on
lp.recipient_id=rp.payer_id
where lp.payer_id=rp.recipient_id

select 
*
FROM
interview.payments
where (payer_id, recipient_id) in
(
select
p1.payer_id, p1.recipient_id
from interview.payments p1
JOIN interview.payments p2 on p1.payer_id=p2.recipient_id and p1.recipient_id=p2.payer_id
)
and payer_id<recipient_id
-- where payer_id=recipient_id

SELECT 
COUNT(DISTINCT CONCAT(payer_id, '-', recipient_id)) AS unique_relationships
FROM interview.payments
WHERE (payer_id, recipient_id) IN (SELECT recipient_id, payer_id FROM interview.payments)
  AND payer_id < recipient_id;

/*markdown
## Question 17
*/

Google marketing managers are analyzing the performance of various advertising accounts over the last month. They need your help to gather the relevant data.
Write a query to calculate the return on ad spend (ROAS) for each advertiser across all ad campaigns. Round your answer to 2 decimal places, and order your output by the advertiser_id.
Hint: ROAS = Ad Revenue / Ad Spend

*/

-- Create ad_campaigns table
CREATE TABLE ad_campaigns (
    campaign_id INTEGER,
    spend INTEGER,
    revenue FLOAT,
    advertiser_id INTEGER
);

-- Insert example data into ad_campaigns table
INSERT INTO ad_campaigns (campaign_id, spend, revenue, advertiser_id)
VALUES
    (1, 5000, 7500, 3),
    (2, 1000, 900, 1),
    (3, 3000, 12000, 2),
    (4, 500, 2000, 4),
    (5, 100, 400, 4);


select * from ad_campaigns;

select
advertiser_id,
round(sum(revenue)/ sum(spend), 2) roas 
from ad_campaigns
group by advertiser_id
order by advertiser_id


/*markdown
## Question 18
Visa is trying to analyze its Apply Pay partnership. Calculate the total transaction volume for each merchant where the transaction was performed via Apple Pay.
Output the merchant ID and the total transactions by merchant. For merchants with no Apple Pay transactions, output their total transaction volume as 0.
Display the result in descending order of transaction volume.

*/

-- Create transactions table
CREATE TABLE merchant_transactions (
    merchant_id INTEGER,
    transaction_amount INTEGER,
    payment_method VARCHAR(50)
);



-- Insert example data into transactions table
INSERT INTO merchant_transactions (merchant_id, transaction_amount, payment_method)
VALUES
    (1, 600, 'Contactless Chip'),
    (1, 850, 'Apple Pay'),
    (1, 500, 'Apple Pay'),
    (2, 560, 'Magstripe'),
    (2, 400, 'Samsung Pay'),
    (4, 1200, 'Apple Pay');


select * from merchant_transactions

select 
merchant_id,
sum(case when payment_method="Apple Pay" then transaction_amount else 0 end) volume -- use case when to create an interim column first and then apply agg
from merchant_transactions
group by merchant_id
order by volume DESC



/*markdown
## Question 19
*/

/*markdown
This is the same question as problem #1 in the SQL Chapter of Ace the Data Science Interview!
Assume you have an events table on app analytics. Write a query to get the click-through rate (CTR %) per app in 2022. Output the results in percentages rounded to 2 decimal places.
Notes:
    • To avoid integer division, you should multiply the click-through rate by 100.0, not 100.
    • Percentage of click-through rate = 100.0 * Number of clicks / Number of impressions
*/

use interview;

CREATE TABLE events (
    app_id INT,
    event_type VARCHAR(255),
    timestamp DATETIME
);

INSERT INTO events (app_id, event_type, timestamp) VALUES
(123, 'impression', '2022-07-18 11:36:12'),
(123, 'impression', '2022-07-18 11:37:12'),
(123, 'click', '2022-07-18 11:37:42'),
(234, 'impression', '2022-07-18 14:15:12'),
(234, 'click', '2022-07-18 14:16:12');


select * from events

/*markdown
##### Solution trick:  
`sum (case when ...  then 1 else 0)`
*/

select 
app_id,
sum(case when event_type="impression" then 1 else 0 end) as impression,
sum(case when event_type="click" then 1 else 0 end) as click,
100.0 * sum(case when event_type="click" then 1 else 0 end) / sum(case when event_type="impression" then 1 else 0 end) as ctr
from  events
group by app_id

/*markdown
## Question 20:
*/

/*markdown
New TikTok users sign up with their emails and each user receives a text confirmation to activate their account. Assume you are given the below tables about emails and texts.
Write a query to display the ids of the users who did not confirm on the first day of sign-up, but confirmed on the second day.
Assumption:
action_date is the date when the user activated their account and confirmed their sign-up through the text.
*/

use interview;

-- Create emails table
CREATE TABLE emails (
    email_id INT,
    user_id INT,
    signup_date DATETIME
);

show tables;

CREATE TABLE texts (
    text_id INT,
    email_id INT,
    signup_action ENUM('Confirmed', 'Not confirmed'),
    action_date DATETIME
);

INSERT INTO emails (email_id, user_id, signup_date) VALUES
    (125, 7771, '2022-06-14 00:00:00'),
    (433, 1052, '2022-07-09 00:00:00');

INSERT INTO emails (email_id, user_id, signup_date)
VALUES
    (125, 7771, '2022-06-14 00:00:00'),
    (236, 6950, '2022-07-01 00:00:00'),
    (433, 1052, '2022-07-09 00:00:00');

INSERT INTO texts (text_id, email_id, signup_action, action_date) VALUES
    (6878, 125, 'Confirmed', '2022-06-14 00:00:00'),
    (6997, 433, 'Not Confirmed', '2022-07-09 00:00:00'),
    (7000, 433, 'Confirmed', '2022-07-10 00:00:00');

INSERT INTO texts (text_id, email_id, signup_action)
VALUES
    (6878, 125, 'Confirmed'),
    (6920, 236, 'Not Confirmed'),
    (6994, 236, 'Confirmed');



select * from emails;

select * from texts;

select email_id, date_add(action_date, interval 1 day) from texts;

select e.user_id,
e.email_id
from emails e
join 
texts t
on e.email_id=t.email_id
where DATEDIFF(t.action_date, e.signup_date) = 1

/*markdown
## Question 21:
*/

/*markdown
Assume you are given the table below on Uber transactions made by users. Write a query to obtain the third transaction of every user. Output the user id, spend and transaction date.
*/

CREATE TABLE transactions (
    user_id INT,
    spend DECIMAL(10, 2),
    transaction_date TIMESTAMP
);

INSERT INTO transactions (user_id, spend, transaction_date) VALUES
(111, 100.50, '2022-01-08 12:00:00'),
(111, 55.00, '2022-01-10 12:00:00'),
(121, 36.00, '2022-01-18 12:00:00'),
(145, 24.99, '2022-01-26 12:00:00'),
(111, 89.60, '2022-02-05 12:00:00');

select * from transactions;

select 
user_id,
spend,
transaction_date
from
(
    select
user_id,
spend,
transaction_date,
row_number() over(partition by user_id order by transaction_date asc) rn 
from transactions
) sub
where rn = 3;

/*markdown
## Question 22:
*/

/*markdown
Your team at Accenture is helping a Fortune 500 client revamp their compensation and benefits program. The first step in this analysis is to manually review employees who are potentially overpaid or underpaid. An employee is considered to be potentially overpaid if they earn more than 2 times the average salary for people with the same title. Similarly, an employee might be underpaid if they earn less than half of the average for their title. We'll refer to employees who are both underpaid and overpaid as compensation outliers for the purposes of this problem. Write a query that shows the following data for each compensation outlier: employee ID, salary, and whether they are potentially overpaid or potentially underpaid.
*/

use interview

show tables

CREATE TABLE employee_pay (
    employee_id INT,
    salary INT,
    title VARCHAR(255)
);

INSERT INTO employee_pay (employee_id, salary, title) VALUES
(101, 80000, 'Data Analyst'),
(102, 90000, 'Data Analyst'),
(103, 100000, 'Data Analyst'),
(104, 30000, 'Data Analyst'),
(105, 120000, 'Data Scientist'),
(106, 100000, 'Data Scientist'),
(107, 80000, 'Data Scientist'),
(108, 310000, 'Data Scientist');

select * from employee_pay;

-- Solution 1: Using window function
select 
employee_id,
salary,
case 
    when salary>= 2.0 * avg_salary then 'overpaid'
    when salary<= 0.5 * avg_salary then 'underpaid'
    end status
from
(select
employee_id,
salary,
title,
avg(salary) over (partition by title) as avg_salary
from
employee_pay
) sub
where salary > 2 * avg_salary OR salary < 0.5 * avg_salary;

-- Solution 2: using join
SELECT
    employee_id,
    salary,
    CASE
        WHEN salary > 2 * avg_salary THEN 'Overpaid'
        WHEN salary < 0.5 * avg_salary THEN 'Underpaid'
    END AS status
FROM
    employee_pay
JOIN (
    SELECT
        title,
        AVG(salary) AS avg_salary
    FROM
        employee_pay
    GROUP BY
        title
) AS title_avg ON employee_pay.title = title_avg.title
WHERE
    salary > 2 * avg_salary OR salary < 0.5 * avg_salary;

/*markdown
## Question 23:
*/

/*markdown
Assume you are given the tables below containing information on Snapchat users, their ages, and their time spent sending and opening snaps. Write a query to obtain a breakdown of the time spent sending vs. opening snaps (as a percentage of total time spent on these activities) for each of the different age groups.
Output the age bucket and percentage of sending and opening snaps. Round the percentages to 2 decimal places.
Notes:
You should calculate these percentages:
time sending / (time sending + time opening)
time opening / (time sending + time opening)
To avoid integer division in percentages, multiply by 100.0 and not 100.
*/

create database interview;

use interview;

show databases;

show tables;

CREATE TABLE activities (
    activity_id INT,
    user_id INT,
    activity_type VARCHAR(255),
    time_spent FLOAT,
    activity_date DATETIME
);

INSERT INTO activities (activity_id, user_id, activity_type, time_spent, activity_date) VALUES
(7274, 123, 'open', 4.50, '2022-06-22 12:00:00'),
(2425, 123, 'send', 3.50, '2022-06-22 12:00:00'),
(1413, 456, 'send', 5.67, '2022-06-23 12:00:00'),
(1414, 789, 'chat', 11.00, '2022-06-25 12:00:00'),
(2536, 456, 'open', 3.00, '2022-06-25 12:00:00');

CREATE TABLE age_breakdown (
    user_id INT,
    age_bucket VARCHAR(255)
);

INSERT INTO age_breakdown (user_id, age_bucket) VALUES
(123, '31-35'),
(456, '26-30'),
(789, '21-25');

select * from activities;

select * from age_breakdown;

select 
ab.age_bucket,
ROUND(
    100.0 * sum(case when a.activity_type="open" then time_spent else 0.0 end)/ 
    sum(case when a.activity_type in ("open", "send") then time_spent else 0.0 end),2) open,
ROUND(
    100.0 * sum(case when a.activity_type="send" then time_spent else 0.0 end)/ 
    sum(case when a.activity_type in ("open", "send") then time_spent else 0.0 end), 2) send
from 
age_breakdown ab 
join activities a on ab.user_id=a.user_id
group by ab.age_bucket

/*markdown
## Question 24:
The table below contains information about tweets over a given period of time. Calculate the 3-day rolling average of tweets published by each user for each date that a tweet was posted. Output the user id, tweet date, and rolling averages rounded to 2 decimal places.
Important Assumptions:
Rows in this table are consecutive and ordered by date.
Each row represents a different day
A day that does not correspond to a row in this table is not counted. The most recent day is the next row above the current row.
Note: Rolling average is a metric that helps us analyze data points by creating a series of averages based on different subsets of a dataset. It is also known as a moving average, running average, moving mean, or rolling mean.

*/

CREATE TABLE tweets (
    tweet_id INT,
    user_id INT,
    tweet_date TIMESTAMP
);


INSERT INTO tweets (tweet_id, user_id, tweet_date) VALUES
(214252, 111, '2022-06-01 12:00:00'),
(739252, 111, '2022-06-01 12:00:00'),
(846402, 111, '2022-06-02 12:00:00'),
(241425, 254, '2022-06-02 12:00:00'),
(137374, 111, '2022-06-04 12:00:00');

select * from tweets

    select 
    user_id,
    date(tweet_date) tweet_date,
    count(tweet_id) tweet_count
    from tweets
    group by user_id, date(tweet_date)


select 
user_id,
tweet_date,
ROUND(avg(tweet_count) over (
    partition by user_id order by tweet_date rows between 2 preceding and current row 
    ), 2) rolling_avg
from
(
    select 
    user_id,
    date(tweet_date) tweet_date,
    count(tweet_id) tweet_count
    from tweets
    group by user_id, date(tweet_date)
) sub

show databases;

/*markdown
## Question 25:
Assume you are given the table below containing measurement values obtained from a sensor over several days. Measurements are taken several times within a given day.
Write a query to obtain the sum of the odd-numbered and even-numbered measurements on a particular day, in two different columns.
Note that the 1st, 3rd, 5th measurements within a day are considered odd-numbered measurements and the 2nd, 4th, 6th measurements are even-numbered measurements.
*/

use interview;

CREATE TABLE measurements (
    measurement_id INT,
    measurement_value DECIMAL(10,2),
    measurement_time DATETIME
);


INSERT INTO measurements (measurement_id, measurement_value, measurement_time)
VALUES
    (131233, 1109.51, '2022-07-10 09:00:00'),
    (135211, 1662.74, '2022-07-10 11:00:00'),
    (523542, 1246.24, '2022-07-10 13:15:00'),
    (143562, 1124.50, '2022-07-11 15:00:00'),
    (346462, 1234.14, '2022-07-11 16:45:00');


select * from measurements

select 
measurement_date,
sum( case when mod(rn,2)=0 then measurement_value else 0 end ) even_measurement,
sum( case when mod(rn,2)=1 then measurement_value else 0 end ) odd_measurement
from
(
    select 
    measurement_id,
    measurement_value,
    measurement_time,
    date(measurement_time)  measurement_date,
    row_number() over  (partition by date(measurement_time)) rn
    from measurements
) sub
group by measurement_date

/*markdown
## Question 26:
Assume you are given the following tables on Walmart transactions and products. Find the top 3 products that are most frequently bought together (purchased in the same transaction).
Output the name of product #1, name of product #2 and number of combinations in descending order.
*/

-- Create products table
CREATE TABLE products (
    product_id INT,
    product_name VARCHAR(255)
);



-- Create transactions table
CREATE TABLE transactions (
    transaction_id INT,
    product_id INT,
    user_id INT,
    transaction_date DATETIME
);

-- Insert example data into products table
INSERT INTO products (product_id, product_name)
VALUES
    (111, 'apple'),
    (222, 'soy milk'),
    (333, 'instant oatmeal'),
    (444, 'banana'),
    (555, 'chia seed');



-- Insert example data into transactions table
INSERT INTO transactions (transaction_id, product_id, user_id, transaction_date)
VALUES
    (231574, 111, 234, '2022-03-01 12:00:00'),
    (231574, 444, 234, '2022-03-01 12:00:00'),
    (231574, 222, 234, '2022-03-01 12:00:00'),
    (137124, 111, 125, '2022-03-05 12:00:00'),
    (137124, 444, 125, '2022-03-05 12:00:00');


select * from transactions

select * from products;

select
count(t1.transaction_id) combination_count,
p1.product_id productid1,
p2.product_id productid2
from
transactions t1
join transactions t2
on t1.transaction_id= t2.transaction_id and t1.product_id < t2.product_id
join products p1 on t1.product_id=p1.product_id
join products p2 on t2.product_id=p2.product_id 
group by productid1, productid2
ORDER by combination_count DESC

SELECT 
    -- t1.transaction_id,
    t1.product_id product1,
    -- t1.user_id,
    -- t2.transaction_id,
    t2.product_id product2,
    -- t2.user_id    
    count(t1.transaction_id) combination_count 
FROM transactions t1
JOIN transactions t2 ON t1.transaction_id = t2.transaction_id AND t1.product_id < t2.product_id
JOIN product p1 on 
GROUP by  t1.product_id, t2.product_id
ORDER by combination_count DESC




SELECT 
    t1.transaction_id,
    t1.product_id p1,
    t1.user_id,
    t2.transaction_id,
    t2.product_id p2
    t2.user_id    
FROM transactions t1
JOIN transactions t2 ON t1.transaction_id = t2.transaction_id AND t1.product_id < t2.product_id
JOIN products p1 ON t1.product_id = p1.product_id
JOIN products p2 ON t2.product_id = p2.product_id
GROUP BY product1, product2
ORDER BY combination_count DESC
LIMIT 3;



/*markdown
#### Tricks:

**Problem 1:** How to break a the values of a  single column into multiple columns:

1. Use case when to generate new columns.
2. Perform self join with some condition to create two columns 

**Problem 2:** How to get the complete row in a situation when you have to perform a group by? Since group by essentially forces you to aggregate columns and skip the cols that can't be agregated:

1.  use where in on a grouped data:
    -  `select * from dummytable where (a, b) in ( select a, b from dummy_table groupy by a)`

**Problem 3:** Unique pairs of all values in a column:

1. Perform self join with a condition t1.col < t2.col
*/


SELECT 
    p1.product_name AS product1,
    p2.product_name AS product2,
    COUNT(*) AS combination_count
FROM transactions t1
JOIN transactions t2 ON t1.transaction_id = t2.transaction_id AND t1.product_id < t2.product_id
-- JOIN products p1 ON t1.product_id = p1.product_id
-- JOIN products p2 ON t2.product_id = p2.product_id
-- GROUP BY product1, product2
-- ORDER BY combination_count DESC
-- LIMIT 3;



CREATE TABLE test_table (
    A INT,
    B INT
);

-- Inserting data into the table
INSERT INTO test_table (A, B) VALUES
    (NULL, NULL),     -- One row has both null
    (1, NULL),        -- One row has only one null
    (2, 3);           -- One row has no nulls


select * from test_table

select count(1) from test_table

/*markdown
## Question 27
Assume you are given the table below containing information on Amazon customers and their spend on products belonging to various categories. Identify the top two highest-grossing products within each category in 2022. Output the category, product, and total spend.

*/

CREATE TABLE product_spend (
    category VARCHAR(255),
    product VARCHAR(255),
    user_id INT,
    spend DECIMAL(10, 2),
    transaction_date TIMESTAMP
);


INSERT INTO product_spend (category, product, user_id, spend, transaction_date)
VALUES
    ('appliance', 'refrigerator', 165, 246.00, '2021-12-26 12:00:00'),
    ('appliance', 'refrigerator', 123, 299.99, '2022-03-02 12:00:00'),
    ('appliance', 'washing machine', 123, 219.80, '2022-03-02 12:00:00'),
    ('electronics', 'vacuum', 178, 152.00, '2022-04-05 12:00:00'),
    ('electronics', 'wireless headset', 156, 249.90, '2022-07-08 12:00:00'),
    ('electronics', 'vacuum', 145, 189.00, '2022-07-15 12:00:00');

select * from product_spend

WITH ranked_products AS (
    SELECT
        category,
        product,
        spend,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY spend DESC) AS rank_within_category
    FROM
        product_spend
    WHERE
        YEAR(transaction_date) = 2022
)
SELECT
    category,
    product,
    spend
FROM
    ranked_products
WHERE
    rank_within_category <= 2;


/*markdown
## Question 28:
Assume you are given the table below on user transactions. Write a query to obtain the list of customers whose first transaction was valued at $50 or more. Output the number of users.
Clarification:
Use the transaction_date field to determine which transaction should be labeled as the first for each user.
Use a specific function (we can't give too much away!) to account for scenarios where a user had multiple transactions on the same day, and one of those was the first.

*/

CREATE TABLE user_transactions (
    transaction_id INT,
    user_id INT,
    spend DECIMAL(10, 2),
    transaction_date TIMESTAMP
);

INSERT INTO user_transactions (transaction_id, user_id, spend, transaction_date)
VALUES
    (759274, 111, 49.50, '2022-02-03 00:00:00'),
    (850371, 111, 51.00, '2022-03-15 00:00:00'),
    (615348, 145, 36.30, '2022-03-22 00:00:00'),
    (137424, 156, 151.00, '2022-04-04 00:00:00'),
    (248475, 156, 87.00, '2022-04-16 00:00:00');

select * from user_transactions

select 
user_id
from
(select 
user_id,
spend,
row_number() over (partition by user_id order by transaction_date asc) rn
from
user_transactions
) sub
where rn=1 and spend>=50.00

/*markdown
## Question 29
The LinkedIn Creator team is looking for power creators who use their personal profile as a company or influencer page. If someone's LinkedIn page has more followers than the company they work for, we can safely assume that person is a power creator.
Write a query to return the IDs of these LinkedIn power creators in alphabetical order.
Assumption:
A person can work at multiple companies.
This is the second part of the question, so make sure your start with Part 1 if you haven't completed that yet!
*/

-- Create personal_profiles Table
CREATE TABLE personal_profiles (
    profile_id INT PRIMARY KEY,
    name VARCHAR(255),
    followers INT
);


-- Insert data into personal_profiles Table
INSERT INTO personal_profiles (profile_id, name, followers) VALUES
(1, 'Nick Singh', 92000),
(2, 'Zach Wilson', 199000),
(3, 'Daliana Liu', 171000),
(4, 'Ravit Jain', 107000),
(5, 'Vin Vashishta', 139000),
(6, 'Susan Wojcicki', 39000);


-- Create employee_company Table
CREATE TABLE employee_company (
    personal_profile_id INT,
    company_id INT
);



-- Insert data into employee_company Table
INSERT INTO employee_company (personal_profile_id, company_id) VALUES
(1, 4),
(1, 9),
(2, 2),
(3, 1),
(4, 3),
(5, 6),
(6, 5);



-- Create company_pages Table
CREATE TABLE company_pages (
    company_id INT PRIMARY KEY,
    name VARCHAR(255),
    followers INT
);

-- Insert data into company_pages Table
INSERT INTO company_pages (company_id, name, followers) VALUES
(1, 'The Data Science Podcast', 8000),
(2, 'Airbnb', 700000),
(3, 'The Ravit Show', 6000),
(4, 'DataLemur', 200),
(5, 'YouTube', 16000000),
(6, 'DataScience.Vin', 4500),
(9, 'Ace The Data Science Interview', 4479);


select * from company_pages

select * from employee_company;

select * from personal_profiles;

select 
pp.profile_id,
pp.name employee,
pp.followers employee_followers,
cp.name employer,
cp.followers company_followers
from 
company_pages cp 
join employee_company ec on cp.company_id=ec.company_id
join personal_profiles pp on ec.personal_profile_id=pp.profile_id
where pp.followers>=cp.followers

/*markdown
## Question 31:
Assume there are three Spotify tables containing information about the artists, songs, and music charts. Write a query to determine the top 5 artists whose songs appear in the Top 10 of the global_song_rank table the highest number of times. From now on, we'll refer to this ranking number as "song appearances".
Output the top 5 artist names in ascending order along with their song appearances ranking (not the number of song appearances, but the rank of who has the most appearances). The order of the rank should take precedence.
For example, Ed Sheeran's songs appeared 5 times in Top 10 list of the global song rank table; this is the highest number of appearances, so he is ranked 1st. Bad Bunny's songs appeared in the list 4, so he comes in at a close 2nd.
Assumptions:
If two artists' songs have the same number of appearances, the artists should have the same rank.
The rank number should be continuous (1, 2, 2, 3, 4, 5) and not skipped (1, 2, 2, 4, 5).
*/

-- Create artists Table
CREATE TABLE artists (
    artist_id INTEGER PRIMARY KEY,
    artist_name VARCHAR(255) NOT NULL
);

-- Insert Example Data into artists Table
INSERT INTO artists (artist_id, artist_name) VALUES
(101, 'Ed Sheeran'),
(120, 'Drake');

-- Create songs Table
CREATE TABLE songs (
    song_id INTEGER PRIMARY KEY,
    artist_id INTEGER
);

-- Insert Example Data into songs Table
INSERT INTO songs (song_id, artist_id) VALUES
(45202, 101),
(19960, 120);


-- Create global_song_rank Table
CREATE TABLE global_song_rank (
    days INTEGER CHECK (days >= 1 AND days <= 52),
    song_id INTEGER,
    ranks INTEGER CHECK (ranks >= 1 AND ranks <= 1000000)
    )


-- Insert Example Data into global_song_rank Table
INSERT INTO global_song_rank (days, song_id, ranks) VALUES
(1, 45202, 5),
(3, 45202, 2),
(1, 19960, 3),
(9, 19960, 15);


select * from global_song_rank;

select * from songs

select * from artists

use interview

select
a.artist_id,
a.artist_name,
sum(days) total_days,
row_number() over (order by sum(days) desc) rnk -- window functions can be used without partition by clause
from 
global_song_rank gsr
join songs s on gsr.song_id= s.song_id
join artists a on s.artist_id=a.artist_id
where ranks<=10
GROUP by a.artist_id
order by total_days DESC



/*markdown
## Question 32

New TikTok users sign up with their emails, so each signup requires a text confirmation to activate the new user's account.
Write a query to find the confirmation rate of users who confirmed their signups with text messages. Round the result to 2 decimal places.
Assumptions:
A user may fail to confirm several times with text. Once the signup is confirmed for a user, they will not be able to initiate the signup again.
A user may not initiate the signup confirmation process at all.

*/

select * from emails;

select * from texts;

select 
round(
    (
        sum(case when signup_action="Confirmed" then 1 else 0 end)/
        count(signup_action)
        ), 2
) confirm_rate
from
(select 
e.email_id,
e.user_id,
t.text_id,
t.signup_action
from emails e
left join
texts t
on e.email_id=t.email_id) sub

/*markdown
## Question 33:
When you log in to your retailer client's database, you notice that their product catalog data is full of gaps in the category column. Can you write a SQL query that returns the product catalog with the missing data filled in?
*/

/*markdown
Assumptions
- Each category is mentioned only once in a category column.
- All the products belonging to same category are grouped together.
- The first product from a product group will always have a defined category.
- Meaning that the first item from each category will not have a missing category value.
*/

CREATE TABLE retailproducts (
    product_id INTEGER,
    category VARCHAR(255),
    name VARCHAR(255)
);


INSERT INTO retailproducts (product_id, category, name) VALUES
    (1, 'Shoes', 'Sperry Boat Shoe'),
    (2, 'Shoes', 'Adidas Stan Smith'),
    (3, 'Shoes', 'Vans Authentic'),
    (4, 'Jeans', 'Levi 511'),
    (5, 'Jeans', 'Wrangler Straight Fit'),
    (6, 'Shirts', 'Lacoste Classic Polo'),
    (7, 'Shirts', 'Nautica Linen Shirt');

select * from retailproducts

truncate table retailproducts



/*markdown
## Question x:
*/

/*markdown
Set all the active applcations to closed in the jobapplications table for ABC Corporation
*/

use interview;

-- Create Company table
CREATE TABLE interview.Company (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL
);

-- Create JobApplications table
CREATE TABLE interview.JobApplications (
    application_id INT PRIMARY KEY,
    status VARCHAR(50) NOT NULL,
    company_id INT,
    FOREIGN KEY (company_id) REFERENCES Company(company_id)
);


-- Insert dummy data into Company table
INSERT INTO interview.Company (company_id, company_name) VALUES
    (1, 'ABC Corporation'),
    (2, 'XYZ Ltd'),
    (3, '123 Industries');


-- Insert dummy data into JobApplications table
INSERT INTO interview.JobApplications (application_id, status, company_id) VALUES
    (101, 'Pending', 1),
    (102, 'Rejected', 2),
    (103, 'Accepted', 3),
    (104, 'Pending', 1),
    (105, 'Accepted', 2);


select * from interview.Company;

select * from JobApplications;

update interview.JobApplications set 
status='closed'
where status='Pending'



update JobApplications
set status='closed'
where company_id=(select company_id from Company where company_name="ABC Corporation")



/*markdown
### Nimoy interview question:
*/

use interview;

CREATE TABLE interview.bugs (
    bug_id INT,
    title VARCHAR(255),
    priority VARCHAR(50),
    severity INT
);

INSERT INTO interview.bugs (title, priority, severity) VALUES
    ('Bug1', 'High', 10),
    ('Bug2', 'High', 8),
    ('Bug3', 'High', 9),
    ('Bug4', 'Medium', 7),
    ('Bug5', 'Medium', 6),
    ('Bug6', 'Medium', 5),
    ('Bug7', 'Low', 3),
    ('Bug8', 'Low', 2),
    ('Bug9', 'Low', 1);

select * from bugs;

with cte as (
    SELECT
    priority,
    max(severity) as top_severe
    from 
    interview.bugs
    group by priority 
)
select 
title,
priority,
severity
from
interview.bugs
where (priority, severity) in (select priority , top_severe from cte)

with cte as (
    SELECT
    title,
    priority,
    severity,
    row_number() over (PARTITION by priority order by severity desc) rn 
    from 
    interview.bugs
)
select 
title,
priority,
severity
from
cte 
where rn<3

select
*,
row_number() over (partition by priority order by severity desc) rn
from bugs

WITH ct1 AS (
  select * from (SELECT 
    priority,
    CASE WHEN rn = 1 THEN title END AS top1,
    
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY priority ORDER BY severity DESC) rn
    FROM bugs
  ) sub) su
  WHERE top1 IS NOT NULL
),
ct2 AS (
  select * from (SELECT 
    priority,
    CASE WHEN rn = 2 THEN title END AS top2
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY priority ORDER BY severity DESC) rn
    FROM bugs
  ) sub) su
  WHERE top2 IS NOT NULL
)
SELECT * FROM ct1 JOIN ct2 ON ct1.priority = ct2.priority;

SELECT * FROM ct1 JOIN ct2 ON ct1.priority = ct2.priority;

/*markdown
### Question 34:
In consulting, being "on the bench" means you have a gap between two client engagements. Google wants to know how many days of bench time each consultant had in 2021. Assume that each consultant is only staffed to one consulting engagement at a time.
Write a query to pull each employee ID and their total bench time in days during 2021.
Assumptions:
All listed employees are current employees who were hired before 2021.
The engagements in the consulting_engagements table are complete for the year 2022.
*/

-- Create staffing Table
CREATE TABLE interview.staffing (
    employee_id INTEGER,
    is_consultant BOOLEAN,
    job_id INTEGER,
    PRIMARY KEY (employee_id)
);



-- Insert data into staffing Table
INSERT INTO interview.staffing (employee_id, is_consultant, job_id) VALUES
(111, true, 7898),
(121, false, 6789),
(156, true, 4455);



-- Create consulting_engagements Table
CREATE TABLE interview.consulting_engagements (
    job_id INTEGER,
    client_id INTEGER,
    start_date DATE,
    end_date DATE,
    contract_amount INTEGER
);


-- Insert data into consulting_engagements Table
INSERT INTO interview.consulting_engagements (job_id, client_id, start_date, end_date, contract_amount) VALUES
(7898, 20076, '2021-05-25', '2021-06-30', 11290.00),
(6789, 20045, '2021-06-01', '2021-11-12', 33040.00),
(4455, 20001, '2021-01-25', '2021-05-31', 31839.00);


select * from interview.staffing

select * from interview.consulting_engagements;

select
s.employee_id,
365-datediff(end_date, start_date) as benched_days
from
staffing s
join consulting_engagements  ce
on s.job_id=ce.job_id and s.is_consultant=1

/*markdown

## Question 35:
You are given a songs_history table that keeps track of users' listening history on Spotify. 
*/

/*markdown
The songs_weekly table tracks how many times users listened to each song for all days between August 1 and August 7, 2022.
*/

/*markdown
Write a query to show the user ID, song ID, and the number of times the user has played each song as of August 4, 2022. We'll refer to the number of song plays as song_plays. The rows with the most song plays should be at the top of the output.
*/

/*markdown
Assumption:
The songs_history table holds data that ends on July 31, 2022. Output should include the historical data in this table.
There may be a new user in the weekly table who is not present in the history table.
A user may listen to a song for the first time, in which case no existing (user_id, song_id) user-song pair exists in the history table.
A user may listen to a specific song multiple times in the same day.
*/

*/

-- Create songs_history Table
CREATE TABLE songs_history (
    history_id INTEGER,
    user_id INTEGER,
    song_id INTEGER,
    song_plays INTEGER
);


-- Insert data into songs_history Table
INSERT INTO songs_history (history_id, user_id, song_id, song_plays) VALUES
(10011, 777, 1238, 11),
(12452, 695, 4520, 1);



-- Create songs_weekly Table
CREATE TABLE songs_weekly (
    user_id INTEGER,
    song_id INTEGER,
    listen_time DATETIME,
    PRIMARY KEY (user_id, song_id)
);


-- Insert data into songs_weekly Table
INSERT INTO songs_weekly (user_id, song_id, listen_time) VALUES
(777, 1238, '2022-08-01 12:00:00'),
(695, 4520, '2022-08-04 08:00:00'),
(125, 9630, '2022-08-04 16:00:00'),
(695, 9852, '2022-08-07 12:00:00');


select * from songs_weekly;

select * from songs_history;


select 
sub.user_id,
sub.song_id,
coalesce(sh.song_plays, 0) + coalesce(sub.song_plays, 0) as song_plays
from
(
    select 
    user_id,
    song_id,
    count(listen_time) as song_plays
    from
    songs_weekly
    where day(listen_time)<=4
    group by user_id, song_id
) sub
left outer JOIN
songs_history sh 
on sub.user_id=sh.user_id and sub.song_id=sh.song_id
order by song_plays desc



/*markdown
### Tiger Analytics SQL question:
*/

/*markdown
#### Q1: find the sum of ordervalue for those users who ordered on both days
*/

truncate table interview.tigerorders;

CREATE TABLE interview.tigerorders (
    orderdate DATE,
    userid INT,
    ordervalue DECIMAL(10, 2)
);


INSERT INTO interview.tigerorders (orderdate, userid, ordervalue) VALUES
    ('2024-02-01', 1, 50.00),
    ('2024-02-01', 2, 75.50),
    ('2024-02-01', 2, 30.25),
    ('2024-02-01', 3, 45.75),
    ('2024-02-01', 3, 60.00),
    ('2024-02-01', 3, 25.50),
    ('2024-02-02', 1, 40.00),
    ('2024-02-02', 2, 55.25),
    ('2024-02-02', 2, 20.75),
    ('2024-02-02', 3, 35.50),
    ('2024-02-02', 3, 50.75),
    ('2024-02-02', 3, 15.25);


delete from interview.tigerorders where userid=1 and orderdate="2024-02-01"

delete from interview.tigerorders where userid=2 and orderdate="2024-02-01"

select * from interview.tigerorders

SELECT 
userid,
sum(ordervalue)
from interview.tigerorders
group by userid
having count( distinct(orderdate))>1

/*markdown
#### Q2: how will inner and outer joins behave for the follwing tables
*/

create table interview.A(
    id int¸
)

insert into interview.A (id) values (1),(1),(1)

insert into interview.A (id) values (null)

create table interview.B(
    id int
)

truncate table interview.B

insert into interview.B (id) values (1),(1)

select * from interview.A;

select * from interview.B;

insert into interview.B (id) values (null)

select * from interview.A join interview.B on A.id=B.id

select * from interview.A left join interview.B on A.id=B.id

select * from interview.A right join interview.B on A.id=B.id

-- nulls don't particiate in joins
-- but in outer joins nulls from the original table will stay
-- ex: in left outer left nulls will appear in the output

/*markdown
## Incedo interview question:

write a sql query to get the score of all the teams participating. 1 point for 1 win . 0 for loss
*/

CREATE TABLE interview.results (
    team1 VARCHAR(50),
    team2 VARCHAR(50),
    result VARCHAR(50)
);



-- Insert data into the results table
INSERT INTO interview.results (team1, team2, result) VALUES
('India', 'Pakistan', 'India'),
('England', 'Afghanistan', 'Afghanistan'),
('Australia', 'India', 'India'),
('Sri Lanka', 'Bangladesh', 'Sri Lanka'),
('England', 'Pakistan', 'England'),
('Australia', 'New Zealand', 'Australia');


select * from interview.results

SELECT team1 AS team, CASE WHEN result = team1 THEN 1 ELSE 0 END AS score FROM interview.results
UNION ALL
SELECT team2 AS team, CASE WHEN result = team2 THEN 1 ELSE 0 END AS score FROM interview.results

SELECT team, COALESCE(SUM(score), 0) AS score
FROM (
    SELECT team1 AS team, CASE WHEN result = team1 THEN 1 ELSE 0 END AS score FROM your_table
    UNION ALL
    SELECT team2 AS team, CASE WHEN result = team2 THEN 1 ELSE 0 END AS score FROM your_table
) AS scores
GROUP BY team;
