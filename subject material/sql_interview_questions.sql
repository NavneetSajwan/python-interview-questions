/*markdown
## Nth highest salary
*/

show DATABASES

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


select * from parts_assembly

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


select * from trades;

select * from users

/*markdown
#### Solution 4:
*/

-- OPtionn1:
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


select * from job_listings;

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


select * from transactions;

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

CREATE TABLE user_transactions (
    transaction_id INT,
    product_id INT,
    user_id INT,
    spend DECIMAL(10, 2)
);

INSERT INTO user_transactions (transaction_id, product_id, user_id, spend)
VALUES
    (131432, 1324, 128, 699.78),
    (131433, 1313, 128, 501.00),
    (153853, 2134, 102, 1001.20),
    (247826, 8476, 133, 1051.00),
    (247265, 3255, 133, 1474.00),
    (136495, 3677, 133, 247.56);

select * from user_transactions;

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

select * from datacenters;

select * from forecasted_demand;

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

Given a table of Facebook posts, for each user who posted at least twice in 2021, write a query to find the number of days between each user’s first post of the year and last post of the year in the year 2021. Output the user and number of the days between each user's first and last post.

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


select * from posts

select
user_id,
DATEDIFF(max(post_date), min(post_date)) as days
FROM
posts
group by user_id
having count(user_id) > 1

/*markdown
## Question 14:

Write a query to find the top 2 power users who sent the most messages on Microsoft Teams in August 2022. Display the IDs of these 2 users along with the total number of messages they sent. Output the results in descending count of the messages.

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


select * from messages;

select 
sender_id,
count(message_id) as total
from messages
where month(sent_date)=8 and year(sent_date)=2022
group by sender_id

/*markdown
## Question 15: Two way relatinship

You are given a table of PayPal payments showing the payer, the recipient, and the amount paid. A two-way unique relationship is established when two people send money back and forth. Write a query to find the number of two-way unique relationships in this data.

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


select * from payments;

select 
*
FROM
payments
where (payer_id, recipient_id) in
(
select
p1.payer_id, p1.recipient_id
from payments p1
JOIN payments p2 on p1.payer_id=p2.recipient_id and p1.recipient_id=p2.payer_id
)
and payer_id<recipient_id
-- where payer_id=recipient_id

SELECT 
COUNT(DISTINCT CONCAT(payer_id, '-', recipient_id)) AS unique_relationships
FROM payments
WHERE (payer_id, recipient_id) IN (SELECT recipient_id, payer_id FROM payments)
  AND payer_id < recipient_id;


/*markdown
## Question 17

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

select 
app_id,
sum(case when event_type="impression" then 1 else 0 end) as impression,
sum(case when event_type="click" then 1 else 0 end) as click,
100.0 * sum(case when event_type="click" then 1 else 0 end) / sum(case when event_type="impression" then 1 else 0 end) as ctr
from  events
group by app_id

/*markdown
## Question 20:

new TikTok users sign up with their emails and each user receives a text confirmation to activate their account. Assume you are given the below tables about emails and texts.
Write a query to display the ids of the users who did not confirm on the first day of sign-up, but confirmed on the second day.
Assumption:
action_date is the date when the user activated their account and confirmed their sign-up through the text.
*/

-- Create emails table
CREATE TABLE emails (
    email_id INT,
    user_id INT,
    signup_date DATETIME
);


-- Create texts table
CREATE TABLE texts (
    text_id INT,
    email_id INT,
    signup_action ENUM('Confirmed', 'Not confirmed'),
    action_date DATETIME
);

-- Insert data into emails table
INSERT INTO emails (email_id, user_id, signup_date) VALUES
    (125, 7771, '2022-06-14 00:00:00'),
    (433, 1052, '2022-07-09 00:00:00');

-- Insert data into texts table
INSERT INTO texts (text_id, email_id, signup_action, action_date) VALUES
    (6878, 125, 'Confirmed', '2022-06-14 00:00:00'),
    (6997, 433, 'Not Confirmed', '2022-07-09 00:00:00'),
    (7000, 433, 'Confirmed', '2022-07-10 00:00:00');

select * from emails;

select * from texts;

