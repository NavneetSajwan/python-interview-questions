/*markdown
### Cognitio 
*/

/*markdown
### Question:
*/

use interview;

drop table interview.Employee;

CREATE TABLE interview.Employee (
    EmployeeID VARCHAR(50),
    EmployeeName VARCHAR(50),
    ManagerID VARCHAR(50)
);

INSERT INTO Employee (EmployeeID, EmployeeName, ManagerID) VALUES
(1, 'Employee1', NULL),               -- Top-level employee
(2, 'Employee2', 1),                  -- First-level employee under Employee1
(3, 'Employee3', 2),                  -- Second-level employee under Employee2
(4, 'Employee4', 3),                  -- Third-level employee under Employee3
(5, 'Employee5', 2),                  -- Second-level employee under Employee2
(6, 'Employee6', 5),                  -- Third-level employee under Employee5
(7, 'Employee7', 1),                  -- First-level employee under Employee1
(8, 'Employee8', 7),                  -- Second-level employee under Employee7
(9, 'Employee9', 8);                  -- Third-level employee under Employee8


select * from interview.Employee order by EmployeeID

SELECT e3.*
FROM Employee e1
JOIN Employee e2 ON e1.EmployeeID = e2.ManagerID
JOIN Employee e3 ON e2.EmployeeID = e3.ManagerID;

select 
L2.EmployeeId,
L2.EmployeeName,
e3.ManagerId as ManagerIDL3
from
(
    select 
e1.EmployeeId,
e1.EmployeeName,
e2.ManagerId as ManagerIDL2
FROM
Employee e1
JOIN Employee e2 on e1.ManagerID=e2.EmployeeID
) L2
join Employee e3 on L2.ManagerIDL2=e3.EmployeeID

use interview;

WITH recursive RecursiveHierarchy AS (
    SELECT EmployeeID, EmployeeName, ManagerID
    FROM Employee
    WHERE ManagerID IS NULL
    
    UNION ALL
    
    SELECT e.EmployeeID, e.EmployeeName, e.ManagerID
    FROM Employee e
    INNER JOIN RecursiveHierarchy rh ON e.ManagerID = rh.EmployeeID
)

SELECT * FROM RecursiveHierarchy;


/*markdown
### Question 2:

Tiger Analytics hashtag#pyspark Interview Question for the Data Engineer / Data Analyst / Data Science role.

Challenge : Find the origin and the destination of each customer.
Note : There can be more than 1 stops for the same customer journey.


flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
 (1,'Flight2' , 'Hyderabad' , 'Kochi'),
 (1,'Flight3' , 'Kochi' , 'Mangalore'),
 (2,'Flight1' , 'Mumbai' , 'Ayodhya'),
 (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')
 ]

_schema = "cust_id int, flight_id string , origin string , destination string"

*/

CREATE TABLE Flights (
    cust_id INT,
    flight_id VARCHAR(50),
    origin VARCHAR(50),
    destination VARCHAR(50)
);

INSERT INTO Flights (cust_id, flight_id, origin, destination)
VALUES
    (1, 'Flight1', 'Delhi', 'Hyderabad'),
    (1, 'Flight2', 'Hyderabad', 'Kochi'),
    (1, 'Flight3', 'Kochi', 'Mangalore'),
    (2, 'Flight1', 'Mumbai', 'Ayodhya'),
    (2, 'Flight2', 'Ayodhya', 'Gorakhpur');

select * from interview.Flights;

select * from interview.Flights;

with cte1 as 
(
    SELECT
*,
rank() over (partition by cust_id order by flight_id asc) rnk1,
rank() over (partition by cust_id order by flight_id desc) rnk2
from interview.Flights
),

cte2 as 
(
    select *
    from cte1 
    where rnk1=1
),

cte3 as 
(
    select *
    from cte1 
    where rnk2=1
)

select 
lc.cust_id,
lc.origin,
rc.destination
FROM
cte2 lc 
inner JOIN
cte3 rc
on lc.cust_id=rc.cust_id


with recursive F2 as
(
    (SELECT
    cust_id,
    origin origin,
    destination destination
    FROM
    interview.Flights 
    ) 
    union all
    (SELECT
    F1.cust_id,
    F1.origin origin,
    F2.destination destination
    FROM
    interview.Flights F1
    JOIN
    F2
    on F1.destination=F2.origin 
    and F1.cust_id and F2.cust_id)
)
select * from F2
order by cust_id;

SELECT
    F1.cust_id,
    F1.origin origin,
    F2.destination destination
    FROM
    interview.Flights F1
    JOIN
    interview.Flights F2
    on F1.destination=F2.origin 
    and F1.cust_id and F2.cust_id

use interview;

with recursive cte as (
    select
    F.cust_id cust_id,
    F.origin origin,
    cte.destination destination
    FROM
    FLights F
    inner JOIN
    cte 
    on F.origin=cte.destination
)

select * from cte;

