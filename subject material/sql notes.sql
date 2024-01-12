/*markdown
### Query execution:
*/

/*markdown
mysql>
 
`select * from employee 
where country='USA' 
order by salary desc 
limit 10;`
*/

/*markdown
1. Syntax check
2. execution plans
3. predicate push down
4. physical plan
5. execution
6. output
*/

/*markdown
### Order of operation:
*/

/*markdown
- from
- where
- group by
- having
- select
- order by
- limit
*/

/*markdown
### Indexing:
*/

/*markdown
 - Indexing creates a lookup table with the column and the pointer to the memory location of the row, containing this column.
 - good for read intensive database
 - 
 - Just like index page in a book
 - we don't have to scan the unnecessaery data
 - fast
 - records are stored in contiguous memory locations
 - why slow quwry?:
    - extra seek time.
    - most od the time in searching
 - index is a data structture which will hold key (on which index created) and search address
*/

/*markdown
### Types of indexing:
*/

/*markdown
- Three types mainly:
    - Primary indexing: on pk
    - Secondary indexing: on candidate key
    - Clustering indexing: on non-key cols
*/

/*markdown
### Optimizations:
*/

/*markdown
1. Reduce Table size(scanning data)
2. Simplify joins(join is a costly option)
    - example:
    - three tables: emp_data 20k, emp_salary 20k, department 1k
    - perfrom the join that returns in less rows first 
        - emp_data join department first
        - then emp_data with emp_salary
3. Avoid `select * ...` . Use specific cols
4. Use `where` clause before `group by`:
    -  group by is costl
*/

/*markdown
### Window functions:
*/

/*markdown
- Syntax:
    - `window_function/aggregation() over (Optional[partition by c1,c2] order by c1,c2) `
*/

/*markdown
- example:
    - `sum() over (partition by dept_name order by salary desc)`
*/

/*markdown
### Widely used window functios:
*/

/*markdown
- row_number
- rank
- dense_rank
- lead(col, n)
- lag(col, n)
- first_alue(col)
- last_value(col)
- sum(col)
- min, max, avg, count
- frame clauses
*/

/*markdown
#### coalesce:
*/

/*markdown

- returns first value that is not null
- coalesce(1,2,3) -> 1
- coalesce(null,2,3) -> 2
- coalesce(null,null,3) -> 3
*/

/*markdown
### Frame clauses:
*/

/*markdown
- can be used for rolling windows
- whwn we don't have to partition by a column
- when we need rolling aggregations
*/

/*markdown
#### Rows between
- syntax:
    - `aggregation(col) over (order by col rows between m preceding and n following and exclude current_row)` 
    - `aggregation(col) over (order by col rows between m preceding and current_row)`
- example:
    - `sum(sales_amount) over (order by sales_date rows between 3 preceding and 3 following and exclude current_row)` 
    - `sum(sales_amount) over (order by sales_date rows between 3 preceding and current_row)`
*/

/*markdown
#### Range between:
*/

/*markdown
- when we are not certain about the number of rows we want. It basically is a logical range and depends on the value of the current row in the selected column
*/

/*markdown
- Syntax:
    - `aggregate(col) over (order by col range between 100 preceding and 200 following)`
*/

/*markdown
- Example:
    - `select *, sum(sales_amount) over (order by sales_date range between interval '6' day precedinh and current row) as weekly_sales from daily_sales`
*/

/*markdown
### Count
*/

/*markdown
- count(1) and count(*) no differnce
*/

/*markdown
## Datetime
*/

/*markdown
#### DATE_ADD:
*/

/*markdown
- Syntax: `DATE_ADD(date, INTERVAL expr unit)`
- Example: `SELECT DATE_ADD('2023-01-01', INTERVAL 1 YEAR) AS new_date;`
*/

/*markdown
#### DATE_DIFF:
*/

/*markdown
- Syntax: `DATEDIFF(date1, date2)`
- Example: `SELECT DATEDIFF(end_date, start_date) AS date_difference FROM your_table;`
*/

/*markdown
#### TIMESTAMP_DIFF:
*/

/*markdown
- Syntax: `TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)`
- Example: `SELECT TIMESTAMPDIFF(HOUR, start_date, end_date) AS hour_difference
FROM your_table;`
*/

/*markdown
#### Difference bw timestamp and datetime data types in Mysql?
*/

/*markdown
In MySQL, both `TIMESTAMP` and `DATETIME` are data types used to store date and time values, but there are some key differences between them:

/*markdown
1. **Range of Values:**
   - `TIMESTAMP`: The `TIMESTAMP` data type has a narrower range of supported values compared to `DATETIME`. It supports values from '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC. It is often used for representing points in time and is affected by the system's time zone setting.
   - `DATETIME`: The `DATETIME` data type has a broader range of supported values from '1000-01-01 00:00:00' to '9999-12-31 23:59:59'. It is not affected by the system's time zone and is suitable for a wider range of historical and future dates.
*/

/*markdown
2. **Time Zone Handling:**
   - `TIMESTAMP`: Values stored in a `TIMESTAMP` column are automatically converted to the time zone set for the MySQL session when they are retrieved, and then converted back when stored. This can lead to unexpected behavior when working with different time zones.
   - `DATETIME`: `DATETIME` values are not automatically adjusted to the time zone setting, making them more suitable for cases where time zone conversion needs to be managed explicitly in the application.
*/

/*markdown
3. **Storage Requirements:**
   - Both `TIMESTAMP` and `DATETIME` require the same amount of storage, which is 4 bytes. However, `TIMESTAMP` columns also have fractional seconds support, which can add additional storage depending on the fractional seconds precision specified.
*/

/*markdown
4. **Default Values:**
   - `TIMESTAMP`: If a `TIMESTAMP` column is defined without the `DEFAULT` clause, it is automatically set to the current timestamp when a new row is inserted.
   - `DATETIME`: If a `DATETIME` column is defined without the `DEFAULT` clause, it is set to '0000-00-00 00:00:00' by default.
*/

/*markdown
In summary, the choice between `TIMESTAMP` and `DATETIME` depends on the specific requirements of your application. If you need to store a wide range of dates and times, especially historical or future dates, and you want to manage time zone conversions in your application, `DATETIME` might be a better choice. If you are dealing with recent dates and times and want MySQL to handle time zone conversions automatically, `TIMESTAMP` might be more suitable.
*/
*/

/*markdown
## UNION ALL Syntax
The UNION operator selects only distinct values by default. To allow duplicate values, use UNION ALL:
*/

/*markdown
`
SELECT column_name(s) FROM table1
UNION ALL
SELECT column_name(s) FROM table2;
`
*/

/*markdown

*/