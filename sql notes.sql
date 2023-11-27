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
 - Just like index page in a book
 - we don't have to scan the unnecessaery data
 - fast
 - records are stored in contiguous memory locations
 - why slow quwry?:
    - extra seek time.
    -  most od the time in searching
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
    - `window_function/aggregation() over (partition by c1,c2 order by c1,c2) `
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
- first_calue(col)
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
- when we are not certain about the number of rows we want. It basically is a logical range adn depends on the value of the current row in the selected column
*/

/*markdown
- Syntax:
    - `aggregate(col) over (order by col range between 100 preceding and 200 following)`
*/

/*markdown
- Example:
    - `select *, sum(sales_amount) over (order by sales_date range between interval '6' day precedinh and current row) as weekly_sales from daily_sales`

/*markdown
### Count
*/

/*markdown
- count(1): gives all rows including the one with all nulls
- count(*): gives all rows excluding the ones with all nulls
*/

/*markdown
## Datetime
*/

/*markdown
#### DATE_ADD:

- Syntax: `DATE_ADD(date, INTERVAL expr unit)`
- Example: `SELECT DATE_ADD('2023-01-01', INTERVAL 1 YEAR) AS new_date;`
*/

/*markdown
#### DATE_DIFF:

- Syntax: `DATEDIFF(date1, date2)`
- Example: `SELECT DATEDIFF(end_date, start_date) AS date_difference FROM your_table;`
*/

/*markdown
#### TIMESTAMP_DIFF:

- Syntax: `TIMESTAMPDIFF(unit, datetime_expr1, datetime_expr2)`
- Example: `SELECT TIMESTAMPDIFF(HOUR, start_date, end_date) AS hour_difference
FROM your_table;`

*/

/*markdown

*/

/*markdown

*/