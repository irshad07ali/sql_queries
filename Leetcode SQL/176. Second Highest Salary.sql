176. Second Highest Salary

# Write your MySQL query statement below


select(
select distinct salary from Employee order by salary DESC limit 1 offset 1
) as SecondHighestSalary