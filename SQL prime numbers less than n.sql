/*
Enter your query here.
*/
with recursive numbers as(
select 2 as n
union all
select numbers.n+1 from numbers 
where numbers.n<=1000)
,
cte as(
select distinct nm.n as num,nm.n%p.n as rem from 
numbers nm
left join numbers p on p.n<=nm.n and p.n!=nm.n
)

select group_concat(distinct num SEPARATOR '&')  from cte
where num not in (select distinct num from cte where rem=0)
order by num asc

