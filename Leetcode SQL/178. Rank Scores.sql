178. Rank Scores


Select t1.id, count(*) as rnk
From scores t1
where t1.score<=
group by 1
