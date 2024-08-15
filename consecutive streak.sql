
-- CREATE or replace TABLE Test.user_events (
--     user_id INT64 NOT NULL,
--     event_time DATETIME NOT NULL,
--     flag INT64 NOT NULL,
--     -- PRIMARY KEY (user_id, event_time)


-- );


-- INSERT INTO Test.user_events (user_id, event_time, flag)
-- VALUES
-- (1, '2024-08-10 12:00:00', 0),
-- (1, '2024-08-10 12:30:00', 1),
-- (1, '2024-08-10 12:31:00', 1),
-- (1, '2024-08-10 12:35:00', 0),
-- (1, '2024-08-10 12:37:00', 1),
-- (1, '2024-08-10 12:38:00', 1),
-- (1, '2024-08-10 12:39:00', 1),
-- (2, '2024-08-11 09:15:00', 1),
-- (2, '2024-08-11 09:25:00', 1),
-- (2, '2024-08-11 09:35:00', 1),
-- (2, '2024-08-11 09:45:00', 1),
-- (2, '2024-08-11 10:15:00', 0),
-- (2, '2024-08-11 10:25:00', 1),
-- (2, '2024-08-11 10:35:00', 1),
-- (3, '2024-08-12 14:30:00', 1),
-- (3, '2024-08-12 15:00:00', 1),
-- (4, '2024-08-13 16:00:00', 0),
-- (4, '2024-08-13 16:30:00', 0),
-- (4, '2024-08-13 17:00:00', 1);

-- select * from Test.user_events
-- order by user_id ,event_time;


-- -- -- select user_id, sum(case when  )

-- -- select * from

-- -- 1 -2
-- -- 2 - 4
-- -- 3 - 2
-- -- 4- 1


with cte as (
  select user_id,event_time,flag,lag(flag) over(partition by user_id order by event_time ) as prev_flag
  from Test.user_events
),
 final as (
select user_id,event_time,sum(case when flag != prev_flag then 1 else 0 end) over(partition by user_id order by event_time) as group_key
 from cte
order by user_id
 )

select user_id,group_key,min(event_time),max(event_time), count(group_key) - 1
from final group by 1,2
order by user_id, group_key


-----final answer

--  select *
--   from Test.user_events
--   order by user_id,event_time

  WITH Streaks AS (
  SELECT 
    user_id,
    event_time,
    flag,
    -- Identify where a new streak starts
    CASE 
      WHEN flag = 1 AND (LAG(flag, 1, 0) OVER (PARTITION BY user_id ORDER BY event_time)) = 0 THEN 1
      ELSE 0 
    END AS new_streak
  FROM Test.user_events
),
-- Assign a streak ID by cumulative sum of new streaks
StreaksWithID AS (
  SELECT 
    user_id,
    event_time,
    flag,
    SUM(new_streak) OVER (PARTITION BY user_id ORDER BY event_time) AS streak_id
  FROM Streaks
  WHERE flag = 1 -- Consider only winning events
)
-- Count the number of events in each streak and get the maximum streak length per user
SELECT 
  user_id,
  MAX(streak_length) AS max_streak
FROM (
  SELECT 
    user_id,
    streak_id,
    COUNT(*) AS streak_length
  FROM StreaksWithID
  GROUP BY user_id, streak_id
) streaks_per_user
GROUP BY user_id
order by user_id;
----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------
--approach 2

WITH NumberedEvents AS (
  SELECT 
    user_id,
    event_time,
    flag,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) AS rn1,
    -- Assign row numbers for only the winning events
    ROW_NUMBER() OVER (PARTITION BY user_id, flag ORDER BY event_time) AS rn2
  FROM Test.user_events
),
Streaks AS (
  SELECT 
    user_id,
    flag,
    rn1 - rn2 AS streak_group
  FROM NumberedEvents
  WHERE flag = 1 -- We are only interested in winning events
),
final as(
-- Count the number of events in each streak group and get the maximum streak length per user
SELECT 
  user_id,
  COUNT(*) AS max_streak
FROM Streaks
GROUP BY user_id, streak_group
ORDER BY user_id
)

select user_id,max(max_streak)
from final
group by 1
order by user_id
