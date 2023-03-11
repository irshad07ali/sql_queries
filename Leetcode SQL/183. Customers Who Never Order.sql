183. Customers Who Never Order

# Write your MySQL query statement below
# Write your MySQL query statement below

SELECT Name as Customers from Customers
LEFT JOIN Orders
ON Customers.Id = Orders.CustomerId
WHERE Orders.CustomerId IS NULL;
