create database project;
use project;


select*from cust_dimen;
select*from market_fact;
select*from orders_dimen;
select*from prod_dimen;
select*from shipping_dimen;




/*
1. Join all the tables and create a new table called combined_table.
(market_fact, cust_dimen, orders_dimen, Ord_id, shipping_dimen)
*/
-- M1
create table combined_table as (
select m.*,c.Customer_Name,c.Province, c.Region, c.Customer_Segment,
o.Order_ID, o.Order_Date,o.Order_Priority,
p.Product_Category,p.Product_Sub_Category,
s.Ship_Mode,s.ship_Date
from cust_dimen c  
join market_fact m using(cust_id)
join orders_dimen o using(ord_id) 
join prod_dimen p using(prod_id)
join shipping_dimen s using(ship_id));

-- M2
select*from market_fact m join cust_dimen c on m.Cust_id=c.Cust_id 
join orders_dimen o on m.Ord_id= o.Ord_id join shipping_dimen s on 
s.ship_id=m.Ship_id join prod_dimen p on  p.Prod_id=m.Prod_id ;

/*select*from market_fact m join cust_dimen c on m.Cust_id=c.Cust_id 
join orders_dimen o on m.Ord_id= o.Ord_id join shipping_dimen s on 
s.ship_id=m.Ship_id join prod_dimen p on  p.Prod_id=m.Prod_id;
*/



-- 2.Find the top 3 customers who have the maximum number of orders
with temp as
(select cust_id , count(Ord_id) num_orders from market_fact group by cust_id order by 
count(Ord_id) desc limit 3)
select*from temp t join cust_dimen c on t.cust_id=c.cust_id;

select distinct*from
(select cust_id,Customer_Name,orders,dense_rank()over(order by orders desc) rnk from
(select c.cust_id,Customer_Name, count( Ord_id)over(partition by cust_id ) orders from market_fact m join cust_dimen c 
on m.Cust_id=c.Cust_id )t)t2
where rnk<=3;
 


/*
3.	Create a new column DaysTakenForDelivery that contains the date difference 
of Order_Date and Ship_Date.
*/

select*from orders_dimen;
select*from shipping_dimen;


select*, datediff(str_to_date(Ship_Date,'%d-%m-%Y'),str_to_date(Order_Date,'%d-%m-%Y'))
DaysTakenForDelivery
 from orders_dimen o join shipping_dimen s 
on o.Order_ID=s.Order_ID;

-- 4.Find the customer whose order took the maximum time to get delivered.

select *from(
select c.Customer_Name,c.Cust_id,s.Order_ID, datediff(str_to_date(Ship_Date,'%d-%m-%Y'),str_to_date(Order_Date,'%d-%m-%Y'))
DaysTakenForDelivery
 from orders_dimen o join shipping_dimen s join market_fact m on m.Ship_id=s.Ship_id 
join cust_dimen c on m.Cust_id=c.Cust_id
on o.Order_ID=s.Order_ID)temp
order by DaysTakenForDelivery desc limit 1 ;

/*
5. Retrieve total sales made by each product from the data (use Windows function)*/

select distinct Prod_id,total_sale from(
select*, sum(sales)over(partition by prod_id)total_sale from market_fact)temp;

/*
6. Retrieve total profit made from each product from the data (use windows function)
*/

select distinct Prod_id,sum(profit)over(partition by prod_id) as profit from market_fact;

/*
7. Count the total number of unique customers in January and how many of 
them came back every month over the entire year in 2011*/
select*from cust_dimen;
select*from market_fact;
select*from orders_dimen;
select*from prod_dimen;
select*from shipping_dimen;

select count(cust_id) from
market_fact 
join orders_dimen using(ord_id) 
where  month(str_to_date(order_date,'%d-%m-%Y'))=1 and year(str_to_date(order_date,'%d-%m-%Y'))=2011;

select count(cust_id) as CustVisitMonth from (
select m.cust_id,count(distinct month(str_to_date(order_date,'%d-%m-%Y'))) as months
from orders_dimen join market_fact m using(ord_id)
where year(str_to_date(order_date,'%d-%m-%Y'))=2011
group by cust_id
having months >= 12) custvisit;

##8.	Retrieve month-by-month customer retention rate since the start of the business.(using views)
/*1). Create a view where each user’s visits are logged by month, allowing for the possibility that these will have occurred
over multiple # years since whenever business started operations
*/

create view customer_visit as
select cust_id,str_to_date(order_date,"%d-%m-%Y") cust_visit 
from combined_table;

select * from customer_visit;

-- 2. Identify the time lapse between each visit. So, for each person and for each month, we see when the next visit is.

create view customer_monthly_visit_timelamp as
select cust_id,cust_visit, lag(cust_visit) over(partition by cust_id 
order by cust_visit) previous_visit_month
from customer_visit cm ;

select * from customer_monthly_visit_timelamp;



-- 3) Calculate the time gaps between visits

create view customer_time_gaps as
select cust_id, cust_visit, previous_visit_month,round(datediff(cust_visit,previous_visit_month)/30) month_diff from customer_monthly_visit_timelamp;


select* from customer_time_gaps;

-- 4). categorise the customer with time gap 1 as retained, >1 as irregular and NULL as churned

create view customer_categorise as 
select cust_id, cust_visit, 
case when month_diff = 1 then "retained"
     when month_diff  >1 then "irregular"
     else "churned"
end retention_status
from customer_time_gaps  ;

select * from customer_visit;
select * from customer_visit_timelamp;
select * from customer_time_gaps;
select * from customer_categorise;

-- 5: calculate the retention month wise

select year(cust_visit),month(cust_visit), count(cust_id) total_customer, 
sum(case when retention_status = "retained" then 1 else 0 end) as retained_customer
from customer_categorise group by year(cust_visit), month(cust_visit) order by year(cust_visit),month(cust_visit);


