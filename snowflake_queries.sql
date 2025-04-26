--creting a table to store json data from s3 (connection from s3)
CREATE OR REPLACE TABLE yelp_reviews (review_data VARIANT);

--copying data from s3 to snowflake for querying
COPY INTO yelp_reviews
FROM 's3://fulldatapipelineproject/yelp/'
CREDENTIALS = (
  AWS_KEY_ID = 'acess key',
  AWS_SECRET_KEY = 'secret access key aws'
)
FILE_FORMAT = (TYPE = JSON)
PATTERN = '.*DAP.*\.json'
REGION = 'ap-south-1';

use database SQL_DA;

-- creating a new table converting json table to structured form
create or replace table tbl_yelp_reviews as
select review_data: business_id :: string as business_id,
        review_data: date :: date as review_date,
        review_data: user_id :: string as user_id,
        review_data: stars :: number as review_stars,
        review_data: text :: string as review_text, 
        analyze_sentiment(review_text) as sentiments
from yelp_reviews;

select * from tbl_yelp_reviews limit 10;

CREATE OR REPLACE TABLE yelp_businesses (business_data VARIANT);

--copying business data from s3
COPY INTO yelp_businesses
FROM 's3://fulldatapipelineproject/yelp/'
CREDENTIALS = (
  AWS_KEY_ID = 'access key',
  AWS_SECRET_KEY = 'secret access key aws'
)
FILE_FORMAT = (TYPE = JSON)
PATTERN = '.*yelp_academic_dataset_business\.json'
REGION = 'ap-south-1';

--creating new table to convert business json data to structured tabular form
create or replace table tbl_yelp_businesses as
select business_data: business_id :: string as business_id,
        business_data : city :: string as city,
        business_data : state :: string as state,
        business_data : stars :: number as stars,
        business_data : review_count :: string as review_count,
        business_data:categories :: string as categories,
from yelp_businesses;

select * from tbl_yelp_businesses limit 10;

--SQL Queries--
-- Question 1 Find the number of businesses in each category
-- Firstly, treating the comma separated values in categories column

with cte as (
select trim(A.value) as category, business_id
from tbl_yelp_businesses,
lateral split_to_table(categories, ',')  A)
select category , count(*) No_of_business
from cte 
group by category
order by No_of_business desc;


-- Question 2 select top 10 users who have reviewed most business in restraunt category.

select R.user_id, count(distinct r.business_id) restraunt_reviewed
from tbl_yelp_reviews R
inner join tbl_yelp_businesses B 
on R.business_id = B.business_id
where B.categories ilike '%restaurant%'
group by 1
order by 2 desc
limit 10;


-- Question 3 Find the most popular categories of business (based on the number of reviews)

with cte as (
select trim(A.value) as category, business_id
from tbl_yelp_businesses,
lateral split_to_table(categories, ',')  A)
select cte.category, count(*) as review_count
from cte inner join tbl_yelp_reviews R
on cte.business_id=R.business_id
group by category
order by review_count desc;


-- Question 4 Find the top 3 most recent reviews for each business.

with cte as (
select R.*, B.business_id,
row_number() over(partition by R.business_id order by review_date desc) as rn
from tbl_yelp_businesses B inner join tbl_yelp_reviews R
on B.business_id=R.business_id)
select * from cte
where rn<=3;

-- Question 5 Find the month with highest number of reviews.

select month(review_date) as review_month, count(*) as no_of_reviews
from tbl_yelp_reviews 
group by review_month
order by no_of_reviews desc;

-- Question 6 Find percentage of five star reviews for each business

select business_id,count(*) as total_reviews,
sum(case when review_stars=5 then 1 else 0 end) as star5_review,
star5_review*100/total_reviews as per_fivestar
from tbl_yelp_reviews 
group by business_id ;



-- Question 7 Find top 5 most reviewed businesses in each city
with cte as(
select city, B.business_id, count(*) as no_of_reviews
from tbl_yelp_reviews R inner join tbl_yelp_businesses B
on R.business_id = B.business_id
group by city, B.business_id)
select * from cte
qualify row_number() over(partition by city order by no_of_reviews desc) <= 5;


-- Question 8 Find the average rating of businesses that have atleast 100 reviews.

select B.business_id, count(*) as no_of_reviews,
avg(R.review_stars) as avg_rating,
from tbl_yelp_businesses B inner join tbl_yelp_reviews R
on B.business_id=R.business_id
group by B.business_id
having no_of_reviews >= 100;

-- Question 9 List the top ten users who have writen most number of reviews along with the businesses they reviewed.
with cte as (
select R.user_id , count(*) as reviews
from tbl_yelp_reviews R inner join tbl_yelp_businesses B
on R.business_id=B.business_id
group by user_id
order by reviews desc
limit 10)
select user_id,business_id from tbl_yelp_reviews where user_id in (select user_id from cte)
group by user_id, business_id
order by user_id;



-- Question 10 fing top 10 businesses with highest positive sentiment reviews.

select B.business_id , count(*) as Positive_reviews
from tbl_yelp_reviews R inner join tbl_yelp_businesses B
on R.business_id=B.business_id
where sentiments='Positive'
group by B.business_id
order by 2 desc
limit 10;