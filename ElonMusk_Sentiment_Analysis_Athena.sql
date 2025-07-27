-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Big Data Project: Twitter Sentiment Analysis in ATHENA (SQL Queries)
-- MAGIC Laiba Shah, Part Time Data Science Bootcamp

-- COMMAND ----------

--CALLING RAW DATA TABLE SAVED AFTER CLEANING TWEETS DATA FROM SENTIMENT ANALYSIS
SELECT * FROM rawdatatable;

-- COMMAND ----------

--CALLING ELON_MUSK PREDICTIONS TABLE SAVED FROM LOGISTIC REGRESSION ANALYSIS
SELECT * FROM em_predictions;

-- COMMAND ----------

--TWEET COUNT GROUPED BY SENTIMENT CATEGORY
SELECT sentiment, COUNT(*) as tweet_count
FROM rawdatatable
GROUP BY sentiment;

-- COMMAND ----------

--USING FILTERED ARRAY COLUMN FROM PREDICTIONS DATASET TO CREATE A LIST OF WORDS AND THEIR COUNT TO USE FOR WORDCLOUD
CREATE TABLE words AS(
SELECT word, COUNT(word) AS word_count
FROM em_predictions,unnest(filtered) AS t(word) --unnest the array and save each word as a row
GROUP BY word); --group and save total count per word for cloud

-- COMMAND ----------

--TOP 5 HANDLES CONTRIBUTING TO EACH TWEET SENTIMENT AND THEIR FOLLOWER COUNTS
CREATE TABLE top5handles as(
WITH rws AS(
select sentiment, handle, MAX(follower_count) follower_count, COUNT(*) tweets,
row_number () over (PARTITION BY sentiment ORDER BY COUNT(*) DESC) rn
FROM rawdatatable
GROUP BY sentiment, handle)
SELECT * FROM rws
WHERE rn <= 5
ORDER BY sentiment, rn); --use nested selected statement, row number and partition to rank handles within each category by tweet count


-- COMMAND ----------

--TOP 10 HANDLES BY FOLLOWER COUNT WITHIN EACH SENTIMENT CATEGORY
CREATE TABLE top10handles AS(
SELECT sentiment, handle, follower_count, RANKING
FROM(SELECT sentiment, handle, follower_count, RANK() OVER (PARTITION BY sentiment ORDER BY sentiment, follower_count desc) RANKING
FROM rawdatatable)
WHERE RANKING <=10
ORDER BY sentiment,RANKING DESC); --use nested select statement, rank and partition to rank handles within each category by follower count

-- COMMAND ----------

--DETERMINING WHAT PORTION OF THE PREDICTIONS WERE ACCURATE V. INACCURATE BY SENTIMENT CATEGORY
CREATE TABLE QC_sent AS(
SELECT sentiment_category, QC, COUNT(*) AS accuracy_count FROM(
SELECT sentiment_category, label,prediction,
CASE --case then to label each row as an accurate or inaccurate prediction
    WHEN label-prediction = 0 THEN 'Accurate'
    ELSE 'Inaccurate'
END AS QC
FROM em_predictions)
GROUP BY sentiment_category, QC
ORDER BY sentiment_category);--use nested select to label rows and group by sentiment category

-- COMMAND ----------

--CLEANING THE CREATED_AT STRING AND CONVERTING TO DATETIME ATTRIBUTE TO THEN GROUPING THE DATA BY SENTIMENT CATEGORY, DATE AND HOUR
CREATE TABLE datetime_sent as(
SELECT sentiment, new_date, date(new_date) as date, hour(new_date) AS hour, COUNT(*) AS no_tweets FROM(
SELECT created_at, sentiment, date_parse(created_at, '%a %b %d %H:%i:%S +0000 %Y') AS new_date --use date_parse to convert string to datetime
FROM rawdatatable
WHERE created_at not like 'Tue Nov 22 07:38:43 +0000 2022"' AND created_at LIKE '% Nov %') --clean date column and remove all unnecessary garbage values
GROUP BY sentiment, new_date, HOUR(new_date));
