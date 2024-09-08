# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE books (
# MAGIC   name STRING,
# MAGIC   author STRING,
# MAGIC   title STRING,
# MAGIC   pages INT
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TABLE authors (
# MAGIC   name STRING,
# MAGIC   country STRING,
# MAGIC   age INT,
# MAGIC   gender STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert values into the books table
# MAGIC INSERT INTO books (name, author, title, pages) VALUES
# MAGIC ('Book1', 'Author1', 'Title1', 200),
# MAGIC ('Book2', 'Author2', 'Title2', 150),
# MAGIC ('Book3', 'Author3', 'Title3', 300);
# MAGIC
# MAGIC -- Insert values into the authors table
# MAGIC INSERT INTO authors (name, country, age, gender) VALUES
# MAGIC ('Author1', 'USA', 45, 'M'),
# MAGIC ('Author2', 'UK', 38, 'F'),
# MAGIC ('Author3', 'Canada', 50, 'M');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert more values into the books table
# MAGIC INSERT INTO books (name, author, title, pages) VALUES
# MAGIC ('Book4', 'Author4', 'Title4', 250),
# MAGIC ('Book5', 'Author5', 'Title5', 180),
# MAGIC ('Book6', 'Author6', 'Title6', 320),
# MAGIC ('Book7', 'Author1', 'Title7', 210),
# MAGIC ('Book8', 'Author2', 'Title8', 170);
# MAGIC
# MAGIC -- Insert more values into the authors table
# MAGIC INSERT INTO authors (name, country, age, gender) VALUES
# MAGIC ('Author4', 'Australia', 40, 'F'),
# MAGIC ('Author5', 'India', 35, 'M'),
# MAGIC ('Author6', 'Germany', 55, 'F'),
# MAGIC ('Author7', 'France', 60, 'M'),
# MAGIC ('Author8', 'Japan', 42, 'F');

# COMMAND ----------

(spark.readStream
      .table("azuredatabrick.default.authors")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

(spark.table("books_streaming_tmp_vw")                               
      .writeStream  
      .trigger(processingTime="2 seconds")
      .outputMode("append")
      .option("checkpointLocation", "hive_metastore.default/authorcounts")
      .table("countTable")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO authors (name, country, age, gender) VALUES
# MAGIC ('Author4', 'Australia', 32, 'F'),
# MAGIC ('Author5', 'Germany', 47, 'M'),
# MAGIC ('Author6', 'India', 29, 'F'),
# MAGIC ('Author7', 'France', 52, 'M'),
# MAGIC ('Author8', 'Spain', 36, 'F'),
# MAGIC ('Author9', 'Mexico', 41, 'M'),
# MAGIC ('Author10', 'Japan', 34, 'F');
