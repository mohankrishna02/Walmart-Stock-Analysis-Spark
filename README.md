## Walmart Analysis using Spark
This code performs analysis on a Walmart stock dataset using Spark. It utilizes various Spark functions to explore and manipulate the data. Here is a breakdown of the tasks performed:

### Reading the dataset:
* The code reads a CSV file containing Walmart stock data into a DataFrame.
### Data exploration:
* Printing the number of rows in the DataFrame.
* Printing the column names.
* Printing the schema of the DataFrame.
* Printing the first 5 lines of the DataFrame.
* Using the describe() function to generate summary statistics of the DataFrame.
* Formatting the decimal places of the summary statistics to two decimal places.

### Data manipulation:
* Converting certain columns to decimal type.
* Creating a new column called "HV Ratio" that represents the ratio of the High Price to the volume of stock traded for a day.
### Calculating statistics:
* Computing the mean of the "Close" column.
* Finding the maximum and minimum values of the "Volume" column.
* Counting the number of days where the "Close" price was lower than $60.
* Calculating the percentage of time the "High" price was greater than $80.
* Finding the maximum "High" price for each year.
* Calculating the average "Close" price for each calendar month.
* This code can be used as a starting point for analyzing and gaining insights from the Walmart stock dataset using Spark.

### Dataset:
* You can get the datasets from the kaggle :- <https://www.kaggle.com/> 

### Solution:
* Scala-Spark :- <https://github.com/mohankrishna02/Walmart-Stock-Analysis-Spark/blob/main/ScalaSpark/pack/WalmartStockAnalysis.scala> <br>
* PySpark :- <https://github.com/mohankrishna02/Walmart-Stock-Analysis-Spark/blob/main/PySpark/Walmart_Stock_Analysis.py>

### Documentation:
* Spark :- <https://spark.apache.org/docs/latest/> 


