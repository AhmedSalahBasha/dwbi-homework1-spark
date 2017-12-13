# dwbi-homework1-spark
First Assignment with Spark - Data Warehousing &amp; Business Intelligence
*Homework.4*

Exercise 1:

#SimpleFlow_with_Statistics.
- Connect a Row Filter node after the File Reader node and keep only those rows where sex=Female.
- Connect a Nominal Value Row Filter node after the File Reader node and keep only those rows where sex=Male.
- Connect a Column Filter node to the new Row Filter node and exclude all columns except for the column marital-status and education.
- Connect a Groupby node after the new Column Filter and group by marital-status; use count as the aggregation method on another column.
- Join the aggregated values to the output data table of the Column Filter node. Use marital-status as the join column.

Exercise from here: https://www.knime.com/knime-online-self-training-lesson-3
