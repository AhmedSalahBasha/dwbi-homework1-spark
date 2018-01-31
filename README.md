<h1>**SPARK**<h1>



<h2>Task - 1</h2>
1- Profile the SimpleFlow_with_Statistics ,CollectionCookbook_blog, and StandardPreprocessing datasets:
<ol>
  <li>
a. Find Minimum, maximum, average, median, and standard variation of the values of numerical columns. Find
distinct values of String columns along with its count. Find the null values. Find the max and min data for data
columns.
  </li>
</ol>
2. Read SimpleFlow_with_Statistics.
<ol>
  <li>Filter rows and keep only those rows where sex=Female.</li>
  <li>Filter rows and keep only those rows where sex=Male.</li>
  <li>Filter Column of b. and exclude all columns except for the column marital-status and education.</li>
  <li>Groupby after the Column Filtering and group by marital-status; use count as the aggregation method on
  another column.</li>
  <li>Join the aggregated values to the output data table of the Column Filter node. Use marital-status as the join
  column.</li>
  </ol>
3. Open and execute the workflow StandardPreprocessing.
<ol>
<li>GroupBy the education column and compute the mean and the variance of the age and hours-per-week
  columns.</li>
<li>Filter rows after the GroupBy and filter all rows (education) with a mean age of less than 40.</li>
<li>Filter Row or the original dataset and include all rows with education values that are contained in the output
  data table of the b.</li>
  </ol>


<h2>Task - 2</h2>
1- Profile the TS_MissingValues and full_data_missing_values datasets:
<ol>
  
<li>Find Minimum, maximum, average, median, and standard variation of the values of numerical columns. Find distinct
values of String columns along with its count. Find the null values. Find the max and min data for data columns.</li>
  </ol>
2. Fill the missing values
<ol>
  <li>Insert the mean value as the missing value</li>
  <li>Insert the average value in the field.</li>
  <li>Delete the field.</li>
<li>Calculate the RMSE (root-mean-square error) from the original data without missing values full the cases above.</li>
  </ol>
