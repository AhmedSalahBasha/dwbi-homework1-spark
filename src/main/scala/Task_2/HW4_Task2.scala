package Task_2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType}
import org.apache.log4j.Logger
import org.apache.log4j.Level



    /*
    * Credits to Andres Ardila
    * Github --> https://github.com/aardila
    */


case class T2NumericColumnStats(
                                 colName: String,
                                 min: Double,
                                 max: Double,
                                 mean: Double,
                                 median: Double,
                                 stddev: Double
                               )
{
  override def toString: String = {
    return f"Min: $min, Max: $max, Mean: $mean, StdDev: $stddev"
  }
}

object HW4_Task2 {

  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val TSMissingDataPath = "src/main/resources/data/ts_MissingData.csv"
    val MissingDataPath = "src/main/resources/data/MissingData.csv"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("HW5")
      .getOrCreate

    var df1 = spark.read
      .option("header", "true")
      .csv(TSMissingDataPath)

    var df2 = spark.read.
      option("header", "true")
      .csv(MissingDataPath)

    //header row: "row ID","True Temp","Temp with missings"
    val df1NumericColumns = Array[String]("True Temp","Temp with missings")
    val df1StringColumns = Array[String]("row ID")

    //header row: "row ID","integer","string","complete"
    val df2NumericColumns = Array[String]("integer")
    val df2StringColumns = Array[String]("row ID","integer","string","complete")


    println("== File: ts_MissingData ==")
    df1 = profileAndReplaceNulls(df1, df1NumericColumns, df1StringColumns)
    println()
    println("== File: MissingData.csv ==")
    df2 = profileAndReplaceNulls(df2, df2NumericColumns, df2StringColumns)
    println()

  }

  /**
    * Shows profiling information for numeric and string columns in a DataFrame
    * @param df the DataFrame to show profiling information for
    * @param numericColNames the list of columns in the DataFrame containing numeric values
    * @param strColNames the list of columns in the DataFrame containing strings
    */
  def profileAndReplaceNulls(df:DataFrame, numericColNames:Array[String], strColNames:Array[String]) : DataFrame = {
    //Get the numeric stats for all numeric columns and store in a map by column name
    val numericStatsMap = numericColNames.foldLeft(Map.empty[String, T2NumericColumnStats]) {
      (m, col) => m.updated(col, numericColStats(df, col, DoubleType))
    }
    println("== Numerical Analysis ==")
    numericStatsMap.foreach( (kv) => {
      println(f"- Column '${kv._1}' -")
      println(kv._2.toString)
    })
    println()
    println("== String Analysis ==")
    strColNames.foreach { df.groupBy(_).count().sort(desc("count")).show() }
    //Find null values
    df.columns.foreach(c => {
      val filteredColumn = df.filter(df.col(c).isNull || df.col(c) == "")
      println(f"- Column '$c' -")
      if (filteredColumn.count() > 0)
        filteredColumn.show()
      else
        println("No null or empty values")
    })

    println("Task 2 - Fill the missing values")

    println("2a")
    //Assumes what is meant is to replace the mean when null on **NUMERIC** columns
    return numericColNames.foldLeft(df) {
      (dataFlow, colName) => {
        df.withColumn(colName,
          when(col(colName).isNull, numericStatsMap.apply(colName).mean).otherwise(col(colName)))
      }
    }
  }

  def numericColStats(df:DataFrame, colName:String, typ:DataType) : T2NumericColumnStats = {
    val column = df.select(df.col(colName).cast(to = typ))
    val minMaxRow = column.agg(min(colName), max(colName), avg(colName), stddev_pop(colName)).head()
    val median = column.stat.approxQuantile(colName, Array(0.5), 0.25)(0)

    return T2NumericColumnStats(
      colName,
      minMaxRow.getAs[Double](0),
      minMaxRow.getAs[Double](1),
      minMaxRow.getAs[Double](2),
      minMaxRow.getAs[Double](3),
      median)
  }
}
