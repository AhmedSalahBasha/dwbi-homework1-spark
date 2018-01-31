package Task_1

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{max, min, stddev_pop, desc}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object HW4_Task1_1 {

  def main(args: Array[String]) {

    /*
    * Credits to Andres Ardila
    * Github --> https://github.com/aardila
    */

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val SimpleFlowWithStatisticsPath = "src/main/resources/data/SimpleFlow_with_Statistics.csv"
    val CookbookBlogPath = "src/main/resources/data/CollectionCookbook.csv"
    val StandardPreprocessingPath = "src/main/resources/data/StandardPreprocessing.csv"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("HW5")
      .getOrCreate

    val simpleFlowDF = spark.read
      .option("header", "true")
      .csv(SimpleFlowWithStatisticsPath)
    //.as[SimpleFlowRecord]

    val collectionCookbookDF = spark.read.
      option("header", "true")
      .csv(CookbookBlogPath)

    val standardPreprocessingDF = spark.read
      .option("header", "true")
      .csv(StandardPreprocessingPath)

    //header row: "row ID","age","workclass","fnlwgt","education","education-num","marital-status","occupation","relationship","race","sex","capital-gain","capital-loss","hours-per-week","native-country","income"
    val simpleFlowNumericalColumns = Array[String]("age","fnlwgt","education-num","capital-gain","capital-loss","hours-per-week")
    val simpleFlowStringColumns = Array[String]("workclass","education","marital-status","occupation","relationship","race","sex","native-country","income")

    println("== File: SimpleFlow_with_Statistics.csv ==")
    profile(simpleFlowDF, simpleFlowNumericalColumns, simpleFlowStringColumns)
    println()
    println("== File: CollectionCookbook.csv ==")
    profile(collectionCookbookDF, Array("Numbers"), Array("Characters"))
    println()
    println("=== File: StandardProcessing.csv ===")
    profile(standardPreprocessingDF, simpleFlowNumericalColumns, simpleFlowStringColumns)
    println()
  }

  /**
    * Shows profiling information for numeric and string columns in a DataFrame
    * @param df the DataFrame to show profiling information for
    * @param numericColNames the list of columns in the DataFrame containing numeric values
    * @param strColNames the list of columns in the DataFrame containing strings
    */
  def profile(df:DataFrame, numericColNames:Array[String], strColNames:Array[String]) : Unit = {
    println("== Numerical Analysis ==")
    numericColNames.foreach(printProfilingNumericColumn(df, _, IntegerType))
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
  }

  /**
    * Shows profiling information (min, max, stddev and median) for a given column of a given type in a DataFrame
    * @param df the DataFrame to show profiling information or
    * @param colName the name of the numeric column to profile
    * @param typ the DataType of the numeric column to profile (e.g. IntegerType, DoubleType, etc.)
    */
  def printProfilingNumericColumn(df:DataFrame, colName:String, typ:DataType): Unit = {
    val column = df.select(df.col(colName).cast(to = typ))
    val minMaxRow = column.agg(min(colName), max(colName), stddev_pop(colName)).head()
    val median = column.stat.approxQuantile(colName, Array(0.5), 0.25)(0)
    println(f"- Column '$colName' -")
    print("Min: "+minMaxRow.getAs[DataType](0)+
      ", Max: "+minMaxRow.getAs[DataType](1)+
      ", StdDev: "+minMaxRow.getAs[Double](2))
    print(f", Median: $median")
    println()
  }
}