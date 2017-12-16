package Task_1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, mean, min}

object SimpleFlowWithStatistics {

  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Task_1")
      .getOrCreate

    val SimpleFlowWithStatistics = "data/SimpleFlow_with_Statistics.csv"

    // read SimpleFlow_with_Statistics.csv
    val df_SFS = spark.read
      .option("header", "true") //reading the headers
      .option("inferSchema", true)
      .format("csv")
      .load(SimpleFlowWithStatistics)



    // -------------  Question 1.A  ------------
    df_SFS.columns.foreach { x =>
      val minValue = df_SFS.agg(min(x)).first()
      print("Minimum Value For: >> "+x, minValue)
    }

    df_SFS.columns.foreach { x =>
      val maxValue = df_SFS.agg(max(x)).first()
      print("Maximum Value For: >> "+x, maxValue)
    }

    df_SFS.columns.foreach { x =>
      val avgValue = df_SFS.agg(avg(x)).first()
      print("Average Value For: >> "+x, avgValue)
    }

    df_SFS.columns.foreach { x =>
      val meanValue = df_SFS.agg(mean(x)).first()
      print("Median Value For: >> "+x, meanValue)
    }

    df_SFS.columns.foreach { x =>
      val distinctValue = df_SFS.groupBy(x).count().show()
      print("Distinct Value For: >> "+x, distinctValue)
    }
    // -----------------------------------------


    df_SFS.createOrReplaceTempView("table")
    val a = spark.sql("SELECT * FROM table WHERE sex = 'Female'")
    a.show()
    println("-->> Question ( 2.A ) | keep only those rows where sex=Female *************** ")
    println("====================================")


    df_SFS.createOrReplaceTempView("table")
    val b = spark.sql("SELECT * FROM table WHERE sex = 'Male'")
    b.show()
    println("-->> Question ( 2.B ) | keep only those rows where sex=Male *************** ")
    println("====================================")


    val c = b.select("education", "marital-status")
    c.show()
    println("-->> Question ( 2.C ) | exclude all columns from (B) except for the column marital-status and education *************** ")
    println("====================================")


    val d = c.groupBy("marital-status").count()
    d.show()
    println("-->> Question ( 2.D ) | group by marital-status and use count as the aggregation method *************** ")
    println("====================================")


    val e = d.join(c, "marital-status")
    e.show()
    println("-->> Question ( 2.E ) | Join the aggregated values to the output data table of the Column Filter node. Use marital-status as the join column *************** ")
    println("====================================")



  }
}
