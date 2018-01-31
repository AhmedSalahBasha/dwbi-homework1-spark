package Task_1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object HW4_Task1_2 {

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

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("HW5")
      .getOrCreate

    //needed for $ notation
    import spark.implicits._

    val simpleFlowDF = spark.read
      .option("header", "true")
      .csv(SimpleFlowWithStatisticsPath)

    //2a
    val femaleRecords = simpleFlowDF.filter("sex == 'Female'") //string condition
    femaleRecords.show()

    //2b
    //val maleRecords = simpleFlowDF.filter(simpleFlowDF("sex") === "Male") //with a column indexer on the DataFrame
    val maleRecords = simpleFlowDF.filter($"sex" === "Male") //with $ notation
    maleRecords.show()

    //2c
    val maleProjection = maleRecords.select("marital-status", "education")
    maleProjection.show()

    //2d
    val maleMaritalStatusCounts = maleProjection.groupBy("marital-status").count().orderBy("count")
    maleMaritalStatusCounts.show()

    //2e
    val maleProjectionJoined = maleMaritalStatusCounts.join(maleProjection, "marital-status")
    maleProjectionJoined.show()
  }
}