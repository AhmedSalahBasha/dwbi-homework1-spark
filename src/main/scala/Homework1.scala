import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql


object Homework1 {
  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ReadCSV")
      .getOrCreate

    val SimpleFlowWithStatistics = "data/SimpleFlow_with_Statistics.csv"
    val CollectionCookbook = "data/CollectionCookbook.csv"
    val StandardPreprocessing = "data/StandardPreprocessing.csv"

    // read SimpleFlow_with_Statistics.csv
    val dfSFS = spark.read
      .option("header", "true") //reading the headers
      .csv(SimpleFlowWithStatistics)

    // read CollectionCookbook.csv
    val dfCC = spark.read
      .option("header", "true") //reading the headers
      .csv(CollectionCookbook)

    // read StandardPreprocessing.csv
    val dfSP = spark.read
      .option("header", "true") //reading the headers
      .csv(StandardPreprocessing)


    println("Starting ....")

    dfSFS.rdd.cache()
    dfCC.rdd.cache()
    dfSP.rdd.cache()

    //df.rdd.foreach(println)
    println("====================================")
    println("*********** SimpleFlow_with_Statistics SCHEMA *************** ")
    println(dfSFS.printSchema)
    println("====================================")
    println("*********** CollectionCookbook SCHEMA *************** ")
    println(dfCC.printSchema)
    println("====================================")
    println("*********** StandardPreprocessing SCHEMA *************** ")
    println(dfSP.printSchema)

    println("### SimpleFlow_with_Statistics Age Minimum ###")
    dfSFS.agg(min("age")).show()

    println("### SimpleFlow_with_Statistics Age Maximum ###")
    dfSFS.agg(max("age")).show()

    println("### SimpleFlow_with_Statistics fnlwgt Minimum ###")
    dfSFS.agg(min("fnlwgt")).show()

    println("### SimpleFlow_with_Statistics fnlwgt Maximum ###")
    dfSFS.agg(max("fnlwgt")).show()

    println("### SimpleFlow_with_Statistics education Minimum ###")
    dfSFS.agg(min("education-num")).show()

    println("### SimpleFlow_with_Statistics education Maximum ###")
    dfSFS.agg(max("education-num")).show()

    println("### SimpleFlow_with_Statistics education distinct values ###")
    dfSFS.groupBy("education").count().show()

    println("### SimpleFlow_with_Statistics relationship distinct values ###")
    dfSFS.groupBy("relationship").count().show()

    println("### SimpleFlow_with_Statistics occupation distinct values ###")
    dfSFS.groupBy("occupation").count().show()

    println("### SimpleFlow_with_Statistics workclass distinct values ###")
    dfSFS.groupBy("workclass").count().show()

    println("### SimpleFlow_with_Statistics race distinct values ###")
    dfSFS.groupBy("race").count().show()

    println("### SimpleFlow_with_Statistics native-country distinct values ###")
    dfSFS.groupBy("native-country").count().show()


    // =============     2      =================== //
    // #############     a      ################### //

    //println("### SimpleFlow_with_Statistics filter rows with Female only ###")

    //dfSFS.filter(col("sex") === "Female").show()




  }
}
