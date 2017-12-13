import org.apache.spark.sql.SparkSession

object Exercise1 {

  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("ReadCSV")
      .getOrCreate

    val SimpleFlowWithStatistics = "data/SimpleFlow_with_Statistics.csv"

    // read SimpleFlow_with_Statistics.csv
    val df_SFS = spark.read
      .option("header", "true") //reading the headers
      .csv(SimpleFlowWithStatistics)


    println("Starting ....")

    df_SFS.rdd.cache()

    //df.rdd.foreach(println)
    println("*********** SimpleFlow_with_Statistics SCHEMA *************** \n" + df_SFS.printSchema)
    println("====================================")



    println("===============  Question ( A )  =====================")
    df_SFS.createOrReplaceTempView("table")
    val a = spark.sql("SELECT * FROM table WHERE sex = 'Female'")
    a.show()
    println("*********** Question ( A ) | keep only those rows where sex=Female *************** ")
    println("====================================")



    println("===============  Question ( B )  =====================")
    df_SFS.createOrReplaceTempView("table")
    val b = spark.sql("SELECT * FROM table WHERE sex = 'Male'")
    b.show()
    println("*********** Question ( B ) | keep only those rows where sex=Male *************** ")
    println("====================================")



    println("===============  Question ( C )  =====================")
    df_SFS.select("education", "marital-status").show()
    println("*********** Question ( C ) | exclude all columns except for the column marital-status and education *************** ")
    println("====================================")


  }
}
