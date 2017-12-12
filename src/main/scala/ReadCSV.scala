
object ReadCSV {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("ReadCSV")
      .getOrCreate

    val pathToFile = "data/SimpleFlow_with_Statistics.csv"

    val df = spark.read
      .option("header", "true") //reading the headers
      .csv(pathToFile)

    println("Starting ....")

    df.rdd.cache()
    //df.rdd.foreach(println)
    println("====================================")
    println(df.printSchema)

  }
}
