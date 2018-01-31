package Task_1
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{mean, var_pop}


object HW4_Task1_3 {


  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val StandardPreprocessingPath = "src/main/resources/data/StandardPreprocessing.csv"

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("HW5")
      .getOrCreate

    //needed for $ notation
    import spark.implicits._

    val dataFrame = spark.read
      .option("header", "true")
      .csv(StandardPreprocessingPath)

    //3a
    val educationGroups = dataFrame.groupBy("education")
    //3b
    val educationStats = educationGroups.agg(mean("age"), var_pop("age"), var_pop("hours-per-week"))
    educationStats.show()
    //3c
    val educationPartitionsWithMeanAgeUnder40 = educationStats.where($"avg(age)" < 40).select("education")
    educationPartitionsWithMeanAgeUnder40.show()
    //3d
    val filteredData = dataFrame.join(educationPartitionsWithMeanAgeUnder40, "education")
    filteredData.show()
  }
}