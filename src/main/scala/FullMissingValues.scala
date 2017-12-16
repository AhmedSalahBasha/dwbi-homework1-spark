import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Imputer

object FullMissingValues {

  def main(args: Array[String]) {

    // reset spark-warehouse directory
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    System.setProperty("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")

    val spark = SparkSession.builder
      .master("local")
      .appName("FullMissingValues")
      .getOrCreate

    val file = "data/data_missing_values.csv"

    val df = spark.read
      .option("header", "true") //reading the headers
      .option("inferSchema", true)
      .format("csv")
      .load(file)


    // -------------  Question 2.A  ------------
    val cols = Array("Humidity Fraction", "Precipitation One Hour (mm)", "Pressure Altimeter (mbar)", "Wind Speed (m/s)", "Temperature (C)")
    //val cols = Array("Wind Speed (m/s)")
    val imputerMean = new Imputer()
      .setInputCols(cols)
      .setOutputCols(cols.map(c => s"${c}_imputedMean"))
      .setStrategy("mean")

    imputerMean.fit(df).transform(df).show()
    // -----------------------------------------


    // --------------  Question 2.B  -----------
    val imputerAvg = new Imputer()
      .setInputCols(cols)
      .setOutputCols(cols.map(c => s"${c}_imputedAverage"))
      .setStrategy("median")

    imputerAvg.fit(df).transform(df).show()
    // -----------------------------------------


    // --------------  Question 2.C  -----------
    val newDF = df
    val DeletedFields = newDF.na.drop()

    df.show()
    DeletedFields.show()
    println(df.count())
    println(DeletedFields.count())
    df.describe().show()
    DeletedFields.describe().show()
    // -----------------------------------------








    /*
    df.withColumn("new_Col", when($"Temp with missings".isNull, df.select(mean("Temp with missings"))
      .first()(0).asInstanceOf[Double])
      .otherwise($"Temp with missings"))*/


    //val tempMissingMean = removeAllDF.agg(avg("Temp with missings")).first()
    //println("####################"+tempMissingMean)
    //val tempMissingMap = Map("Temp with missings" -> tempMissingMean)
    //df.na.fill(tempMissingMap)

    //df.na.fill(tempMissingMap).show()
    //df.show()
    /*
    df.columns.foreach { x =>
      val meanValue = removeAllDF.agg(avg(x)).first()
      print(x, meanValue)
      //val map = Map("Temp with missings" -> meanValue)
      //df.na.fill(map) // Question 2.a
    }
    */

    /*
    val i = "";
    val colNames = Array("True Temp", "Temp with missings")

    // for loop execution with a collection
    for( i <- colNames ){
      val meanValue = removeAllDF.agg(avg(i)).first()
      val map = Map(i -> meanValue)
      df.na.fill(map).show()
    }
    */


  }

}
