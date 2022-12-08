import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object MyApp {

    def main(args : Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf().setAppName("FlightDelayLearning").setMaster("local[2]").set("spark.executor.memory", "1g")
        val sc = new SparkContext(conf)

        val spark = SparkSession
            .builder()
            .appName("Spark SQL example")
            .config("some option", "value")
            .getOrCreate()

        val dataframe = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
            .csv("src/main/resources/1998.csv")
        dataframe.printSchema()

        //val dropped = ("ArrTime","ActualElapsedTime","AirTime","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")

        val filtered = dataframe.drop(
            "ArrTime","ActualElapsedTime","AirTime","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")
        filtered.printSchema()

    }

}