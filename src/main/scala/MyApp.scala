import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}


object MyApp {

    def main(args : Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)

        val conf = new SparkConf().setAppName("FlightDelayLearning").setMaster("local[2]").set("spark.executor.memory", "1g")
        new SparkContext(conf)

        val spark = SparkSession
            .builder()
            .appName("Spark SQL example")
            .config("some option", "value")
            .getOrCreate()

        val dataframe = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
            .csv("src/main/resources/1998.csv")

        //val dropped = ("ArrTime","ActualElapsedTime","AirTime","TaxiIn","TaxiOut","Cancelled","CancellationCode","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")

        val filtered = dataframe.drop(
            "ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")
        val pattern = "^[0-9]"
        val filtered_regex = filtered.filter(_.getString(4).matches(pattern)).filter(_.getString(10).matches(pattern)).filter(_.getString(11).matches(pattern)).filter(_.getString(12).matches(pattern))
            .select(filtered.columns.map {
            case column@"DepTime" =>
                col(column).cast("int").as(column)
            case column@"CRSElapsedTime" =>
                col(column).cast("int").as(column)
            case column@"ArrDelay" =>
                col(column).cast("int").as(column)
            case column@"DepDelay" =>
                col(column).cast("int").as(column)
            case column =>
                col(column)
        }: _*)

        filtered_regex.printSchema()
    }
}