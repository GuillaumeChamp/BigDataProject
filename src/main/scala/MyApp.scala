import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
object MyApp {
    def main(args : Array[String]): Unit = {
        Logger.getLogger("org").setLevel(Level.WARN)
        val conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]").set("spark.executor.memory", "1g")
        val sc = new SparkContext(conf)
        //val data = sc.textFile("file:///tmp/book/98.txt")
        val data = sc.textFile("src/main/resources/98.txt")
        val numAs = data.filter(line => line.contains("a")).count()
        val numBs = data.filter(line => line.contains("b")).count()
        println(s"Lines with a: $numAs, Lines with b: $numBs")
    }
}