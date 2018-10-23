import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(new SparkConf().setMaster("local").setAppName("AAAAA"))
      .getOrCreate()

    val ds = spark.read.json("C:\\Users\\Public\\Documents\\data-students.json")

    ds.printSchema
  }
}
