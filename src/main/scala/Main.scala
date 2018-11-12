import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{udf, when, col}
import java.util.Date
import java.text.SimpleDateFormat

object clickinBad {
  /* ----------------------------------------- */
  /* -------------- Preprocessing ------------ */
  /* ----------------------------------------- */
    def preprocess(df: DataFrame): DataFrame = {
        val network = udf { // getting countries from networks
            (str: String) =>
                val fr = new Regex("208-(.*)")
                val can = new Regex("302-(.*)")
                val es = new Regex("214-(.*)")
                val tur = new Regex("286-(.*)")
                val ger = new Regex("262-(.*)")

                str match {
                case fr(x) => "France"
                case null | "Other" | "Unknown" => "Unknown"
                case can(x) => "Canada"
                case es(x) => "Espagne"
                case tur(x) => "Turquie"
                case ger(x) => "Allemagne"
            }
        }

        // Replacing null values by "Unknown"s

        // Removing all add with size null
        //val ds2 = unknown.filter(!col("size").getItem(0).isNull).filter(!col("size").getItem(1).isNull)

        // Gathering all windows phones together, set nulls & "Other" to "Unknown"
        val os = udf {
            (str: String) =>
                val windows_pattern = new Regex("(.*)indows(.*)")
                
                str match {
                case windows_pattern(x) => "Windows Phone"
                case "ios" | "iOS" => "iOS"
                case "Rim" | "Bada" | "WebOS"| "Symbian" | "BlackBerry" | null => "Unknown"
                case x => x.toLowerCase.capitalize
            }
        }

        val df2 = df.select("appOrSite","interests","media","type","bidfloor","label","os","network","timestamp","size")

        val df3 = 
      {df2.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor")))
      .withColumn("label", when(col("label") === true, 1).otherwise(0))
      .withColumn("os", os(df2("os")))
      .withColumn("network", network(df2("network")))
      .withColumn("timestamp", epochToDate(df2("timestamp")))} 

        

        df3 // return clean dataframe
    }

    def epochToDate = udf((epochMillis: Long) => {
            val df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            df.format(epochMillis)
    })

    def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(new SparkConf().setMaster("local").setAppName("AAAAA"))
      .getOrCreate()

    val ds = spark.read.json("/home/cyp/IG/WI/data-students.json")

    val ds2 = preprocess(ds)
    //ds2.summary().show
    ds2.groupBy("os").count().show()

    print("ICIII \n" + epochToDate(ds2.col("timestamp").getItem(0)).toString() +"\n")

    spark.close()
  }
}