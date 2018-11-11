import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{udf, when, col}

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
                case _ => "Unknown"
            }
        }

        // Replacing null values by "Unknown"s
        val unknown = df.na.fill(Map("city" -> "Unknown","impid" -> "Unknown","interests" -> "Unknown","network" -> "Unknown","type" -> "Unknown", "os" -> "Unknown"))

        // Removing all add with size null
        val ds2 = unknown.filter(!col("size").getItem(0).isNull).filter(!col("size").getItem(1).isNull)

        // Gathering all windows phones together, set nulls & "Other" to "Unknown"
        val os = udf {
            (str: String) =>
                val windows_pattern = new Regex("Windows(.*)")
                
                str match {
                case windows_pattern(x) => "Windows Phone"
                case "ios" | "iOS" => "iOS"
                case "Rim" | "Bada" | "WebOS"| "Symbian" | "BlackBerry" => "Unknown"
                case x => x.toLowerCase.capitalize
            }
        }

        ds2 // return clean dataframe
    }

    def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(new SparkConf().setMaster("local").setAppName("AAAAA"))
      .getOrCreate()

    val ds = spark.read.json("/home/cyp/IG/WI/data-students.json")

    val ds2 = preprocess(ds)
    ds2.summary().show

    spark.close()
  }
}