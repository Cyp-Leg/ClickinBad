import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{udf, when, col}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.log4j.{Logger, Level}

object clickinBad {
  /* ----------------------------------------- */
  /* -------------- Preprocessing ------------ */
  /* ----------------------------------------- */
    def preprocess(df: DataFrame): DataFrame = {
        val network = udf { // getting countries from networks
            (str: String) =>

              str match{
                case null => "Unknown"
                case s => if(s.length>2) {s.splitAt(3)._1} else "Unknown"
              }
              
        }

        // Replacing null values by "Unknown"s

        // Removing all add with size null
        //val ds2 = unknown.filter(!col("size").getItem(0).isNull).filter(!col("size").getItem(1).isNull)

        // Gathering all windows phones together, set nulls & "Other" to "Unknown"
        val os = udf {
            (str: String) =>
                val windows_pattern = new Regex("Windows(.*)")
                val windows_lowercase = new Regex("windows(.*)")
                
                str match {
                case null => "Unknown"
                case windows_pattern(x) => "Windows Phone"
                case windows_lowercase(x) => "Windows Phone"
                case "ios" | "iOS" => "iOS"
                case "Rim" | "Bada" | "WebOS"| "Symbian" | "blackberry" => "Other"
                case x => x.toLowerCase.capitalize
            }
        }

        val df1 = df.na.fill(Map("city" -> "Unknown","impid" -> "Unknown","interests" -> "Unknown","network" -> "Unknown","type" -> "Unknown"))

        val df2 = df1.select("appOrSite","interests","media","type","bidfloor","label","os","network","timestamp","size","city", "publisher")
        val df3 = df2.filter(!col("size").getItem(0).isNull)
        val df4 = df3.filter(!col("size").getItem(1).isNull)

        val df5 = 
      {df4.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor")))
      .withColumn("label", when(col("label") === true, 1).otherwise(0))
      .withColumn("os", os(df4("os")))
      .withColumn("network", network(df4("network")))} 


      //print("\n\n\n" + df5.dtypes.foreach(println) +"\n\n")

      val categoricals = df5.dtypes.filter (_._2 == "StringType").map (_._1)

      val indexers = categoricals.map (
        c => new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx")
      )

      val encoders = categoricals.map (
        c => new OneHotEncoder().setInputCol(s"${c}_idx").setOutputCol(s"${c}_enc")
      )

      val pipeline = new Pipeline().setStages(indexers ++ encoders)
      val df6 = pipeline.fit(df5).transform(df5)

      val assembler = new VectorAssembler().setInputCols(Array("appOrSite_enc", "bidfloor", "media_enc", "os_enc", "publisher_enc","network_enc")).setOutputCol("features")
      //return a dataframe with all of the  feature columns in  a vector column**

      assembler.transform(df6).select("appOrSite", "bidfloor", "city", "interests", "label", "media", "network", "os", "publisher", "type", "features")
    }


    def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(new SparkConf().setMaster("local").setAppName("AAAAA"))
      .getOrCreate()

    val ds = spark.read.json("../WI/data/data-students.json")

    val ds2 = preprocess(ds)
    ds2.summary().show()
    //ds2.groupBy("network").count().sort(col("count")).show()
    //ds2.select("timestamp").show()
    val dsBidZero = ds2.filter(col("bidfloor") === 0)
    val dsBidZeroClick = dsBidZero.filter(col("label") === true).count()
    val dsBidZeroNoClick = dsBidZero.filter(col("label") === false).count()

    print("\n\n\n\n\n\n\n\n\n\n\n")
    print(dsBidZero.count())
    print("\n\n\n\n\n\n\n\n\n\n\n")
    print(dsBidZeroClick)
    print("\n\n\n\n\n\n\n\n\n\n\n")
    print(dsBidZeroNoClick)
    print("\n\n\n\n\n\n\n\n\n\n\n")




    spark.close()
  }
}