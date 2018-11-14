import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{udf, when, col, bround}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator


object Main extends App {
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
        
        
        // Replacing null values by "Unknown"s

        val df1 = df.na.fill(Map("city" -> "Unknown","interests" -> "Unknown","network" -> "Unknown","type" -> "Unknown"))

        val df2 = df1.select("appOrSite","interests","media","type","bidfloor","label","os","network","size","city", "publisher")
        
        
        // Removing all add with size null
        val df3 = df2.filter(!col("size").getItem(0).isNull)
        val df4 = df3.filter(!col("size").getItem(1).isNull)

        val df5 = 
      {df4.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor")))
      .withColumn("label", when(col("label") === true, 1).otherwise(0))
      .withColumn("os", os(df4("os")))
      .withColumn("network", network(df4("network")))} 


      //print("\n\n\n" + df5.dtypes.foreach(println) +"\n\n")

      // get categorical values
      val stringTypes = df5.dtypes.filter(_._2 == "StringType").map(_._1)

      val indexers = stringTypes.map (
        c => new StringIndexer().setInputCol(c).setOutputCol(s"${c}_indexed")
      )

      val encoders = stringTypes.map (
        c => new OneHotEncoder().setInputCol(s"${c}_indexed").setOutputCol(s"${c}_encoded")
      )

      val pipeline = new Pipeline().setStages(indexers ++ encoders)

      // apply the indexed data to the dataframe
      val df6 = pipeline.fit(df5).transform(df5)

      val assembler = new VectorAssembler().setInputCols(Array("appOrSite_encoded", "bidfloor", "media_encoded", "os_encoded", "publisher_encoded","network_encoded")).setOutputCol("features")
      //return a dataframe with all of the  feature columns in  a vector column**

      assembler.transform(df6).select("label", "features")
    }


    //////////// Entry point ////////////////

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(new SparkConf().setMaster("local").setAppName("AAAAA"))
      .getOrCreate()

    val ds = spark.read.json("/home/cyp/IG/WI/data-students.json")

    val ds2 = preprocess(ds)
    
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(10).setThreshold(0.6)


    val splitData = ds2.randomSplit(Array(0.7,0.3))
    var (training, test) = (splitData(0), splitData(1))

    // use logistic regression to train (fit) the model with the training data 
    val lrModel = lr.fit(training)

    val predict = lrModel.transform(test)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")

    val accuracy = evaluator.evaluate(predict)



    val lp = predict.select("label", "prediction")
    val counttotal = predict.count()
    val correct = lp.filter(col("label") === col("prediction")).count()
    val wrong = lp.filter(!(col("label") === col("prediction"))).count()
    val truep = lp.filter(col("prediction") === 0.0).filter(col("label") === col("prediction")).count()
    val falseN = lp.filter(col("prediction") === 0.0).filter(!(col("label") === col("prediction"))).count()
    val falseP = lp.filter(col("prediction") === 1.0).filter(!(col("label") === col("prediction"))).count()
    val ratioWrong=wrong.toDouble/counttotal.toDouble
    val ratioCorrect=correct.toDouble/counttotal.toDouble

    println("\n Correct : " + correct)
    println("Wrong : " + wrong)
    println("True positive : " + truep)
    println("False negative : " + falseN)
    println("False positive : " + falseP)
    println("Ratio wrong : " + ratioWrong)
    println("Ratio correct : " + ratioCorrect)



    spark.close()
}