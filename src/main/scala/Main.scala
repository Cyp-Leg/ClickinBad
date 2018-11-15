import org.apache.spark.sql.DataFrame
import scala.util.matching.Regex
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{udf, when, col, bround}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler, VectorSlicer}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.types.StringType
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._


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

        val toDouble = udf[Double, String]( _.toDouble)        
        
        // Replacing null values by "Unknown"s

        val df1 = df.na.fill(Map("city" -> "Unknown","interests" -> "Unknown","network" -> "Unknown","type" -> "Unknown"))

        val df2 = df1.select("appOrSite","interests","media","type","bidfloor","label","network","size", "city", "os")
        
        
        // Removing all add with size null
        val df3 = df2.filter(!col("size").getItem(0).isNull)
        val df4 = df3.filter(!col("size").getItem(1).isNull)

        val df5 = 
      {df4.withColumn("bidfloor", when(col("bidfloor").isNull, 3).otherwise(col("bidfloor")))
      .withColumn("size", df4("size").cast(StringType))
      .withColumn("label", when(col("label") === true, 1).otherwise(0))
      .withColumn("network", network(df4("network")))}


      // get categorical values
      val stringTypes = df5.dtypes.filter(_._2 == "StringType").map(_._1)

      // index them
      val indexers = stringTypes.map (
        c => new StringIndexer().setInputCol(c).setOutputCol(s"${c}_indexed").setHandleInvalid("skip")
      )

      val encoders = stringTypes.map (
        c => new OneHotEncoder().setInputCol(s"${c}_indexed").setOutputCol(s"${c}_encoded")
      )

      val pipeline = new Pipeline().setStages(indexers ++ encoders)

      // apply the indexed data to the dataframe
      val df6 = pipeline.fit(df5).transform(df5)


      val assembler = new VectorAssembler().setInputCols(Array("appOrSite_encoded", "bidfloor", "interests_encoded", "media_encoded", "city_encoded","network_encoded", "size_encoded", "os_encoded", "type_encoded")).setOutputCol("features")
      
      //return a dataframe with all of the  feature columns in  a vector column**
      assembler.transform(df6).select("label", "features", "appOrSite","interests","media","type","bidfloor","network","size", "city", "os")
    }



    def balanceDataset(dataset: DataFrame): DataFrame = {
    // Re-balancing (weighting) of records to be used in the logistic loss objective function
    // This function improve the weight of the true label while it decrease the weight of false label
    val numNegatives = dataset.filter(dataset("label") === 0).count()
    val datasetSize = dataset.count()
    val balancingRatio = (datasetSize - numNegatives).toDouble / datasetSize

    val calculateWeights = udf { d: Double =>
      if (d == 0.0) {
        1 * balancingRatio
      }
      else {
        (1 * (1.0 - balancingRatio))
      }
    }

    val weightedDataset = dataset.withColumn("classWeightCol", calculateWeights(dataset("label")))
    weightedDataset
  }



    //////////// Entry point ////////////////

    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(new SparkConf().setMaster("local").setAppName("ClickinBad"))
      .getOrCreate()

    val ds = spark.read.json(args(0))

    val ds2 = preprocess(ds)

    // initializing logistic regression object & balance training dataset
    val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setWeightCol("classWeightCol")
    val trainingDS = balanceDataset(ds2.select("label","features"))

    // use logistic regression to train (fit) the model with the training data 
    val lrModel = lr.fit(trainingDS.select("label", "features", "classWeightCol"))

    // read the text file, preprocess it and apply the logistic regression model
    val test = spark.read.json(args(1))
    val cleanTest = preprocess(test)
    val predict = lrModel.transform(cleanTest)

    // casting label to Double type (for BinaryClassficationMetrics to get the ROC)
    val toDouble = udf[Double, String]( _.toDouble)
    val predict2 = predict.withColumn("label", toDouble(predict("label")))



    //* ----------------------------------------------------------------------------- */
    /* ---------------------------------- Evaluation -------------------------------- */
    /* ------------------Uncomment following lines to display rates------------------ */
    /* /!\ You should test a file containing a "label" column to get the stastics /!\ */
    //* ----------------------------------------------------------------------------- */

    /*val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")

    val accuracy = evaluator.evaluate(predict)



    val lp = predict2.select("label", "prediction")
    val counttotal = predict.count()
    val correct = lp.filter(col("label") === col("prediction")).count()
    val wrong = lp.filter(!(col("label") === col("prediction"))).count()
    val truep = lp.filter(col("prediction") === 1.0).filter(col("label") === col("prediction")).count()
    val falseN = lp.filter(col("prediction") === 0.0).filter(!(col("label") === col("prediction"))).count()
    val falseP = lp.filter(col("prediction") === 1.0).filter(!(col("label") === col("prediction"))).count()
    val recall = truep.toDouble/(truep.toDouble+falseN.toDouble)
    val ratioWrong=wrong.toDouble/counttotal.toDouble
    val ratioCorrect=correct.toDouble/counttotal.toDouble
    

    println("\n Correct : " + correct)
    println("Wrong : " + wrong)
    println("True positive : " + truep)
    println("False negative : " + falseN)
    println("False positive : " + falseP)
    println("Recall : " + recall)
    println("Ratio wrong : " + ratioWrong)
    println("Ratio correct : " + ratioCorrect)


    // use MLlib to evaluate, convert DF to RDD**
    val predictionAndLabels = predict2.select("rawPrediction", "label").rdd.map(x => (x(0).asInstanceOf[DenseVector](1), x(1).asInstanceOf[Double]))
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    println("area under the precision-recall curve: " + metrics.areaUnderPR)
    println("area under the receiver operating characteristic (ROC) curve : " + metrics.areaUnderROC)
    */
    
    // Exporting as CSV file

    val finalDs = predict2.withColumn("Label", predict2("prediction")).select("Label","appOrSite","interests","media","type","bidfloor","network","size", "city", "os")
    finalDs.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("result_predictions")
    
    spark.close()
}