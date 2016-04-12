
package claims
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.{ LogisticRegressionWithLBFGS, LogisticRegressionModel }
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{ StructType, StructField, DoubleType, IntegerType, StringType };
import org.apache.spark.sql.Row

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    //v3, v22, v24, v30, v31, v47, v52, v56, v66, v71, v74, v75, v79, v91, v107, v110, v112, v113, v125
    val customSchema = StructType(Array(
      StructField("ID", IntegerType, true),
      StructField("target", DoubleType, true),
      StructField("v1", DoubleType, true),
      StructField("v2", DoubleType, true),
      StructField("v3", StringType, true),
      StructField("v4", DoubleType, true),
      StructField("v5", DoubleType, true),
      StructField("v6", DoubleType, true),
      StructField("v7", DoubleType, true),
      StructField("v8", DoubleType, true),
      StructField("v9", DoubleType, true),
      StructField("v10", DoubleType, true),
      StructField("v11", DoubleType, true),
      StructField("v12", DoubleType, true),
      StructField("v13", DoubleType, true),
      StructField("v14", DoubleType, true),
      StructField("v15", DoubleType, true),
      StructField("v16", DoubleType, true),
      StructField("v17", DoubleType, true),
      StructField("v18", DoubleType, true),
      StructField("v19", DoubleType, true),
      StructField("v20", DoubleType, true),
      StructField("v21", DoubleType, true),
      StructField("v22", StringType, true),
      StructField("v23", StringType, true),
      StructField("v24", StringType, true),
      StructField("v25", DoubleType, true),
      StructField("v26", DoubleType, true),
      StructField("v27", DoubleType, true),
      StructField("v28", DoubleType, true),
      StructField("v29", DoubleType, true),
      StructField("v30", StringType, true),
      StructField("v31", StringType, true),
      StructField("v32", DoubleType, true),
      StructField("v33", DoubleType, true),
      StructField("v34", DoubleType, true),
      StructField("v35", DoubleType, true),
      StructField("v36", DoubleType, true),
      StructField("v37", DoubleType, true),
      StructField("v38", DoubleType, true),
      StructField("v39", DoubleType, true),
      StructField("v40", DoubleType, true),
      StructField("v41", DoubleType, true),
      StructField("v42", DoubleType, true),
      StructField("v43", DoubleType, true),
      StructField("v44", DoubleType, true),
      StructField("v45", DoubleType, true),
      StructField("v46", DoubleType, true),
      StructField("v47", StringType, true),
      StructField("v48", DoubleType, true),
      StructField("v49", DoubleType, true),
      StructField("v50", DoubleType, true),
      StructField("v51", DoubleType, true),
      StructField("v52", StringType, true),
      StructField("v53", DoubleType, true),
      StructField("v54", DoubleType, true),
      StructField("v55", DoubleType, true),
      StructField("v56", StringType, true),
      StructField("v57", DoubleType, true),
      StructField("v58", DoubleType, true),
      StructField("v59", DoubleType, true),
      StructField("v60", DoubleType, true),
      StructField("v61", DoubleType, true),
      StructField("v62", DoubleType, true),
      StructField("v63", DoubleType, true),
      StructField("v64", DoubleType, true),
      StructField("v65", DoubleType, true),
      StructField("v66", StringType, true),
      StructField("v67", DoubleType, true),
      StructField("v68", DoubleType, true),
      StructField("v69", DoubleType, true),
      StructField("v70", DoubleType, true),
      StructField("v71", StringType, true),
      StructField("v72", DoubleType, true),
      StructField("v73", DoubleType, true),
      StructField("v74", StringType, true),
      StructField("v75", StringType, true),
      StructField("v76", DoubleType, true),
      StructField("v77", DoubleType, true),
      StructField("v78", DoubleType, true),
      StructField("v79", StringType, true),
      StructField("v80", DoubleType, true),
      StructField("v81", DoubleType, true),
      StructField("v82", DoubleType, true),
      StructField("v83", DoubleType, true),
      StructField("v84", DoubleType, true),
      StructField("v85", DoubleType, true),
      StructField("v86", DoubleType, true),
      StructField("v87", DoubleType, true),
      StructField("v88", DoubleType, true),
      StructField("v89", DoubleType, true),
      StructField("v90", DoubleType, true),
      StructField("v91", StringType, true),
      StructField("v92", DoubleType, true),
      StructField("v93", DoubleType, true),
      StructField("v94", DoubleType, true),
      StructField("v95", DoubleType, true),
      StructField("v96", DoubleType, true),
      StructField("v97", DoubleType, true),
      StructField("v98", DoubleType, true),
      StructField("v99", DoubleType, true),
      StructField("v100", DoubleType, true),
      StructField("v101", DoubleType, true),
      StructField("v102", DoubleType, true),
      StructField("v103", DoubleType, true),
      StructField("v104", DoubleType, true),
      StructField("v105", DoubleType, true),
      StructField("v106", DoubleType, true),
      StructField("v107", StringType, true),
      StructField("v108", DoubleType, true),
      StructField("v109", DoubleType, true),
      StructField("v110", StringType, true),
      StructField("v111", DoubleType, true),
      StructField("v112", StringType, true),
      StructField("v113", StringType, true),
      StructField("v114", DoubleType, true),
      StructField("v115", DoubleType, true),
      StructField("v116", DoubleType, true),
      StructField("v117", DoubleType, true),
      StructField("v118", DoubleType, true),
      StructField("v119", DoubleType, true),
      StructField("v120", DoubleType, true),
      StructField("v121", DoubleType, true),
      StructField("v122", DoubleType, true),
      StructField("v123", DoubleType, true),
      StructField("v124", DoubleType, true),
      StructField("v125", StringType, true),
      StructField("v126", DoubleType, true),
      StructField("v127", DoubleType, true),
      StructField("v128", DoubleType, true),
      StructField("v129", DoubleType, true),
      StructField("v130", DoubleType, true),
      StructField("v131", DoubleType, true)))

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(customSchema)
      .load(args(0))

    val selectedData = df.select("target", "v10", "v12", "v14", "v21", "v34", "v40", "v50", "v62", "v72", "v114", "v129").rdd;
    //selectedData.collect().foreach{ println(_) }

    val data = for {
      dataRow <- selectedData
      rowSeq = dataRow.toSeq
    } yield { LabeledPoint(rowSeq.head.asInstanceOf[Double], Vectors.dense(convertToArray(rowSeq.tail))) }

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

      println("Threshold =" + model.getThreshold)
      
    // Compute raw scores on the test set.
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.clearThreshold().predict(features)
        (prediction, label)
    }
    
    val predictionAndLabels2 = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }
    
    val metrics = new MulticlassMetrics(predictionAndLabels2)
    val precision = metrics.precision
    val recall = metrics.recall
    println(s"Precision = $precision")
    println("Recall =" + recall)

    // TODO compute using FP, FN etc on your own 2. skew

    val loss = for {
      pair <- predictionAndLabels
      error = calculateErrorFactor(pair._1, pair._2)
    } yield error

    val logloss = loss.collect().toSeq.reduce(_ + _)
    println(s"logloss= ${logloss / loss.count * (-1)}")

    // scala class evaluation Get evaluation metrics.
    

  }

  def convertToArray(seq: Seq[Any]): Array[Double] = {
    val seqDouble = for {
      item <- seq
    } yield item.asInstanceOf[Double]
    seqDouble.toArray
  }

  def calculateErrorFactor(pi: Double, yi: Double): Double = {
    yi * Math.log10(pi) + (1 - yi) * Math.log10(1 - pi)
  }

}