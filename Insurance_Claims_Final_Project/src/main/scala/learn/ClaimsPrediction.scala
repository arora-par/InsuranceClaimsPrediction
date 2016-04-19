
package learn
import scala.util._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.PCA
import data.LoadClaimData
import org.apache.spark.annotation.Since
import scala.beans.BeanInfo

object ClaimsPrediction {
  def main(args: Array[String]) {

    if (args.size < 1) {
     
      println("Please provide train.csv path as input argument");
      return;
      
    }
    val data = LoadClaimData.loadData(args(0));

    val pca = new PCA(60).fit(data.map(_.features))
    val projected = data.map(p => p.copy(features = pca.transform(p.features)))

    // Split data into training (60%) and test (40%).
    val splits = projected.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    // Scala class evaluation Get evaluation metrics. 

    val predictionAndLabels2 = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.setThreshold(0.6).predict(features)
        (prediction, label)
    }

    // Precision and Recall using BinaryClassificationMetrics
    val metrics = new BinaryClassificationMetrics(predictionAndLabels2)

    println(s"Recall =   ${metrics.pr().collect().tail.head._1}")
    println(s"Precision = ${metrics.pr().collect().tail.head._2}")

    // Compute raw scores on the test set. clearThreshold() allows for real values in the range of 0 to 1.

    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.clearThreshold().predict(features)
        (prediction, label)
    }


    // Calculating logloss error using the real values in the range 0 to 1.
    val loss = for {
      pair <- predictionAndLabels
      error = calculateErrorFactor(pair._1, pair._2)
    } yield error

    val logloss = loss.collect().toSeq.reduce(_ + _)
    println(s"logloss= ${logloss / loss.count * (-1)}")

  }

  def calculateErrorFactor(pi: Double, yi: Double): Double = {
    if (pi == 0 || pi ==1 )
      0.0 // edge case values of predictions will not contribute to error
    else (yi * Math.log10(pi) + (1 - yi) * Math.log10(1 - pi))
  }

}