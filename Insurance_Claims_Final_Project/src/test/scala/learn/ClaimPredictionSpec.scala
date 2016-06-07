package learn

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.scalatest.{ FlatSpec, Matchers, BeforeAndAfterAll }
import org.apache.spark.sql.types.{ DoubleType, StructType, StructField }
import learn._

class ClaimPredictionSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  import org.scalactic._

  implicit val efEq = new Equality[Double] {
    def areEqual(a: Double, b: Any): Boolean = math.abs(a-b.asInstanceOf[Double])<5E-3
  }
  
  val preal = 0.6

  "calculateErrorFactor(preal, 1)" should "match case Math.log(preal)" in {   
    val r = ClaimsPrediction.calculateErrorFactor(preal, 1)
    r should === (Math.log(preal))
  }
  
  
  it should "match case 0.0" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.0, 1)
    r should === (0.0)
  }
  
  it should "match case 1.0" in {    
    val r = ClaimsPrediction.calculateErrorFactor(1.0, 1)
    r should === (0.0)
  }  
  
  "calculateErrorFactor(preal, 0)" should "match case Math.log(1 - preal)" in {   
    val r = ClaimsPrediction.calculateErrorFactor(preal, 0)
    r should === (Math.log(1 - preal))
  }
 
  
  it should "match case 0.0" in {    
    val r = ClaimsPrediction.calculateErrorFactor(0.0, 0)
    r should === (0.0)
  }
  
  it should "match case 1.0" in {   
    val r = ClaimsPrediction.calculateErrorFactor(1.0, 0)
    r should === (0.0)
  }

}