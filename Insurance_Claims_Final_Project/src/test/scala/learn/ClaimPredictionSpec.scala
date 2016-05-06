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

  "calculateErrorFactor(0.45, 1)" should "match case -0.34678748622" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.45, 1)
    r should === (-0.34678748622)
  }
  
  it should "match case -0.07058107" in {    
    val r = ClaimsPrediction.calculateErrorFactor(0.85, 1)
    r should === (-0.07058107)
  }
  
  it should "match case -0.2218487496163564" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.6, 1)
    r should === (-0.2218487496163564)
  }
  
  
  it should "match case 0.0" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.0, 1)
    r should === (0.0)
  }
  
  it should "match case 1.0" in {    
    val r = ClaimsPrediction.calculateErrorFactor(1.0, 1)
    r should === (0.0)
  }  
  
  "calculateErrorFactor(0.45, 0)" should "match case -0.2596373105057561" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.45, 0)
    r should === (-0.2596373105057561)
  }
  
  it should "match case -0.8239087409443187" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.85, 0)
    r should === (-0.8239087409443187)
  }
  
  it should "match case -0.3979400086720376" in {    
    val r = ClaimsPrediction.calculateErrorFactor(0.6, 0)
    r should === (-0.3979400086720376)
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