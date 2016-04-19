package learn

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.scalatest.{ FlatSpec, Matchers, BeforeAndAfterAll }
import org.apache.spark.sql.types.{ DoubleType, StructType, StructField }
import learn._

class ClaimPredictionSpec extends FlatSpec with BeforeAndAfterAll with Matchers {

  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf().setMaster("local[2]")
    .setAppName("Insurance Claim")
    .set("spark.driver.allowMultipleContexts","true")
    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
  }

  override protected def afterAll(): Unit = {
    try {
      sc.stop()
      sc = null
      sqlContext = null
    } finally {
      super.afterAll()
    }
  }

  "calculateErrorFactor(0.45, 1)" should "match case -0.34678748622" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.45, 1)
    r - ( -0.34678748622) shouldBe <= (5E-3)
  }
  
  it should "match case -0.07058107" in {    
    val r = ClaimsPrediction.calculateErrorFactor(0.85, 1)
    r - ( -0.07058107) shouldBe <= (5E-3)
  }
  
  it should "match case -0.2218487496163564" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.6, 1)    
    r - (-0.2218487496163564) shouldBe <= (5E-3)
  }
  
  
  it should "match case 0.0" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.0, 1)   
    r - 0.0 shouldBe <= (5E-3)
  }
  
  it should "match case 1.0" in {    
    val r = ClaimsPrediction.calculateErrorFactor(1.0, 1)   
    r - 0.0 shouldBe <= (5E-3)
  }  
  
  "calculateErrorFactor(0.45, 0)" should "match case -0.2596373105057561" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.45, 0)   
    r - ( -0.2596373105057561) shouldBe <= (5E-3)
  }
  
  it should "match case -0.8239087409443187" in {   
    val r = ClaimsPrediction.calculateErrorFactor(0.85, 0)   
    r - ( -0.8239087409443187) shouldBe <= (5E-3)
  }
  
  it should "match case -0.3979400086720376" in {    
    val r = ClaimsPrediction.calculateErrorFactor(0.6, 0)   
    r - (-0.3979400086720376) shouldBe <= (5E-3)
  }  
  
  it should "match case 0.0" in {    
    val r = ClaimsPrediction.calculateErrorFactor(0.0, 0)   
    r - 0.0 shouldBe <= (5E-3)
  }
  
  it should "match case 1.0" in {   
    val r = ClaimsPrediction.calculateErrorFactor(1.0, 0)   
    r - 0.0 shouldBe <= (5E-3)
  }

  
}