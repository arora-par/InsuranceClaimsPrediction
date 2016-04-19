package data

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ StructType, StructField, DoubleType, IntegerType, StringType };
import org.apache.spark.mllib.linalg.Vectors

object LoadClaimData {

  def loadData(filename: String): RDD[LabeledPoint] = {
    val conf = new SparkConf().setAppName("Claim Prediction")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

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
      .option("header", "true")
      .schema(customSchema)
      .load(filename)

    // TODO - calculate avg 

    //    df.registerTempTable("train")
    //
    //    val vn = "v1,v2,v4,v5,v6,v7,v8,v9,v10,v12,v13,v14,v21,v34,v40,v50,v62,v72,v114,v129";
    //
    //    val ftrAvgMap: Map[String, DataFrame] = vn.split(",").map { vi => (vi, sqlContext.sql(s"Select avg($vi) from train where $vi is not null")) }.toMap

    //sqlData.foreach(x => println(x._1 + " avg is: " + x._2.map(t => t(0)).collect().foreach(print)))

    val selectedData = df.select("target", "v1", "v2", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
      "v11", "v12", "v13", "v14", "v15", "v16", "v17", "v18", "v19", "v20",
      "v21", "v25", "v26", "v27", "v28", "v29",
      "v32", "v34", "v33", "v35", "v36", "v37", "v38", "v39", "v40",
      "v41", "v42", "v43", "v44", "v45", "v46", "v48", "v49", "v50",
      "v51", "v53", "v54", "v55", "v57", "v58", "v59", "v60",
      "v61", "v62", "v64", "v65", "v63", "v67", "v68", "v69", "v70",
      "v72", "v73", "v76", "v77", "v78", "v80",
      "v81", "v83", "v82", "v84", "v85", "v86", "v87", "v88", "v89", "v90",
      "v93", "v92", "v94", "v95", "v96", "v97", "v98", "v99", "v100",
      "v101", "v102", "v104", "v105", "v106", "v103", "v108", "v109",
      "v111", "v114", "v115", "v116", "v117", "v118", "v119", "v120",
      "v121", "v122", "v123", "v124", "v126", "v127", "v128", "v129", "v130", "v131").rdd;

    for {
      dataRow <- selectedData
      rowSeq = dataRow.toSeq
    } yield { LabeledPoint(rowSeq.head.asInstanceOf[Double], Vectors.dense(convertToArray(rowSeq.tail))) }

  }

  def convertToArray(seq: Seq[Any]): Array[Double] = {
    val seqDouble = for {
      item <- seq
      x = emptyCheck(item)
    } yield x
    seqDouble.toArray
  }

  def emptyCheck(n: Any): Double = {
    n match {
      case null => 10
      case _    => n.asInstanceOf[Double]
    }
  }

}