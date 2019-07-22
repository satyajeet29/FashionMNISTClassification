package pradhan.com.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive
import org.apache.spark.mllib.clustering
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.doubleRDDToDoubleRDDFunctions
import org.apache.spark.streaming
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by satyajeetpradhan on 7/21/19.
  * Meant for converting Fashion MNIST Data into LibSVM format for running it on MLLib projects
  */
object conversionLibSVM {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
          .builder
          .appName("ConvLibSVM")
          .config("spark.some.config.option", "some-value")
          .enableHiveSupport()
          .getOrCreate()
    // Importing the SparkSession gives access to all the SQL functions and implicit conversions. 
    import spark.implicits._
    // $example off:init_session$  
    //val conf = new SparkConf()

     spark.conf
              .set("spark.driver.allowMultipleContexts", "true")


    //the above line is intended to overwrite the existing files  
    //val sc = new SparkContext(conf)
    //val hc =  new org.apache.spark.sql.hive.HiveContext(sc)

    //import hc.implicits._

    //val df = spark.sql("SELECT * FROM fashionmnist.traindataset")
    val df = spark.sql("SELECT * FROM fashionmnist.testdataset")

    val dfRDD = df.rdd

    val dfRDDString = dfRDD.map(x => x.toString)

    val newRdds_1  = dfRDDString.map(x => x.replaceAll("\\[","").replaceAll("\\]","").split("\\,").map(_.toDouble))

    val newRdds_2 = newRdds_1.map(x => LabeledPoint(x(0), Vectors.dense(x.slice(1, x.length))))

    try{
      newRdds_2.first
    }
    catch{
      case x: Exception =>
        println("Exception unable to get first row")
    }

    try{
      newRdds_2.repartition(1)
    }
    catch{
      case x: Exception =>
        println("Exception unable to repartition")
    }

    try {
      //MLUtils.saveAsLibSVMFile(newRdds_2, "rdds/FM_TrainLibSVM_r1")
      MLUtils.saveAsLibSVMFile(newRdds_2, "rdds/FM_TestLibSVM_r1")
    }
    catch{
      case x: Exception =>
        println("Exception unable to save files")
    }

    spark.stop()
  }
}
