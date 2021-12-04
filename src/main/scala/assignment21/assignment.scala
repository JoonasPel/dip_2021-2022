package assignment21

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}




import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}


import java.io.{PrintWriter, File}


//import java.lang.Thread
import sys.process._


import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range

object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
  
  val spark = SparkSession.builder()
	                        .appName("assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
                          
  //spark.conf.set("spark.sql.shuffle.partitions", "5")
                          
  // poista inferschema ja tilalle manuaalisti schemat = tehokkaampi                         
                          
  val dataK5D2 =  spark.read
                       .option("inferSchema", true)
                       .option("header", true)
                       .csv("data/dataK5D2.csv")

  val dataK5D3 =  spark.read
                       .option("inferSchema", "true")
                       .option("header", true)
                       .csv("data/dataK5D3.csv")
                       
  dataK5D2.show()
  dataK5D2.printSchema()

  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    // create vectorassembler
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a","b"))
    .setOutputCol("features")
    // create df with features
    val transformedDF = vectorAssembler.transform(df)
    
    //transformedDF.show()
    
    val kmeans = new KMeans()
    .setK(k).setSeed(1L)   
    val kmModel = kmeans.fit(transformedDF)
    
 
    val centers = kmModel.clusterCenters
    val t1 = (centers(0)(0),centers(0)(1))
    val t2 = (centers(1)(0),centers(1)(1))
    val t3 = (centers(2)(0),centers(2)(1))
    val t4 = (centers(3)(0),centers(3)(1))
    val t5 = (centers(4)(0),centers(4)(1))
    val x = Array(t1,t2,t3,t4,t5);

      
    return x
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    ???
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    ???
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    ???
  }
     
  
    
}


