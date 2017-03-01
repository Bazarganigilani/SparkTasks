import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mehdi on 2/28/2017.
  */
object AirportHandler {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Airport handler")
      //.enableHiveSupport()
      .getOrCreate()

    val rowData=spark.sparkContext.textFile("/files/Flight_DataS.csv")
    val sourceAirports=rowData.map(x=>x.split(",")).map(x=> (x(16))).filter(_!="Origin")
    val destAirports=rowData.map(x=>x.split(",")).map(x=> (x(17))).filter(_!="Dest")
    val allAirports=spark.sparkContext.union(sourceAirports,destAirports)
    val scoredAirports= allAirports.map(x=>(x,1)).reduceByKey(_+_)//.map(x=>Row(x._1,x._2))
    //scoredAirports.foreach(println)
    val orderingById: Ordering[(String,Int)] = Ordering.by(e => e._2)
    val firstAirport=scoredAirports.max()(orderingById)
    val secondAirport=scoredAirports.filter(_._1!=firstAirport._1).max()(orderingById)
    val thirdAirport=scoredAirports.filter(_._1!=firstAirport._1).filter(_._1!=secondAirport._1).max()(orderingById)
    println(s"The busiest airport is ${firstAirport._1} with ${firstAirport._2} flights.")
    println(s"The second busiest airport is ${secondAirport._1} with ${secondAirport._2} flights.")
    println(s"The third busiest airport is ${thirdAirport._1} with ${thirdAirport._2} flights.")



    /*
    Hive approach//less efficient
    val fields=List(new StructField("word", StringType, nullable = true),
      new StructField("count", IntegerType, nullable = true) )
    val schema = StructType(fields)
    val df=spark.sqlContext.createDataFrame(scoredAirports,schema)
    df.createOrReplaceTempView("counts")

    val sortedDF=spark.sqlContext.sql("select * from counts ORDER BY count ")

    sortedDF.foreach(x=>println(x))
    */


  }

}
