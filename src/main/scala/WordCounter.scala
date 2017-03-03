import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
//import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Mehdi on 2/28/2017.
  */
object WordCounter {

  def main(args: Array[String]): Unit = {


    val tableInputName = "/test1"
    val tableOutputName="/WordCounts"
    val sparkConf = new SparkConf().setAppName("HBaseReader")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //Read the table from Hbase
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, tableInputName)
    val hBaseRDD = sc.newAPIHadoopRDD(hbaseConfig, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println("Number of Records found : " + hBaseRDD.count())
    val resultRDD = hBaseRDD.map(tuple => tuple._2)



    //Convert the HBase result to a RDD format
    val getStrings=(result:Result)=>{

      var rowString=""
      val cells=result.rawCells

      for (i <- 0 to cells.length-1)
      {
        val family = CellUtil.cloneFamily(cells(i))
        val column = CellUtil.cloneQualifier(cells(i))
        val value = CellUtil.cloneValue(cells(i))
        val stringValue=Bytes.toString(value)
        //print(s"value is $stringValue")
        if(i!=cells.length-1)
          rowString=rowString.concat(stringValue+",")
        else
          rowString=rowString.concat(stringValue+System.lineSeparator())
      }
      rowString
    }

    //Populate RDD from HBase and computing the word count
    val wordCounts = resultRDD.map(getStrings).
      flatMap(_.split(",")).map(w=>(w,1)).reduceByKey(_+_)

    //Write (word,count) tuples to HBase using belw put convertor
    val convertToPut=(a: (String,Int)) =>{
      // create a composite row key: sensorid_date time

      val rowkey = a._1.hashCode()
      val put = new Put(Bytes.toBytes(rowkey))
      val cfName=Bytes.toBytes("Column_Familly_1")
      // add to column family data, column  data values to put object
      put.addColumn(cfName, Bytes.toBytes("Word"), Bytes.toBytes(a._1))
      put.addColumn(cfName, Bytes.toBytes("Count"), Bytes.toBytes(a._2))
      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

    //if table exist first delete its content
    val admin = new HBaseAdmin(hbaseConfig)
    if (admin.tableExists(tableOutputName)) {
      admin.deleteTable(tableOutputName)
    }

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableOutputName))
    tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("Column_Familly_1"), 1, org.apache.hadoop.hbase.io.compress.Compression.Algorithm.NONE.toString(), true, true, Int.MaxValue,
      org.apache.hadoop.hbase.regionserver.BloomType.NONE.toString()))
    admin.createTable(tableDescriptor)

    hbaseConfig.set(TableOutputFormat.OUTPUT_TABLE, tableOutputName)
    val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConfig, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableOutputName)


    //Prompt tuple word counts on the screen
    wordCounts.foreach(println)

    //Write to HBase
    wordCounts.map(convertToPut).saveAsHadoopDataset(jobConfig)


    //Stop the Spark context
    sc.stop()

  }

}
