import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{HBaseAdmin, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
  * Created by Mehdi on 2/25/2017.
*/

object CSVLoader {

  def main(args: Array[String]) {


    def convertToPut(row: org.apache.spark.sql.Row) = {
      // create a composite row key: sensorid_date time

      val rowkey = (row.getString(0)+row.getString(1)+row.getString(2)+row.getString(3)).hashCode()
      val put = new Put(Bytes.toBytes(rowkey))
      val cfName=Bytes.toBytes("Column_Familly_1")
      // add to column family data, column  data values to put object
      put.addColumn(cfName, Bytes.toBytes("servedIMSIng"), Bytes.toBytes(row.getString(0)))
      put.addColumn(cfName, Bytes.toBytes("ggsnIPAddress"), Bytes.toBytes(row.getString(1)))
      put.addColumn(cfName, Bytes.toBytes("chargingID"), Bytes.toBytes(row.getString(2)))
      put.addColumn(cfName, Bytes.toBytes("sgsnIPAddress"), Bytes.toBytes(row.getString(3)))
      (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()


    // The schema is encoded in a string
    val schemaString = "servedIMSI,ggsnIPAddress,chargingID,sgsnIPAddress"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)


    val df = spark.read
      .schema(schema)
      .option("header", "true")
      .csv("/files/Sample_CSV.csv")


    df.show()
    //df.registerTempTable("test")

    val tableName = "/test1"
    val hbaseConfig = HBaseConfiguration.create()
    val admin = new HBaseAdmin(hbaseConfig)

    if (!admin.tableExists(tableName)) {
      admin.deleteTable(tableName)
    }

    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("Column_Familly_1"), 1, org.apache.hadoop.hbase.io.compress.Compression.Algorithm.NONE.toString(), true, true, Int.MaxValue,
      org.apache.hadoop.hbase.regionserver.BloomType.NONE.toString()))
    admin.createTable(tableDescriptor)


    hbaseConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConfig, this.getClass)
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    //mappedCSV.map(convertToPut1).saveAsHadoopDataset(jobConfig)
    df.rdd.map(convertToPut).saveAsHadoopDataset(jobConfig)

  }
}




