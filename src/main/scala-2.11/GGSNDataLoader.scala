/* GGSNFileLoader.scala */
/*
//Import required classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.StatCounter
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, LongType, IntegerType, ShortType, DateType}
import org.apache.spark.sql.functions._
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.IndexedSeq
import java.io.File

// Option processing code: http://stackoverflow.com/questions/2315912/scala-best-way-to-parse-command-line-parameters-cli

object GGSNFileLoader {

    private var unitTesting = false // set true for unit testing only - has additional overhead
    private var saveAsHadoopDataSet = true // set true for actual table write for production functionality
    private var testSlowPut = true // for unit testing only
    private val aggregatePerReport = false // aggregate "on-the-fly" per report processed
    private val deferedAggregation = false // aggregation will be defered until sometime later
    private val norTableCFName  = "D"
    private val norTableCFDataBytes = Bytes.toBytes(norTableCFName)
    private val aggTableCFName  = "D"
    private val aggTableCFDataBytes = Bytes.toBytes(aggTableCFName)
    private val aggTrackerTableCFName  = "D"
    private val aggTrackerTableCFDataBytes = Bytes.toBytes(aggTrackerTableCFName)
    private val norTableName = "/Analytics/RA/db/nor_ggsn"
    private val aggTableName = "/Analytics/RA/db/agg_ggsn_daily"
    private val aggTrackerTableName = "/Analytics/RA/db/agg_tracker"
    private val aggDirPath   = "/Analytics/RA/etl/agg/"
    //private val aggDirPath   = "/mapr/cellos-mapr" + "/Analytics/RA/etl/agg"
    private val maxAggScanTimestamp  = 1L * 60L * 60L // a limit on timestamp range when scanning for records to aggregate

    var cdr_source = ""
    var rec_population_time = ""

    def load( // load and input file to database after mapping the columns
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf,
        inputFile : String
    )
    {
        //enableUnitTesting

        var nameParts = getInfoFromFilename(inputFile)
        cdr_source = nameParts(0)
        rec_population_time = nameParts(2)
        println("cdr_source = " + cdr_source + ", rec_population_time = " + rec_population_time)

        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
        val customSchema = getInputSchema()
        println(customSchema)

        val inputDF = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .schema(customSchema)
            .load(inputFile)    
            //.limit(10000) // temporary limit until all the spark conf options are sorted out
            //.option("quote", "'")

        //inputDF.write.format("parquet").mode("overwrite").save(parquetFile)

        inputDF.registerTempTable("INPUT_DATA")

        val query : String = getMappingQuery()
        println(query)
        val withRowkeyDF = sqlContext.sql(query)
        //val inputWithRowkeyDF = withRowkeyDF.repartition(withRowkeyDF("ROW_KEY"))
        val inputWithRowkeyDF = withRowkeyDF
            //.repartition(withRowkeyDF("ROW_KEY"))
            //.persist

        inputWithRowkeyDF.registerTempTable("INPUT_DATA")
        inputWithRowkeyDF.printSchema()

        if (unitTesting) {
            println("Records count in inputWithRowkeyDF: " + inputWithRowkeyDF.count)
        }

        println("inputWithRowkeyDF RDD partitions size :-" + inputWithRowkeyDF.rdd.partitions.size)
        println("inputWithRowkeyDF :-" + inputWithRowkeyDF.rdd.toDebugString)
        if (saveAsHadoopDataSet) {
            // Write to table
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, norTableName)
            val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConf, this.getClass)
            jobConfig.setOutputFormat(classOf[TableOutputFormat])
            jobConfig.set(TableOutputFormat.OUTPUT_TABLE, norTableName)
            inputWithRowkeyDF.map(makePutNorTable).saveAsHadoopDataset(jobConfig)
        } else {
            // -------------------------------------------------------------------------------
            // Slow put - for testing only
            // -------------------------------------------------------------------------------

            var hbaseConf = HBaseConfiguration.create();
            var connection = ConnectionFactory.createConnection(hbaseConf);
            var tableDescriptor = new HTableDescriptor(TableName.valueOf(norTableName));
            tableDescriptor.addFamily(new HColumnDescriptor(norTableCFName));
            var table = connection.getTable(TableName.valueOf(norTableName));

            var allPuts = inputWithRowkeyDF.collect().take(20) // take subset for testing
            allPuts.foreach(println)

            if (testSlowPut) {
                for (eachPut <- allPuts)
                {
                    var puts = makePutSlowNorTable(eachPut);
                    table.put(puts);
                }
            }
        }

        if (unitTesting) { // turn this on for debugging only
            val scanConf = HBaseConfiguration.create()
            scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
            //scanConf.set(TableInputFormat.SCAN_BATCHSIZE, "10")
            //scanConf.set(TableInputFormat.SCAN_ROW_START, "621300010016640-1307612030-2")
            //scanConf.set(TableInputFormat.SCAN_ROW_STOP, "621300010066842-1557639204-2")
            //scanConf.setMaxResultsPerColumnFamily(10)
            val scanRDD = sc.newAPIHadoopRDD(
                scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

            //scanRDD.collect()
            println("Records count now in table '" + norTableName + "': " + scanRDD.count())
        }

        if (aggregatePerReport) {
            val scanRangeDF = sqlContext.sql("SELECT MIN(ROW_KEY) AS RK_MIN, MAX(ROW_KEY) AS RK_MAX FROM INPUT_DATA")
            if (unitTesting) {
                scanRangeDF.show()
            }

            val first = scanRangeDF.first
            val scan_rk_min_s = first.getString(0).split('|')
            val scan_rk_max_s = first.getString(1).split('|')
            val scan_rk_min = scan_rk_min_s(0) + "|" + scan_rk_min_s(1) // msisdn|charging_id
            val scan_rk_max = scan_rk_max_s(0) + "|" + scan_rk_max_s(1) + "a" // msisdn|charging_id

            println("Scan row key range for aggregation: min: " + scan_rk_min + ", scan_max: " + scan_rk_max)

            import sqlContext.implicits._

            val scanConf = HBaseConfiguration.create()
            scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
            //scanConf.set(TableInputFormat.SCAN_BATCHSIZE, "10")
            //scanConf.set(TableInputFormat.SCAN_ROW_START, "2347010210968|1090451301|")
            //scanConf.set(TableInputFormat.SCAN_ROW_STOP, "2347010210968|1090451302|")
            //scanConf.set(TableInputFormat.SCAN_ROW_START, "2347011472241|48344634")
            //scanConf.set(TableInputFormat.SCAN_ROW_STOP, "2347011472241|49489611a|")
            scanConf.set(TableInputFormat.SCAN_ROW_START, scan_rk_min)
            scanConf.set(TableInputFormat.SCAN_ROW_STOP, scan_rk_max)
            //scanConf.setMaxResultsPerColumnFamily(10)
            val hbaseRDD = sc.newAPIHadoopRDD(
                scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

            val resultRDD = hbaseRDD.map(tuple => tuple._2)

            val norTableRDD = resultRDD.map(AggRow.parseRow)

            val norTableDF = norTableRDD.toDF()

            if (unitTesting) {
                println("Records count scanned into norTableDF: " + norTableDF.count)
            }

            norTableDF.printSchema()
            if (unitTesting) {
                norTableDF.show()
            }

            norTableDF.registerTempTable("NOR_TABLE_DATA")

            val query : String = getAggregateByInputJoinQuery()
            println(query)
            //val aggDF = sqlContext.sql(query)
            val aggDF = sqlContext.sql(query)
                //.persist
            aggDF.printSchema()
            println("aggDF RDD partitions size :-" + aggDF.rdd.partitions.size)
            println("aggDF :-" + aggDF.rdd.toDebugString)


            if (unitTesting) {
                println("Records count aggregated into aggDF: " + aggDF.count)
                aggDF.take(20).foreach(println) // take subset for testing
            }

            if (deferedAggregation) { // TODO: presumably to do a bulk aggregation at a later time!
                // Here we save the list of aggregatable records from this input file for defered aggregation
                val aggFilename = getAggFilename(inputFile, ".parquet")
                val colsToSave = List("ROW_KEY", "MSISDN", "CHARGING_ID", "EVENT_DATE")
                val colsToSaveDF = aggDF.select(colsToSave.head, colsToSave.tail: _*)
                colsToSaveDF.write.format("parquet").mode("overwrite").save(aggFilename)
            } else if (saveAsHadoopDataSet) { // write the aggregated records to the database
                // This should be used when running in an application to increase performance
                // but not available through notebooks.
                // set up Hadoop HBase configuration using TableOutputFormat
                val hbaseConf = HBaseConfiguration.create()
                hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
                val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConf, this.getClass)
                jobConfig.setOutputFormat(classOf[TableOutputFormat])
                jobConfig.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
                aggDF.map(makePutAggTable).saveAsHadoopDataset(jobConfig)
            }

            if (unitTesting) { // turn this on for debugging only
                val scanConf = HBaseConfiguration.create()
                scanConf.set(TableInputFormat.INPUT_TABLE, aggTableName)
                //scanConf.set(TableInputFormat.SCAN_BATCHSIZE, "10")
                //scanConf.set(TableInputFormat.SCAN_ROW_START, "621300010016640-1307612030-2")
                //scanConf.set(TableInputFormat.SCAN_ROW_STOP, "621300010066842-1557639204-2")
                //scanConf.setMaxResultsPerColumnFamily(10)
                val scanRDD = sc.newAPIHadoopRDD(
                    scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

                //scanRDD.collect()
                println("Records count now in table '" + aggTableName + "': " + scanRDD.count())
            }
        }
    }

    case class AggRow(
            ROW_KEY:  String,
            CDR_SOURCE:  String,
            REC_POPULATION_TIME:  String,
            CDR_REASON:  String,
            PARTIAL_SEQ_NUM:  String,
            PARTIAL_IND:  String,
            TERMINATION_CAUSE:  String,
            SOURCE_SEQ_NUM:  String,
            imsi:  String,
            msisdn:  String,
            imei:  String,
            CHARGING_ID:  String,
            UE_IP_ADDRESS:  String,
            EVENT_START_TIME:  String,
            EVENT_END_TIME:  String,
            duration:  String,
            UPLINK_VOL:  String,
            DOWNLINK_VOL:  String,
            TOTAL_VOL:  String,
            RAT_TYPE:  String
        )

    object AggRow extends Serializable {
        def parseRow(result: Result): AggRow = {
            val rowkey = Bytes.toString(result.getRow())
            val p0 = rowkey
            val p1 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("CDR_SOURCE")))
            val p2 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("REC_POPULATION_TIME")))
            val p3 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("CDR_REASON")))
            val p4 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("PARTIAL_SEQ_NUM")))
            val p5 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("PARTIAL_IND")))
            val p6 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("TERMINATION_CAUSE")))
            val p7 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("SOURCE_SEQ_NUM")))
            val p8 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("IMSI")))
            val p9 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("MSISDN")))
            val p10 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("IMEI")))
            val p11 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("CHARGING_ID")))
            val p12 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("UE_IP_ADDRESS")))
            val p13 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("EVENT_START_TIME")))
            val p14 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("EVENT_END_TIME")))
            val p15 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("DURATION")))
            val p16 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("UPLINK_VOL")))
            val p17 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("DOWNLINK_VOL")))
            val p18 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("TOTAL_VOL")))
            val p19 = Bytes.toString(result.getValue(norTableCFDataBytes, Bytes.toBytes("RAT_TYPE")))
            AggRow(
                p0, p1, p2, p3, p4, p5, p6, p7, p8, p9,
                p10, p11, p12, p13, p14, p15, p16, p17, p18, p19
            )
        }
    }

    def createAggregationPlan( // This function aggregates using the range provides in the input scan config
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf
    )
    {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

        import sqlContext.implicits._

        val scanConf = HBaseConfiguration.create()
        scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
        val hbaseRDD = sc.newAPIHadoopRDD(
            scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val resultRDD = hbaseRDD.map(tuple => tuple._2)

        val norTableRDD = resultRDD.map(AggRow.parseRow)

        val norTableDF = norTableRDD.toDF()

        norTableDF.printSchema()
        if (unitTesting) {
            norTableDF.show()
        }

        norTableDF.registerTempTable("NOR_TABLE_DATA")

        val aggQuery : String = """
SELECT
CONCAT(CAST(SUBSTR(ROW_KEY,0,7) AS varchar(100)), '0000000|') AS SCAN_ROW_START,
COUNT(DISTINCT ROW_KEY) AS NUM_ROW_KEYS
FROM NOR_TABLE_DATA
GROUP BY
SUBSTR(ROW_KEY,0,7)
ORDER BY
SUBSTR(ROW_KEY,0,7)
"""
        println(aggQuery)
        val aggDF = sqlContext.sql(aggQuery)
        aggDF.printSchema()

        if (unitTesting) {
            println("Records count aggregated into aggDF: " + aggDF.count)
            //aggDF.take(20).foreach(println) // take subset for testing
            aggDF.collect.foreach(println) // shouldn't be too many
        }

        // save as parquet
        //aggDF.write.format("csv").mode("overwrite").save(planOutputFile)
    }

    def aggregateByRowKeyRange( // This function aggregates using the row key range provided
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf,
        start_row_key : String,
        end_row_key : String
    )
    {
        println("Scan row key range for aggregation: min: " + start_row_key + ", scan_max: " + end_row_key)
        val scanConf = HBaseConfiguration.create()
        scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
        scanConf.set(TableInputFormat.SCAN_ROW_START, start_row_key)
        scanConf.set(TableInputFormat.SCAN_ROW_STOP, end_row_key)
        aggregateByScanConf(sc, conf, scanConf)
    }

    def aggregateByScanConf( // This function aggregates using the range provides in the input scan config
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf,
        scanConf : Configuration
    )
    {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

        import sqlContext.implicits._

        //val scanConf = HBaseConfiguration.create()
        scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
        val hbaseRDD = sc.newAPIHadoopRDD(
            scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val resultRDD = hbaseRDD.map(tuple => tuple._2)

        val norTableRDD = resultRDD.map(AggRow.parseRow)

        val norTableDF = norTableRDD.toDF()

        norTableDF.printSchema()
        if (unitTesting) {
            println("Records count of norTableDF: " + norTableDF.count)
            norTableDF.show()
        }

        norTableDF.registerTempTable("NOR_TABLE_DATA")

        val aggQuery : String = getAggregateQuery()
        println(aggQuery)
        val aggDF = sqlContext.sql(aggQuery)
        aggDF.printSchema()

        if (unitTesting) {
            println("Records count aggregated into aggDF: " + aggDF.count)
            aggDF.take(20).foreach(println) // take subset for testing
        }

        if (saveAsHadoopDataSet) { // write the aggregated records to the database
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
            val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConf, this.getClass)
            jobConfig.setOutputFormat(classOf[TableOutputFormat])
            jobConfig.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
            aggDF.map(makePutAggTable).saveAsHadoopDataset(jobConfig)
        }
    }

    // --------------------------------------------------------------------------
    def aggregateBulkReports( // Aggregate a bunch of reports together : Inefficient; try aggregateByRowKeyRange
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf,
        inputFiles: IndexedSeq[String]
    )
    {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

        import sqlContext.implicits._

        // read all the files presented
        val customSchema = getInputSchema()
        println(customSchema)

        val allDFs = for (inputFile <- inputFiles) yield {
            sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .schema(customSchema)
                .load(inputFile)    
                .select("servedMSISDN", "chargingID", "recordOpeningTime")
                //.limit(100) // temporary limit until all the spark conf options are sorted out
                //.option("quote", "'")

        }
        println("Dataframes generated from list of input csv files: " + allDFs.length)
        if (allDFs.length <= 0) { // till we figure out how to handle this
            println("No dataframes could be generated from list of input csv files")
            return
        }
        var combinedDF = allDFs(0)
        if (allDFs.length > 1) {
            for (i <- 1 until allDFs.length) {
                combinedDF = combinedDF.unionAll(allDFs(i))
            }
        }
        val inputDF = combinedDF.dropDuplicates
        if (unitTesting) {
            inputDF.take(20).foreach(println)
        }
        inputDF.registerTempTable("INPUT_DATA")

        val mappingQuery : String = getMappingQueryForAggJoin()
        println(mappingQuery)
        val inputWithRowkeyDF = sqlContext.sql(mappingQuery)

        inputWithRowkeyDF.printSchema()
        if (unitTesting) {
            inputWithRowkeyDF.take(20).foreach(println)
        }
        inputWithRowkeyDF.registerTempTable("INPUT_DATA")

        val scanRangesQuery : String = getScanRowkeyRangeForAggQuery2()
        println(scanRangesQuery)
        val scanRangesDF = sqlContext.sql(scanRangesQuery)
        if (unitTesting) {
            scanRangesDF.show()
        }
        scanRangesDF.registerTempTable("SCAN_RANGES")
        val scanRangeDF = sqlContext.sql("SELECT MIN(START_ROW_KEY) AS START_ROW_KEY, MAX(END_ROW_KEY) AS END_ROW_KEY FROM SCAN_RANGES")

        val first = scanRangeDF.first
        val scan_rk_min = first.getString(0)
        val scan_rk_max = first.getString(1)

        println("Scan row key range for aggregation: min: " + scan_rk_min + ", scan_max: " + scan_rk_max)

        val scanConf = HBaseConfiguration.create()
        scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
        val hbaseRDD = sc.newAPIHadoopRDD(
            scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val resultRDD = hbaseRDD.map(tuple => tuple._2)

        val norTableRDD = resultRDD.map(AggRow.parseRow)

        val norTableDF = norTableRDD.toDF()

        norTableDF.printSchema()
        if (unitTesting) {
            norTableDF.show()
        }

        norTableDF.registerTempTable("NOR_TABLE_DATA")

        val aggQuery : String = getAggregateByInputJoinQuery()
        println(aggQuery)
        val aggDF = sqlContext.sql(aggQuery)
        aggDF.printSchema()

        if (unitTesting) {
            println("Records count aggregated into aggDF: " + aggDF.count)
            aggDF.take(20).foreach(println) // take subset for testing
        }

        if (saveAsHadoopDataSet) { // write the aggregated records to the database
            // set up Hadoop HBase configuration using TableOutputFormat
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
            val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConf, this.getClass)
            jobConfig.setOutputFormat(classOf[TableOutputFormat])
            jobConfig.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
            aggDF.map(makePutAggTable).saveAsHadoopDataset(jobConfig)
        }
    }

    // --------------------------------------------------------------------------
    def aggregateBulkReports2( // TODO: Aggregate a bunch of reports together
    // TODO: Find a way to use this by getting a DF of rowkey range min,max pairs
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf,
        inputFiles: IndexedSeq[String]
    )
    {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

        import sqlContext.implicits._

        // read all the files presented
        val customSchema = getInputSchema()
        println(customSchema)

        val allDFs = for (inputFile <- inputFiles) yield {
            sqlContext.read
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .schema(customSchema)
                .load(inputFile)    
                .select("servedMSISDN", "chargingID", "recordOpeningTime")
                //.limit(100) // temporary limit until all the spark conf options are sorted out
                //.option("quote", "'")

        }
        println("Dataframes generated from list of input csv files: " + allDFs.length)
        if (allDFs.length <= 0) { // till we figure out how to handle this
            println("No dataframes could be generated from list of input csv files")
            return
        }
        var combinedDF = allDFs(0)
        if (allDFs.length > 1) {
            for (i <- 1 until allDFs.length) {
                combinedDF = combinedDF.unionAll(allDFs(i))
            }
        }
        val inputDF = combinedDF.dropDuplicates
        if (unitTesting) {
            inputDF.take(20).foreach(println)
        }
        inputDF.registerTempTable("INPUT_DATA")

        val mappingQuery : String = getScanRowkeyRangeForAggQuery()
        println(mappingQuery)
        val scanRangeDF = sqlContext.sql(mappingQuery)

        scanRangeDF.printSchema()
        if (unitTesting) {
            scanRangeDF.take(20).foreach(println)
        }

        println("Records in scanRangeDF: " + scanRangeDF.count)

        //scanRangeDF.foreach(row => aggByRowkeyRange(sc, conf, row.getString(0), row.getString(1)))
        //scanRangeDF.collect().foreach(row => aggByRowkeyRange(sc, conf, row.getString(0), row.getString(1))) // not an option, this
        //scanRangeDF.foreach(row => aggByRowkeyRange(sc, conf, row.getString(0), row.getString(1)))
        //scanRangeDF.rdd.map(row => aggByRowkeyRange(sc, conf, row.getString(0), row.getString(1)))
        //for (row <- scanRangeDF.foreach) { }
        //scanRangeDF.take(20).foreach(row => AggByRowkeyRange.load(sc, conf, row.getString(0), row.getString(1))) // this works!!!
        //scanRangeDF.map(row => loadAllRowkeyRanges(sc, conf, row.getString(0), row.getString(1)))
    }

    def aggByRowkeyRange( // TODO: Find a way to use this on a DF of rowkey range min,max pairs
        sc : org.apache.spark.SparkContext,
        conf : org.apache.spark.SparkConf,
        scan_rk_min : String,
        scan_rk_max : String
    )
    {
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)
        //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

        import sqlContext.implicits._

        //val scan_rk_min = row.getString(0)
        //val scan_rk_max = row.getString(1)

        println("Scan row key range for aggregation: min: " + scan_rk_min + ", scan_max: " + scan_rk_max)
        return

        val scanConf = HBaseConfiguration.create()
        scanConf.set(TableInputFormat.INPUT_TABLE, norTableName)
        scanConf.set(TableInputFormat.SCAN_ROW_START, scan_rk_min)
        scanConf.set(TableInputFormat.SCAN_ROW_STOP, scan_rk_max)
        val hbaseRDD = sc.newAPIHadoopRDD(
            scanConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val resultRDD = hbaseRDD.map(tuple => tuple._2)

        val norTableRDD = resultRDD.map(AggRow.parseRow)

        val norTableDF = norTableRDD.toDF()

        norTableDF.printSchema()
        if (unitTesting) {
            norTableDF.show()
        }

        norTableDF.registerTempTable("NOR_TABLE_DATA")

        val aggQuery : String = getAggregateQuery() // DANGER: limitless query!!!
        println(aggQuery)
        val aggDF = sqlContext.sql(aggQuery)
        aggDF.printSchema()

        if (unitTesting) {
            println("Records count aggregated into aggDF: " + aggDF.count)
            aggDF.take(20).foreach(println) // take subset for testing
        }

        if (saveAsHadoopDataSet) { // write the aggregated records to the database
            val hbaseConf = HBaseConfiguration.create()
            hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
            val jobConfig : org.apache.hadoop.mapred.JobConf = new org.apache.hadoop.mapred.JobConf(hbaseConf, this.getClass)
            jobConfig.setOutputFormat(classOf[TableOutputFormat])
            jobConfig.set(TableOutputFormat.OUTPUT_TABLE, aggTableName)
            aggDF.map(makePutAggTable).saveAsHadoopDataset(jobConfig)
        }
    }

    def makePutNorTable(row: org.apache.spark.sql.Row): (ImmutableBytesWritable, Put) = {
        val rowkey = row.getString(0)
        val put = new Put (Bytes.toBytes( rowkey ) )
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CDR_SOURCE"), Bytes.toBytes(row.getString(1).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("REC_POPULATION_TIME"), Bytes.toBytes(row.getString(2).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CDR_REASON"), Bytes.toBytes(row.getString(3).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("PARTIAL_SEQ_NUM"), Bytes.toBytes(row.getInt(4).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("PARTIAL_IND"), Bytes.toBytes(row.getInt(5).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("TERMINATION_CAUSE"), Bytes.toBytes(row.getString(6).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("SOURCE_SEQ_NUM"), Bytes.toBytes(row.getInt(7).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("IMSI"), Bytes.toBytes(row.getString(8).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("MSISDN"), Bytes.toBytes(row.getString(9).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("IMEI"), Bytes.toBytes(row.getString(10).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CHARGING_ID"), Bytes.toBytes(row.getString(11).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("UE_IP_ADDRESS"), Bytes.toBytes(row.getString(12).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("EVENT_START_TIME"), Bytes.toBytes(row.getString(13).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("EVENT_END_TIME"), Bytes.toBytes(row.getString(14).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("DURATION"), Bytes.toBytes(row.getLong(15).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("UPLINK_VOL"), Bytes.toBytes(row.getLong(16).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("DOWNLINK_VOL"), Bytes.toBytes(row.getLong(17).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("TOTAL_VOL"), Bytes.toBytes(row.getLong(18).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("RAT_TYPE"), Bytes.toBytes(row.getInt(19).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("HPMN"), Bytes.toBytes(row.getString(20).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("VPMN"), Bytes.toBytes(row.getString(21).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("ROAM_TYPE"), Bytes.toBytes(row.getInt(22).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("GATEWAY_IP_ADDRESS"), Bytes.toBytes(row.getString(23).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("SERVING_IP_ADDRESS"), Bytes.toBytes(row.getString(24).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("GATEWAY_NODE"), Bytes.toBytes(row.getString(25).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("SERVING_NODE"), Bytes.toBytes(row.getString(26).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("APN"), Bytes.toBytes(row.getString(27).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("APN_TYPE"), Bytes.toBytes(row.getString(28).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CHARGING_CHARACTERISTICS"), Bytes.toBytes(row.getInt(29).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("PAY_TYPE"), Bytes.toBytes(row.getString(30).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("APN_RA_FLAG"), Bytes.toBytes(row.getString(31).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("FILE_NAME"), Bytes.toBytes(row.getString(32).toString ))
        return (new ImmutableBytesWritable( Bytes.toBytes( rowkey ) ), put)
    }


    def makePutSlowNorTable(row: org.apache.spark.sql.Row): Put = {
        val rowkey = row.getString(0)
        val put = new Put (Bytes.toBytes( rowkey ) )
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CDR_SOURCE"), Bytes.toBytes(row.getString(1).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("REC_POPULATION_TIME"), Bytes.toBytes(row.getString(2).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CDR_REASON"), Bytes.toBytes(row.getString(3).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("PARTIAL_SEQ_NUM"), Bytes.toBytes(row.getInt(4).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("PARTIAL_IND"), Bytes.toBytes(row.getInt(5).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("TERMINATION_CAUSE"), Bytes.toBytes(row.getString(6).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("SOURCE_SEQ_NUM"), Bytes.toBytes(row.getInt(7).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("IMSI"), Bytes.toBytes(row.getString(8).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("MSISDN"), Bytes.toBytes(row.getString(9).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("IMEI"), Bytes.toBytes(row.getString(10).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CHARGING_ID"), Bytes.toBytes(row.getString(11).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("UE_IP_ADDRESS"), Bytes.toBytes(row.getString(12).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("EVENT_START_TIME"), Bytes.toBytes(row.getString(13).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("EVENT_END_TIME"), Bytes.toBytes(row.getString(14).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("DURATION"), Bytes.toBytes(row.getLong(15).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("UPLINK_VOL"), Bytes.toBytes(row.getLong(16).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("DOWNLINK_VOL"), Bytes.toBytes(row.getLong(17).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("TOTAL_VOL"), Bytes.toBytes(row.getLong(18).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("RAT_TYPE"), Bytes.toBytes(row.getInt(19).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("HPMN"), Bytes.toBytes(row.getString(20).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("VPMN"), Bytes.toBytes(row.getString(21).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("ROAM_TYPE"), Bytes.toBytes(row.getInt(22).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("GATEWAY_IP_ADDRESS"), Bytes.toBytes(row.getString(23).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("SERVING_IP_ADDRESS"), Bytes.toBytes(row.getString(24).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("GATEWAY_NODE"), Bytes.toBytes(row.getString(25).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("SERVING_NODE"), Bytes.toBytes(row.getString(26).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("APN"), Bytes.toBytes(row.getString(27).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("APN_TYPE"), Bytes.toBytes(row.getString(28).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("CHARGING_CHARACTERISTICS"), Bytes.toBytes(row.getInt(29).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("PAY_TYPE"), Bytes.toBytes(row.getString(30).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("APN_RA_FLAG"), Bytes.toBytes(row.getString(31).toString ))
        put.addColumn( norTableCFDataBytes, Bytes.toBytes("FILE_NAME"), Bytes.toBytes(row.getString(32).toString ))
        return put
    }

    def makePutAggTable(row: org.apache.spark.sql.Row): (ImmutableBytesWritable, Put) = {
        val rowkey = row.getString(0)
        val put = new Put (Bytes.toBytes( rowkey ) )
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("MSISDN"), Bytes.toBytes(row.getString(1).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("CHARGING_ID"), Bytes.toBytes(row.getString(2).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("RAT_TYPE"), Bytes.toBytes(row.getInt(3).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("EVENT_DATE"), Bytes.toBytes(row.getString(4).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("NUM_RECORDS"), Bytes.toBytes(row.getInt(5).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("DURATION"), Bytes.toBytes(row.getLong(6).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("UPLINK_VOL"), Bytes.toBytes(row.getLong(7).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("DOWNLINK_VOL"), Bytes.toBytes(row.getLong(8).toString ))
        put.addColumn( aggTableCFDataBytes, Bytes.toBytes("TOTAL_VOL"), Bytes.toBytes(row.getLong(9).toString ))

        return (new ImmutableBytesWritable( Bytes.toBytes( rowkey ) ), put)
    }

    def getInputSchema() : StructType = {
        var columns = ArrayBuffer[StructField](
                StructField("servedIMSI"        , StringType, true)
                , StructField("ggsnIPAddress"     , StringType, true)
                , StructField("chargingID"        , StringType, true)
                , StructField("sgsnIPAddress"     , StringType, true)
                , StructField("servedPDPAddress"  , StringType, true)
                , StructField("ListOfTrafficVlm_dataVolumeGPRSUplink", LongType, true)
                , StructField("ListOfTrafficVlm_dataVolumeGPRSDownlink", LongType, true)
                , StructField("ListOfTrafficVlm_changeCondition"  , StringType, true)
                , StructField("ListOfTrafficVlm_changeTime", StringType, true)
                , StructField("recordOpeningTime" , StringType, true)
                , StructField("duration"          , LongType, true)
                , StructField("causeForRecClosing", StringType, true)
                , StructField("recordSequenceNumber", IntegerType, true)
                , StructField("servedMSISDN"      , StringType, true)
                , StructField("chargingCharacteristics", IntegerType, true)
                , StructField("sgsnPLMNIdentifier", StringType, true)
                , StructField("servedIMEISV"      , StringType, true)
                , StructField("rATType"           , IntegerType, true)
                , StructField("userLocationInformation", StringType, true)
                , StructField("accessPointNameNI" , StringType, true)
                , StructField("ruleSpaceId"       , StringType, true)
                , StructField("ratingGroup"       , StringType, true)
                , StructField("ulvlm_rtgrp"       , StringType, true)
                , StructField("dlvlm_rtgrp"       , StringType, true)
                , StructField("OriginalFileName"  , StringType, true)
            )

        val customSchema = StructType(columns)
        return customSchema
    }            

    def getMappingQuery(): String = {
        val query = """
SELECT CONCAT(SUBSTR(servedMSISDN,2,15),'|',LPAD(chargingID, 14, '0'),'|',recordOpeningTime,'|') AS ROW_KEY
, '""" + cdr_source + """' AS CDR_SOURCE
, '""" + rec_population_time + """' AS REC_POPULATION_TIME
, ''     AS CDR_REASON
, 0      AS PARTIAL_SEQ_NUM
, 0      AS PARTIAL_IND
, INPUT_DATA.causeForRecClosing  AS TERMINATION_CAUSE
, IF(INPUT_DATA.recordSequenceNumber IS NULL, 0, INPUT_DATA.recordSequenceNumber) AS SOURCE_SEQ_NUM
, INPUT_DATA.servedIMSI          AS IMSI
, SUBSTR(INPUT_DATA.servedMSISDN, 2, 15)        AS MSISDN
, INPUT_DATA.servedIMEISV        AS IMEI
, INPUT_DATA.chargingID          AS CHARGING_ID
, INPUT_DATA.servedPDPAddress    AS UE_IP_ADDRESS
, INPUT_DATA.recordOpeningTime   AS EVENT_START_TIME
--, INPUT_DATA.recordOpeningTime   AS EVENT_END_TIME
--, CAST(CAST(CAST(SUBSTR(INPUT_DATA.recordOpeningTime, 0, 19) AS TIMESTAMP) + INTERVAL 10 SECONDS AS TIMESTAMP) AS STRING)  AS EVENT_END_TIME
--, CAST(from_utc_timestamp(to_utc_timestamp(SUBSTR(INPUT_DATA.recordOpeningTime, 0, 19)) + INPUT_DATA.duration) AS STRING)  AS EVENT_END_TIME
, CAST(from_unixtime(unix_timestamp(SUBSTR(INPUT_DATA.recordOpeningTime, 0, 19)) + INPUT_DATA.duration) AS STRING)  AS EVENT_END_TIME
, IF(INPUT_DATA.duration IS NULL, 0, INPUT_DATA.duration)            AS DURATION
, IF(INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSUplink IS NULL, 0, INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSUplink)   AS UPLINK_VOL
, IF(INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSDownlink IS NULL, 0, INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSDownlink) AS DOWNLINK_VOL
--, (INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSUplink+INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSDownlink) AS TOTAL_VOL
, (
    IF(INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSUplink IS NULL, 0, INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSUplink)
    + IF(INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSDownlink IS NULL, 0, INPUT_DATA.ListOfTrafficVlm_dataVolumeGPRSDownlink)
  ) AS TOTAL_VOLUME
, IF(INPUT_DATA.rATType IS NULL, 0, INPUT_DATA.rATType)   AS RAT_TYPE
, SUBSTR(INPUT_DATA.servedMSISDN, 0, 6)  AS HPMN
, INPUT_DATA.sgsnPLMNIdentifier  AS VPMN
, 0                                   AS ROAM_TYPE
, INPUT_DATA.ggsnIPAddress       AS GATEWAY_IP_ADDRESS
, INPUT_DATA.sgsnIPAddress       AS SERVING_IP_ADDRESS
, 'Unknown' AS GATEWAY_NODE
, 'Unknown' AS SERVING_NODE
--, INPUT_DATA.ggsnIPAddress=TDS_RA_NODE.IP_ADDRESS then NODE_ID ELSE 'Unknown' AS GATEWAY_NODE
--, INPUT_DATA.sgsnIPAddress=TDS_RA_NODE.IP_ADDRESS then NODE_ID ELSE 'Unknown' AS SERVING_NODE
, INPUT_DATA.accessPointNameNI   AS APN
, 'Other'                             AS APN_TYPE
, IF(INPUT_DATA.chargingCharacteristics IS NULL, 0, INPUT_DATA.chargingCharacteristics) AS CHARGING_CHARACTERISTICS
, 'Unknown' AS PAY_TYPE
, 'O'                        AS APN_RA_FLAG
, INPUT_DATA.OriginalFileName    AS FILE_NAME
FROM INPUT_DATA"""
        return query
    }

    def getMappingQueryForAggJoin(): String = {
        val query = """
SELECT CONCAT(SUBSTR(servedMSISDN,2,15),'|',LPAD(chargingID, 14, '0'),'|',recordOpeningTime,'|') AS ROW_KEY
, SUBSTR(INPUT_DATA.servedMSISDN, 2, 15)        AS MSISDN
, INPUT_DATA.chargingID          AS CHARGING_ID
, INPUT_DATA.recordOpeningTime   AS EVENT_START_TIME
FROM INPUT_DATA"""
        return query
    }

    def getTableSchema() : StructType = {
        var columns = ArrayBuffer[StructField](
            StructField("ROW_KEY",  StringType, true)
            , StructField("CDR_SOURCE",  StringType, false)
            , StructField("REC_POPULATION_TIME",  StringType, false)
            , StructField("CDR_REASON",  StringType, false)
            , StructField("PARTIAL_SEQ_NUM",  IntegerType, false)
            , StructField("PARTIAL_IND",  IntegerType, false)
            , StructField("TERMINATION_CAUSE",  StringType, true)
            , StructField("SOURCE_SEQ_NUM",  IntegerType, true)
            , StructField("IMSI",  StringType, true)
            , StructField("MSISDN",  StringType, true)
            , StructField("IMEI",  StringType, true)
            , StructField("CHARGING_ID",  StringType, true)
            , StructField("UE_IP_ADDRESS",  StringType, true)
            , StructField("EVENT_START_TIME",  StringType, true)
            , StructField("EVENT_END_TIME",  StringType, true)
            , StructField("DURATION",  LongType, true)
            , StructField("UPLINK_VOL",  LongType, true)
            , StructField("DOWNLINK_VOL",  LongType, true)
            , StructField("TOTAL_VOL",  LongType, true)
            , StructField("RAT_TYPE",  IntegerType, true)
            , StructField("HPMN",  StringType, true)
            , StructField("VPMN",  StringType, true)
            , StructField("ROAM_TYPE",  IntegerType, false)
            , StructField("GATEWAY_IP_ADDRESS",  StringType, true)
            , StructField("SERVING_IP_ADDRESS",  StringType, true)
            , StructField("GATEWAY_NODE",  StringType, false)
            , StructField("SERVING_NODE",  StringType, false)
            , StructField("APN",  StringType, true)
            , StructField("APN_TYPE",  StringType, false)
            , StructField("CHARGING_CHARACTERISTICS",  IntegerType, true)
            , StructField("PAY_TYPE",  StringType, false)
            , StructField("APN_RA_FLAG",  StringType, false)
            , StructField("FILE_NAME",  StringType, true)
        )
        val customSchema = StructType(columns)
        return customSchema
    }            

    def getScanRowkeyRangeForAggQuery(): String = {
        val query = """
SELECT
CONCAT(
    CAST(SUBSTR(input_data.servedMSISDN,2,15)   AS VARCHAR(100)),
    '|', LPAD(CAST(input_data.chargingID   AS VARCHAR(100)), 14, '0'),
    '|'
) AS START_ROW_KEY,
CONCAT(
    CAST(SUBSTR(input_data.servedMSISDN,2,15)   AS VARCHAR(100)),
    '|', LPAD(CAST((CAST(input_data.chargingID AS BIGINT)+1)   AS VARCHAR(100)), 14, '0'),
    '|'
) AS END_ROW_KEY
FROM INPUT_DATA input_data
GROUP BY
input_data.servedMSISDN,
input_data.chargingID
"""
        return query
    }

    def getScanRowkeyRangeForAggQuery2(): String = {
        val query = """
SELECT
CONCAT(
    CAST(input_data.MSISDN   AS VARCHAR(100)),
    '|', LPAD(CAST(input_data.CHARGING_ID   AS VARCHAR(100)), 14, '0'),
    '|'
) AS START_ROW_KEY,
CONCAT(
    CAST(input_data.MSISDN   AS VARCHAR(100)),
    '|', LPAD(CAST((CAST(input_data.CHARGING_ID AS BIGINT)+1)   AS VARCHAR(100)), 14, '0'),
    '|'
) AS END_ROW_KEY
FROM INPUT_DATA input_data
GROUP BY
input_data.MSISDN,
input_data.CHARGING_ID
"""
        return query
    }

    def getAggregateByInputJoinQuery(): String = {
        val query = """
SELECT
CONCAT(
    CAST(nor_data.msisdn   AS VARCHAR(100)),
    '|', LPAD(CAST(nor_data.CHARGING_ID   AS VARCHAR(100)), 14, '0'),
    '|', SUBSTR(CAST(MIN(nor_data.EVENT_START_TIME)   AS VARCHAR(100)), 0, 10),
    '|'
) AS ROW_KEY,
CAST(nor_data.msisdn   AS VARCHAR(100))  MSISDN,
CAST(nor_data.CHARGING_ID   AS VARCHAR(100))  CHARGING_ID,
CAST(nor_data.RAT_TYPE   AS INTEGER)  RAT_TYPE,
SUBSTR(CAST(MIN(nor_data.EVENT_START_TIME)   AS VARCHAR(100)), 0, 10) EVENT_DATE,
CAST(COUNT(DISTINCT((nor_data.EVENT_START_TIME)))   AS INTEGER) NUM_RECORDS,
SUM(CAST(nor_data.duration   AS BIGINT))  DURATION,
SUM(CAST(nor_data.UPLINK_VOL   AS BIGINT))  UPLINK_VOL,
SUM(CAST(nor_data.DOWNLINK_VOL   AS BIGINT))  DOWNLINK_VOL,
SUM(CAST(nor_data.TOTAL_VOL   AS BIGINT))  TOTAL_VOL
FROM NOR_TABLE_DATA nor_data
INNER JOIN INPUT_DATA input_data ON (nor_data.msisdn = input_data.MSISDN AND nor_data.CHARGING_ID = input_data.CHARGING_ID)
GROUP BY
CAST(nor_data.msisdn   AS VARCHAR(100)),
CAST(nor_data.CHARGING_ID   AS VARCHAR(100)),
CAST(nor_data.RAT_TYPE   AS INTEGER)
--SUBSTR(CAST(nor_data.EVENT_START_TIME   AS VARCHAR(100)), 0, 10)
--LIMIT 100
"""
        return query
    }

    def getAggregateQuery(limit: Int = 0): String = {
        val query = """
SELECT
CONCAT(
    CAST(nor_data.msisdn   AS VARCHAR(100)),
    '|', LPAD(CAST(nor_data.CHARGING_ID   AS VARCHAR(100)), 14, '0'),
    '|', SUBSTR(CAST(MIN(nor_data.EVENT_START_TIME)   AS VARCHAR(100)), 0, 10),
    '|'
) AS ROW_KEY,
CAST(nor_data.msisdn   AS VARCHAR(100))  MSISDN,
CAST(nor_data.CHARGING_ID   AS VARCHAR(100))  CHARGING_ID,
CAST(nor_data.RAT_TYPE   AS INTEGER)  RAT_TYPE,
SUBSTR(CAST(MIN(nor_data.EVENT_START_TIME)   AS VARCHAR(100)), 0, 10) EVENT_DATE,
CAST(COUNT(DISTINCT((nor_data.EVENT_START_TIME)))   AS INTEGER) NUM_RECORDS,
SUM(CAST(nor_data.duration   AS BIGINT))  DURATION,
SUM(CAST(nor_data.UPLINK_VOL   AS BIGINT))  UPLINK_VOL,
SUM(CAST(nor_data.DOWNLINK_VOL   AS BIGINT))  DOWNLINK_VOL,
SUM(CAST(nor_data.TOTAL_VOL   AS BIGINT))  TOTAL_VOL
FROM NOR_TABLE_DATA nor_data
GROUP BY
CAST(nor_data.msisdn   AS VARCHAR(100)),
CAST(nor_data.CHARGING_ID   AS VARCHAR(100)),
CAST(nor_data.RAT_TYPE   AS INTEGER)
--SUBSTR(CAST(nor_data.EVENT_START_TIME   AS VARCHAR(100)), 0, 10)
""" + { if (limit > 0) s"LIMIT ${limit}" else "" }
        return query
    }

    def getInfoFromFilename(fileName : String) : Array[String] = {
        val file = new File(fileName)
        //val dirName = file.getParent()
        //val procType = dirName.substring(dirName.lastIndexOf("/")+1)
        val baseName = file.getName()
        //println("Dirname: '" + dirName + "', BaseName: '" + baseName + "', DataType = '" + procType + "'")
        val namePart = baseName.substring(0, baseName.lastIndexOf("."))
        //println("namePart = " + namePart)
        var parts : Array[String] = namePart.split("_")
        return parts
    }

    def getAggFilename(fileName : String, ext : String) : String = {
        val file = new File(fileName)
        //val dirName = file.getParent()
        //val procType = dirName.substring(dirName.lastIndexOf("/")+1)
        val baseName = file.getName()
        //println("Dirname: '" + dirName + "', BaseName: '" + baseName + "', DataType = '" + procType + "'")
        val name = aggDirPath + "/" + baseName.substring(0, baseName.lastIndexOf(".")) + ext
        println(s"Aggregation file name = ${name}")
        return name
    }

    def enableUnitTesting : Unit = {
        unitTesting = true // set true for unit testing only - has additional overhead
        saveAsHadoopDataSet = false // set true for actual table write for production functionality
        testSlowPut = true // for unit testing only

        println("Unit testing enabled")
    }

}
*/