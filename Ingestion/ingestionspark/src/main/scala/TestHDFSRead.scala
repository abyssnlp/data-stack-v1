import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import java.io.File

object TestHDFSRead extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark: SparkSession = SparkSession
    .builder()
    .appName("TestHDFSSink")
    .master("local[*]")
    .getOrCreate()

  val config: Config = ConfigFactory.parseFile(new File(System.getProperty("user.dir") + File.separator + "config"
                                                + File.separator + "postgres.conf"))

  val postgresqlSinkOptions: Map[String, String] = Map (
    "dbtable" -> "public.retail",
    "user" -> config.getString("localpostgres.secrets.username"),
    "password" -> config.getString("localpostgres.secrets.password"),
    "driver" -> config.getString("localpostgres.secrets.driver"),
    "url" -> config.getString("localpostgres.secrets.url")
  )


  val sc: SparkContext = spark.sparkContext

  val schema: StructType = StructType(
    List(
      StructField("invoiceNumber", IntegerType),
      StructField("stockCode", StringType),
      StructField("description", StringType),
      StructField("quantity", IntegerType),
      StructField("invoiceDate", StringType),
      StructField("unitPrice", DoubleType),
      StructField("customerId", DoubleType),
      StructField("country", StringType)
    )
  )

  val fileDF = spark.readStream
    .format("csv")
    .option("maxFilesPerTrigger", 1)
    .schema(schema)
    .load("hdfs://localhost:19000/retailtest/*.csv")

  fileDF.printSchema()

  fileDF.writeStream.foreachBatch {
    (batch: DataFrame, _: Long) =>
      batch.write
        .format("jdbc")
        .options(postgresqlSinkOptions)
        .mode(SaveMode.Append)
        .save()
  }
    .start()
    .awaitTermination()
}
