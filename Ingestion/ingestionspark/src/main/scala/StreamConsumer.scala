import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

case class Retail(invoiceNumber: Int, stockCode: String, description: String, quantity: Int,
                  invoiceDate: String, unitPrice: Double, customerId:Double, country: String)

// TODO: Add logging (logback.xml)
// TODO: Add scalafmt
// TODO: Make code modular
// TODO: Optimize build.sbt
// TODO: build fat jar for spark-submit

object StreamConsumer extends App {

    final val ENV = "DEV"

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
                    .setMaster("local[*]")
                    .setAppName("Kafka Stream Consumer")

    conf.set("spark.hadoop.dfs.client.use.datanode.hostname", "true")

    val buffer = if(ENV == "PROD") Minutes(1) else if(ENV == "DEV") Seconds(15)
    val ssc: StreamingContext = new StreamingContext(conf, buffer.asInstanceOf[Duration])
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map[String, Object] (
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "testGroup1",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("bankretail")

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
    )

    val orders = stream.flatMap(
        line => {
            val fields = line.value().split(",")
            try{
                List(Retail(fields(0).toInt, fields(1), fields(2), fields(3).toInt,
                    fields(4), fields(5).toDouble, fields(6).toDouble, fields(7)))
            } catch {
                case e: Throwable => println(s"Format error parsing into case class!" + line + s"${e.printStackTrace()}")
                List()
            }
        }
    )

    // Windowed Writes ( 1 minute )
    if(ENV == "PROD") {
        val orderStream1Minute: DStream[Retail] = orders.window(Minutes(1))
        orderStream1Minute.foreachRDD {
            currentRDD =>
                val spark = SparkSession.builder().config(currentRDD.sparkContext.getConf).getOrCreate()
                import spark.implicits._
                val currentDF: DataFrame = currentRDD.toDF()
                currentDF.write.csv(s"hdfs://localhost:19000/bankretail/test-${System.currentTimeMillis()/(1000*60)}.csv")
        }
    }
    // DEV : Windowed writes (15 seconds)
    else if(ENV == "DEV") {
        val orderStreamDev: DStream[Retail] = orders.window(Seconds(15))
        orderStreamDev.foreachRDD {
            currentRDD =>
                val sparkSession = SparkSession.builder().config(currentRDD.sparkContext.getConf).getOrCreate()
                import sparkSession.implicits._
                val currentDF: DataFrame = currentRDD.toDF()
                currentDF.write.csv(s"hdfs://localhost:19000/retailtest/test-${System.currentTimeMillis()}.csv")
        }
//        orderStreamDev.saveAsTextFiles("hdfs://localhost:19000/retailtest/test", "csv")
    }

    ssc.start()
    ssc.awaitTermination()

}
