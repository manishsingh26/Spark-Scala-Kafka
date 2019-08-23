package DataStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ProducerMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder
                .appName("Spark-Kafka-Producer-Integration")
                .master("local")
                .getOrCreate()

    val mySchema = StructType(Array(
                    StructField("id", StringType),
                    StructField("make", StringType),
                    StructField("fuel_type", StringType),
                    StructField("aspiration", StringType),
                    StructField("num_of_doors", StringType),
                    StructField("body_style", StringType),
                    StructField("drive_wheels", StringType),
                    StructField("engine_location", StringType),
                    StructField("wheel_base", StringType),
                    StructField("length", StringType),
                    StructField("width", StringType),
                    StructField("height", StringType),
                    StructField("curb_weight", StringType),
                    StructField("engine_type", StringType),
                    StructField("num_of_cylinders", StringType),
                    StructField("engine_size", StringType),
                    StructField("fuel_system", StringType),
                    StructField("compression_ratio", StringType),
                    StructField("horsepower", StringType),
                    StructField("peak_rpm", StringType),
                    StructField("city_mpg", StringType),
                    StructField("highway_mpg", StringType),
                    StructField("price", StringType)))

    val streamingDataFrame = spark
                            .readStream
                            .schema(mySchema)
                            .csv("/home/msingh/Documents/StreamDataSample/KafkaCarInput")

    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
                      .writeStream
                      .format("kafka")
                      .option("topic", "cars")
                      .option("kafka.bootstrap.servers", "localhost:9092")
                      .option("checkpointLocation", "/home/msingh/Documents/StreamDataSample/KafkaTemp")
                      .start()

    spark.streams.awaitAnyTermination()
    spark.stop()
  }
}
