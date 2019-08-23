package DataSteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ConsumerMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
                .builder
                .appName("Spark-Kafka-Consumer-Integration")
                .master("local[*]")
                .getOrCreate()

    val df = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "cars")
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()

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

    import spark.implicits._
    val dfStream = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
                    .select(from_json($"value", mySchema).as("data"), $"timestamp")
                    .select("data.*", "timestamp")

    dfStream
      .writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "/home/msingh/Documents/StreamDataSample/ElasticTemp")
      .start("cars/doc")
      .awaitTermination()

  }
}
