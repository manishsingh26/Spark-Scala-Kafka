name := "StreamKafkaConsumer"
version := "0.1"
scalaVersion := "2.11.10"
val sparkVersion = "2.2.0"

libraryDependencies ++= Seq("org.scala-lang" % "scala-library" % scalaVersion.value,
                            "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
                            "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion,
                            "org.apache.kafka" % "kafka-clients" % "0.11.0.1",
                            "org.elasticsearch" % "elasticsearch-hadoop" % "6.4.1")
