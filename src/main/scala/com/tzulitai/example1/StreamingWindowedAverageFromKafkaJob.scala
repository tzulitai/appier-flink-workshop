package com.tzulitai.example1

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * A job that consumes a sine wave from Kafka, and calculates a windowed average over it.
  */
object StreamingWindowedAverageFromKafkaJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(1000)

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", "localhost:9092")
    kafkaProps.put("group.id", "tzulitai")

    val sinewaveFromKafka = env
      .addSource(
        new FlinkKafkaConsumer011[DataPoint]("sinewave-topic", new DataPointSerializationSchema, kafkaProps))
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[DataPoint](Time.seconds(0)) {
          override def extractTimestamp(element: DataPoint): Long = element.timestamp
        }
      )

    val stream = sinewaveFromKafka
      .keyBy(_.key)
      .timeWindow(Time.seconds(1))
      .apply(
        (
          key : String,
          window : TimeWindow,
          input : scala.Iterable[DataPoint],
          out : org.apache.flink.util.Collector[String]
        ) => {
          var sum = 0.0
          var count = 0.0

          for (i <- input) {
            count += 1
            sum += i.value
          }

          out.collect("Window " +  window.getEnd + " , Avg: " + (sum / count).toString)
        }
      )

    stream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "output-avg-topic", new SimpleStringSchema()))

    env.execute()
  }
}
