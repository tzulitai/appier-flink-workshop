package com.tzulitai.example1

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  * A job that generates and writes a sine wave to Kafka
  */
object SinewaveGeneratorKafkaJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.enableCheckpointing(1000)

    val SLOWDOWN_FACTOR = 1
    val PERIOD_MS = 100

    // produce the sine wave datapoints to Kafka
    env
      .addSource(new TimestampedSawtoothSource(PERIOD_MS, SLOWDOWN_FACTOR, 10))
      .map(x => DataPoint(x.key, x.timestamp, x.value * 2 * Math.PI)) // sine wave
      .addSink(new FlinkKafkaProducer011[DataPoint]("localhost:9092", "sinewave-topic", new DataPointSerializationSchema))

    env.execute()
  }
}
