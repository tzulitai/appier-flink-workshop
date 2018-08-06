package com.tzulitai.example1

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Example #1: Streaming Windowed Average
  *
  * This example demonstrates calculating an average over data points of time-based windows.
  * The input to the time-based windows is a series of data points that form a sine wave.
  * Because of this, when breaking up the stream into windows using event time, the calculated
  * average of each window should remain constant.
  */
object StreamingWindowedAverageJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val SLOWDOWN_FACTOR = 1
    val PERIOD_MS = 100

    // initial data - just timestamped messages
    val timestampSource = env.addSource(new TimestampedSawtoothSource(PERIOD_MS, SLOWDOWN_FACTOR, 10))

    val stream = timestampSource
      .map(x => DataPoint(x.key, x.timestamp, x.value * 2 * Math.PI)) // sine wave
      .keyBy(_.key)
      .timeWindow(Time.seconds(1))
      .apply(
        (
          key : String,
          window : TimeWindow,
          input : scala.Iterable[DataPoint],
          out : org.apache.flink.util.Collector[Double]
        ) => {
          var sum = 0.0
          var count = 0.0

          for (i <- input) {
            count += 1
            sum += i.value
          }

          out.collect(sum / count)
        }
      )

    stream.print()

    env.execute()
  }

}
