package com.tzulitai.example2

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Example #2: Simple stream-stream join by key.
  */
object ActionClickEnrichmentJoinJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val actionStream = Sources.generateActions(env)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Action](Time.seconds(0)) {
          override def extractTimestamp(element: Action): Long = element.timestamp
        }
      )

    val clickStream = Sources.generateClicks(env)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Click](Time.seconds(0)) {
          override def extractTimestamp(element: Click): Long = element.timestamp
        }
      )

    val enrichedActionStream = actionStream
      .keyBy(_.bidID)
      .connect(clickStream.keyBy(_.bidID))
      .process(new ActionClickJoinFunction(10000000, 100000))

    enrichedActionStream.print()
    env.execute()
  }

}
