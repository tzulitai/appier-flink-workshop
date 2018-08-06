package com.tzulitai.example3

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
  * Example #3: Low latency stream-stream event-time joins.
  */
object EventTimeJoinJob {

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // simulated trade stream
    val tradeStream = Sources.generateTrades(env)

    // simulated customer stream
    val customerStream = Sources.generateCustomers(env)

    val enrichedTradeStream = tradeStream
      .keyBy(_.customerID)
      .connect(customerStream.keyBy(_.customerID))
      .process(new EventTimeJoinFunction)

    enrichedTradeStream.print()

    env.execute
  }

}
