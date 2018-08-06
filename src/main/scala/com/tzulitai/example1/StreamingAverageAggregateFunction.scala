package com.tzulitai.example1

import com.tzulitai.MissingSolutionException
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * An aggregate function that demonstrates the more efficient way of calculating
  * streaming windowed averages.
  */
class StreamingAverageAggregateFunction extends AggregateFunction[DataPoint, AverageAccumulator, Double] {

  override def createAccumulator(): AverageAccumulator = {
    throw new MissingSolutionException
  }

  override def add(datapoint: DataPoint, accumulator: AverageAccumulator): AverageAccumulator = {
    throw new MissingSolutionException
  }

  override def getResult(accumulator: AverageAccumulator): Double = {
    throw new MissingSolutionException
  }

  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
    throw new MissingSolutionException
  }
}

case class AverageAccumulator(total: Double, count: Int)
