package com.tzulitai.example1

import java.util
import java.util.Collections

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark

import scala.collection.JavaConverters._

/**
  * A timestamped-source that generates a sawtooth pattern.
  */
class TimestampedSawtoothSource(val periodMs: Long, val slowdownFactor: Int, val numSteps: Int)
    extends RichSourceFunction[DataPoint] with ListCheckpointed[(Long, Int)] {

  @volatile var running: Boolean = true
  @volatile var currentTimeMs: Long = 0

  var currentStep: Int = 0

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)

    val now = System.currentTimeMillis()
    if (currentTimeMs == 0) {
      currentTimeMs = now - (now % 1000)
    }
  }

  override def run(ctx: SourceContext[DataPoint]): Unit = {
    while (running) {
      val phase: Double = currentStep.asInstanceOf[Double] / numSteps.asInstanceOf[Double]
      currentStep = (currentStep + 1) % numSteps

      ctx.getCheckpointLock.synchronized {
        ctx.collectWithTimestamp(DataPoint("test-key", currentTimeMs, phase), currentTimeMs)
        ctx.emitWatermark(new Watermark(currentTimeMs))
        currentTimeMs += periodMs
      }

      timeSync()
    }
  }

  override def cancel(): Unit = {
    running = false
  }

  private def timeSync(): Unit = {
    // Sync up with real time
    val realTimeDeltaMs = currentTimeMs - System.currentTimeMillis()
    var sleepTime = periodMs + realTimeDeltaMs + randomJitter()

    if(slowdownFactor != 1){
      sleepTime = periodMs * slowdownFactor
    }

    if(sleepTime > 0) {
      Thread.sleep(sleepTime)
    }
  }

  private def randomJitter(): Long = {
    var sign = -1.0

    if(Math.random() > 0.5){
      sign = 1.0
    }

    (Math.random() * periodMs * sign).asInstanceOf[Long]
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[(Long, Int)] = {
    Collections.singletonList((currentTimeMs, currentStep))
  }

  override def restoreState(state: util.List[(Long, Int)]): Unit = {
    for (s <- state.asScala) {
      currentTimeMs = s._1
      currentStep = s._2
    }
  }
}