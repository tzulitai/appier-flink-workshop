package com.tzulitai.example3

import java.util.{Collections, PriorityQueue}

import com.tzulitai.MissingSolutionException
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

class EventTimeJoinFunction extends CoProcessFunction[Trade, Customer, EnrichedTrade] {

  lazy val tradeBufferState: ValueState[PriorityQueue[EnrichedTrade]] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[PriorityQueue[EnrichedTrade]](
        "tradeBuffer",
        createTypeInformation[PriorityQueue[EnrichedTrade]]
      )
    )

  lazy val customerBufferState: ValueState[PriorityQueue[Customer]] =
    getRuntimeContext.getState(
      new ValueStateDescriptor[PriorityQueue[Customer]](
        "customerBuffer",
        createTypeInformation[PriorityQueue[Customer]]
      )
    )

  override def processElement1(
      trade: Trade,
      ctx: CoProcessFunction[Trade, Customer, EnrichedTrade]#Context,
      collector: Collector[EnrichedTrade]): Unit = {

    System.out.println("Received " + trade.toString)

    throw new MissingSolutionException
  }

  override def processElement2(
      customer: Customer,
      ctx: CoProcessFunction[Trade, Customer, EnrichedTrade]#Context,
      collector: Collector[EnrichedTrade]): Unit = {

    System.out.println("Received " + customer.toString)

    throw new MissingSolutionException
  }

  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[Trade, Customer, EnrichedTrade]#OnTimerContext,
      collector: Collector[EnrichedTrade]): Unit = {

    throw new MissingSolutionException
  }

  // ------------------------------------------------------------------------------------
  //   Utility methods
  // ------------------------------------------------------------------------------------

  def joinWithCustomerInfo(trade: Trade): EnrichedTrade = {
    EnrichedTrade(trade, getCustomerInfoAtEventTime(trade.timestamp))
  }

  def getCustomerInfoAtEventTime(targetTimestamp: Long): String = {
    val customerInfos = customerBufferState.value()

    if (customerInfos != null) {
      val customerInfosCopy = new PriorityQueue[Customer](customerInfos)

      while (!customerInfosCopy.isEmpty) {
        val customer = customerInfosCopy.poll()
        if (customer.timestamp <= targetTimestamp) {
          return customer.customerInfo
        }
      }
    }

    "N/A"
  }

  def enqueueEnrichedTrade(enrichedTrade: EnrichedTrade): Unit = {
    var tradeBuffer = tradeBufferState.value()

    if (tradeBuffer == null) {
      tradeBuffer = new PriorityQueue[EnrichedTrade]()
    }
    tradeBuffer.add(enrichedTrade)

    tradeBufferState.update(tradeBuffer)
  }

  def enqueueCustomer(customer: Customer): Unit = {
    var customerBuffer = customerBufferState.value()

    if (customerBuffer == null) {
      customerBuffer = new PriorityQueue[Customer](10, Collections.reverseOrder())
    }
    customerBuffer.add(customer)

    customerBufferState.update(customerBuffer)
  }

  def dequeueEnrichedTrade(collector: Collector[EnrichedTrade]): EnrichedTrade = {
    val tradeBuffer = tradeBufferState.value()

    val lastEmittedEnrichedTrade = tradeBuffer.poll()
    tradeBufferState.update(tradeBuffer)

    lastEmittedEnrichedTrade
  }

  def purgeBufferedCustomerDataOlderThanEventTime(targetTimestamp: Long): Unit = {
    val customerBuffer = customerBufferState.value()
    val purgedBuffer = new PriorityQueue[Customer](10, Collections.reverseOrder())

    var customerTimestamp = Long.MaxValue
    while (!customerBuffer.isEmpty && customerTimestamp >= targetTimestamp) {
      val customer = customerBuffer.poll()
      purgedBuffer.add(customer)
      customerTimestamp = customer.timestamp
    }

    customerBufferState.update(purgedBuffer)
  }
}
