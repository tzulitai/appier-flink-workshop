package com.tzulitai.example2

import com.tzulitai.MissingSolutionException
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
  * Created by tzulitai on 06/08/2018.
  */
class ActionClickJoinFunction(clickTtl: Long, actionTtl: Long)
    extends CoProcessFunction[Action, Click, EnrichedAction] {

  lazy val storedClickState: ValueState[Click] =
    getRuntimeContext.getState(new ValueStateDescriptor[Click]("clickState", createTypeInformation[Click]))

  lazy val storedActionsState: ListState[Action] =
    getRuntimeContext.getListState(new ListStateDescriptor[Action]("actionState", createTypeInformation[Action]))

  override def processElement1(
      action: Action,
      context: CoProcessFunction[Action, Click, EnrichedAction]#Context,
      collector: Collector[EnrichedAction]): Unit = {

    throw new MissingSolutionException
  }

  override def processElement2(
      click: Click,
      context: CoProcessFunction[Action, Click, EnrichedAction]#Context,
      collector: Collector[EnrichedAction]): Unit = {

    throw new MissingSolutionException
  }

  override def onTimer(
      timestamp: Long,
      ctx: CoProcessFunction[Action, Click, EnrichedAction]#OnTimerContext,
      out: Collector[EnrichedAction]): Unit = {

    throw new MissingSolutionException
  }

  // ------------------------------------------------------------------------------------
  //   Utility methods
  // ------------------------------------------------------------------------------------

  def joinClickAndAction(click: Click, action: Action): EnrichedAction = {
    EnrichedAction(action.timestamp, click.timestamp, action.actionName, click.campaignID, click.bidID)
  }
}
