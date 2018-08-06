package com.tzulitai.example2

import org.apache.flink.streaming.api.scala._

case class Action(timestamp: Long, actionName: String, bidID: String)
case class Click(timestamp: Long, campaignID: String, bidID: String)
case class EnrichedAction(actionTimestamp: Long, clickTimestamp: Long, actionName: String, campaignId: String, bidID: String)

/**
  * Utility to generate simulated data points.
  */
object Sources {

  def generateActions(env: StreamExecutionEnvironment): DataStream[Action] = {
    env.fromElements(
      Action(1533560770, "app_install", "bid-dee05241-0578-46bf-a8b3-7fa09ae0a398"),
      Action(1533560795, "app_purchase", "bid-8c453637-4eb8-42c0-a414-ebf68766ef1a"),
      Action(1533560827, "app_open", "bid-c995dbb3-8a7b-407b-986e-c1ec811f0865"),
      Action(1533560841, "app_install", "bid-8126eee2-725b-4582-b8ea-c4f75bb0c7bc"),
      Action(1533560841, "app_purchase", "bid-88934a7f-2993-4429-a4b2-0c6ce84c981d"),
      Action(1533560852, "app_open", "bid-26b5e674-8f60-40bc-ba22-bb7e744b3377"),
      Action(1533560874, "app_install", "bid-8c453637-4eb8-42c0-a414-ebf68766ef1a"),
      Action(1533560882, "app_purchase", "bid-80e60f79-a11c-476d-be21-53a6a64ba62d"),
      Action(1533560892, "app_purchase", "bid-ff04a181-6bb4-424c-a77b-7b78ed9df94d"),
      Action(1533560897, "app_open", "bid-ff04a181-6bb4-424c-a77b-7b78ed9df94d"),
      Action(1533560899, "app_purchase", "bid-1244eb1c-815e-4074-9e3e-f884e9cdba12"),
      Action(1533560919, "app_install", "bid-c590b317-3e33-4492-81ce-9552b32e3c97"),
      Action(1533560947, "app_purchase", "bid-b3bfd165-1c7e-404e-a327-3f5fbc76b707"),
      Action(1533560954, "app_open", "bid-88934a7f-2993-4429-a4b2-0c6ce84c981d"),
      Action(1533560979, "app_purchase", "bid-f8be9587-f16c-4a00-add1-3b3731369a94"),
      Action(1533560980, "app_install", "bid-844f0758-8b81-40b5-ad04-ec09be88554b"),
      Action(1533560980, "app_open", "bid-dee05241-0578-46bf-a8b3-7fa09ae0a398"),
      Action(1533561002, "app_install", "bid-024ae0a0-9350-494d-a25f-6ef25280d042"),
      Action(1533561007, "app_install", "bid-7ce88f69-ebf7-4011-863e-d4074a96d2bf"),
      Action(1533561009, "app_purchase", "bid-26b5e674-8f60-40bc-ba22-bb7e744b3377"),
      Action(1533561015, "app_open", "bid-8c453637-4eb8-42c0-a414-ebf68766ef1a"),
      Action(1533561028, "app_install", "bid-26b5e674-8f60-40bc-ba22-bb7e744b3377"),
      Action(1533561054, "app_open", "bid-c590b317-3e33-4492-81ce-9552b32e3c97"),
      Action(1533561061, "app_install", "bid-ad97af4f-e2ab-406d-ac00-3db64afb38d1"),
      Action(1533561062, "app_open", "bid-8126eee2-725b-4582-b8ea-c4f75bb0c7bc"),
      Action(1533561074, "app_purchase", "bid-c995dbb3-8a7b-407b-986e-c1ec811f0865"),
      Action(1533561075, "app_purchase", "bid-8126eee2-725b-4582-b8ea-c4f75bb0c7bc"),
      Action(1533561079, "app_install", "bid-778fd9c9-11e5-491e-9ce6-390a70926aeb"),
      Action(1533561100, "app_open", "bid-b3bfd165-1c7e-404e-a327-3f5fbc76b707"),
      Action(1533561109, "app_install", "bid-b3bfd165-1c7e-404e-a327-3f5fbc76b707"),
      Action(1533561116, "app_purchase", "bid-ad97af4f-e2ab-406d-ac00-3db64afb38d1"),
      Action(1533561167, "app_install", "bid-5603042f-4614-41d2-a819-69fdf5fedcf3"),
      Action(1533561180, "app_purchase", "bid-5603042f-4614-41d2-a819-69fdf5fedcf3"),
      Action(1533561186, "app_install", "bid-5647d514-f4d3-4f29-81a8-4e045cb59e9c"),
      Action(1533561192, "app_open", "bid-80e60f79-a11c-476d-be21-53a6a64ba62d"),
      Action(1533561218, "app_purchase", "bid-024ae0a0-9350-494d-a25f-6ef25280d042"),
      Action(1533561249, "app_open", "bid-f8be9587-f16c-4a00-add1-3b3731369a94"),
      Action(1533561252, "app_open", "bid-7ce88f69-ebf7-4011-863e-d4074a96d2bf"),
      Action(1533561274, "app_purchase", "bid-dee05241-0578-46bf-a8b3-7fa09ae0a398"),
      Action(1533561289, "app_purchase", "bid-778fd9c9-11e5-491e-9ce6-390a70926aeb"),
      Action(1533561308, "app_purchase", "bid-c590b317-3e33-4492-81ce-9552b32e3c97"),
      Action(1533561342, "app_open", "bid-024ae0a0-9350-494d-a25f-6ef25280d042"),
      Action(1533561356, "app_install", "bid-f8be9587-f16c-4a00-add1-3b3731369a94"),
      Action(1533561397, "app_install", "bid-ff04a181-6bb4-424c-a77b-7b78ed9df94d"),
      Action(1533561403, "app_open", "bid-5603042f-4614-41d2-a819-69fdf5fedcf3"),
      Action(1533561412, "app_open", "bid-778fd9c9-11e5-491e-9ce6-390a70926aeb"),
      Action(1533561415, "app_purchase", "bid-b153fc41-662c-4560-a690-3ae44757adf8"),
      Action(1533561421, "app_open", "bid-5647d514-f4d3-4f29-81a8-4e045cb59e9c"),
      Action(1533561448, "app_open", "bid-ad97af4f-e2ab-406d-ac00-3db64afb38d1"),
      Action(1533561467, "app_install", "bid-1244eb1c-815e-4074-9e3e-f884e9cdba12"),
      Action(1533561467, "app_install", "bid-c995dbb3-8a7b-407b-986e-c1ec811f0865"),
      Action(1533561506, "app_purchase", "bid-844f0758-8b81-40b5-ad04-ec09be88554b"),
      Action(1533561550, "app_open", "bid-b153fc41-662c-4560-a690-3ae44757adf8"),
      Action(1533561562, "app_install", "bid-b153fc41-662c-4560-a690-3ae44757adf8"),
      Action(1533561569, "app_install", "bid-80e60f79-a11c-476d-be21-53a6a64ba62d"),
      Action(1533561592, "app_purchase", "bid-5647d514-f4d3-4f29-81a8-4e045cb59e9c"),
      Action(1533561613, "app_open", "bid-1244eb1c-815e-4074-9e3e-f884e9cdba12"),
      Action(1533561625, "app_install", "bid-88934a7f-2993-4429-a4b2-0c6ce84c981d"),
      Action(1533561641, "app_purchase", "bid-7ce88f69-ebf7-4011-863e-d4074a96d2bf"),
      Action(1533561743, "app_open", "bid-844f0758-8b81-40b5-ad04-ec09be88554b")
    )
  }

  def generateClicks(env: StreamExecutionEnvironment): DataStream[Click] = {
    env.fromElements(
      Click(1533560769, "cmp-2439928a-ea22-405e-80a4-77fa445150c6", "bid-dee05241-0578-46bf-a8b3-7fa09ae0a398"),
      Click(1533560770, "cmp-2439928a-ea22-405e-80a4-77fa445150c6", "bid-024ae0a0-9350-494d-a25f-6ef25280d042"),
      Click(1533560771, "cmp-3bb05b5e-adda-46c0-b335-85becd86d7b3", "bid-80e60f79-a11c-476d-be21-53a6a64ba62d"),
      Click(1533560778, "cmp-7b44561b-c918-45e4-9c17-28f70cccea43", "bid-f8be9587-f16c-4a00-add1-3b3731369a94"),
      Click(1533560792, "cmp-3e329ef2-0b4b-4efd-8f94-4f3cba5e3792", "bid-b153fc41-662c-4560-a690-3ae44757adf8"),
      Click(1533560797, "cmp-42e8772b-362d-4c9a-b546-5dd4945c9001", "bid-5603042f-4614-41d2-a819-69fdf5fedcf3"),
      Click(1533560799, "cmp-430ed074-c348-4c6d-8e17-fd833add6f4e", "bid-ff04a181-6bb4-424c-a77b-7b78ed9df94d"),
      Click(1533560800, "cmp-7b44561b-c918-45e4-9c17-28f70cccea43", "bid-26b5e674-8f60-40bc-ba22-bb7e744b3377"),
      Click(1533560805, "cmp-7b44561b-c918-45e4-9c17-28f70cccea43", "bid-8c453637-4eb8-42c0-a414-ebf68766ef1a"),
      Click(1533560811, "cmp-42e8772b-362d-4c9a-b546-5dd4945c9001", "bid-8126eee2-725b-4582-b8ea-c4f75bb0c7bc"),
      Click(1533560821, "cmp-3e329ef2-0b4b-4efd-8f94-4f3cba5e3792", "bid-ad97af4f-e2ab-406d-ac00-3db64afb38d1"),
      Click(1533560825, "cmp-2439928a-ea22-405e-80a4-77fa445150c6", "bid-88934a7f-2993-4429-a4b2-0c6ce84c981d"),
      Click(1533560827, "cmp-db64dd80-59e1-47aa-be3a-c778d0f8b7d2", "bid-1244eb1c-815e-4074-9e3e-f884e9cdba12"),
      Click(1533560832, "cmp-87857ab1-b879-4d29-b171-909f429aa3ce", "bid-844f0758-8b81-40b5-ad04-ec09be88554b"),
      Click(1533560833, "cmp-87857ab1-b879-4d29-b171-909f429aa3ce", "bid-c590b317-3e33-4492-81ce-9552b32e3c97"),
      Click(1533560840, "cmp-87857ab1-b879-4d29-b171-909f429aa3ce", "bid-778fd9c9-11e5-491e-9ce6-390a70926aeb"),
      Click(1533560848, "cmp-3bb05b5e-adda-46c0-b335-85becd86d7b3", "bid-c995dbb3-8a7b-407b-986e-c1ec811f0865"),
      Click(1533560852, "cmp-2439928a-ea22-405e-80a4-77fa445150c6", "bid-5647d514-f4d3-4f29-81a8-4e045cb59e9c"),
      Click(1533560854, "cmp-7b44561b-c918-45e4-9c17-28f70cccea43", "bid-7ce88f69-ebf7-4011-863e-d4074a96d2bf"),
      Click(1533560854, "cmp-db64dd80-59e1-47aa-be3a-c778d0f8b7d2", "bid-b3bfd165-1c7e-404e-a327-3f5fbc76b707")
    )
  }

}
