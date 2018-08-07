package com.tzulitai.example1

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala._

class DataPointSerializationSchema extends SerializationSchema[DataPoint] with DeserializationSchema[DataPoint] {

  override def serialize(element: DataPoint): Array[Byte] = {
    (element.key + "," + element.timestamp.toString + "," + element.value.toString).getBytes
  }

  override def deserialize(message: Array[Byte]): DataPoint = {
    val split = new String(message).split(",")
    DataPoint(split(0), split(1).toLong, split(2).toDouble)
  }

  override def isEndOfStream(nextElement: DataPoint) = false

  override def getProducedType: TypeInformation[DataPoint] = createTypeInformation[DataPoint]
}
