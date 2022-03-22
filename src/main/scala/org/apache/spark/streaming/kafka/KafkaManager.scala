package org.apache.spark.streaming.kafka

import java.lang.{Long => JLong}
import java.util.{Map => JMap, Set => JSet}
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters.mapAsScalaMapConverter

class KafkaManager(val kafkaParams:Map[String,String],
                   val ignoreZKOffsets:Boolean=false) extends Logging {
  //@trasient 开始是红色的，不要怕，往下敲就可以
  @transient private val kc = new KafkaCluster(kafkaParams)
  @transient private val groupId = kafkaParams.get("group.id")

}

object KafkaManager {
  def apply(jKafkaParams:JMap[String,String],ignoreZKOffsets: Boolean = false):KafkaManager = {
    new KafkaManager(jKafkaParams.asScala.toMap,ignoreZKOffsets)
  }
}
