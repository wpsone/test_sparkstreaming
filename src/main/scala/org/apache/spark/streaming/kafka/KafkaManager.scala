package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.hadoop.hbase.codec.KeyValueCodec.KeyValueDecoder
import org.apache.spark.SparkException
import org.apache.spark.api.java.function.{Function => JFunction}

import java.lang.{Long => JLong}
import java.util.{Map => JMap, Set => JSet}
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaInputDStream, JavaPairDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

import java.util
import scala.collection.JavaConverters.{asScalaSetConverter, mapAsScalaMapConverter}
import scala.reflect.ClassTag

class KafkaManager(val kafkaParams:Map[String,String],
                   val ignoreZKOffsets:Boolean=false) extends Logging {
  //@trasient 开始是红色的，不要怕，往下敲就可以
  @transient private val kc = new KafkaCluster(kafkaParams)
  @transient private val groupId = kafkaParams.get("group.id")

  def createDirectStream[K:ClassTag,V:ClassTag,KD<:Decoder[K]:ClassTag,VD<:Decoder[V]:ClassTag,R:ClassTag](ssc:StreamingContext,
                                                                                                           kafkaParams:Map[String,String],
                                                                                                           topics:Set[String],
                                                                                                           messageHandler: MessageAndMetadata[K,V]=>R):InputDStream[R] = {
    val cleanedHandler = ssc.sc.clean(messageHandler)
    val fromOffsets: Map[TopicAndPartition, Long] = getStartOffsets(kc, kafkaParams, topics)
    fromOffsets.foreach {
      case (tp,offset) =>
        logInfo(s"SimpleConsumer init offset info: $tp offset: $offset")
    }
    new DirectKafkaInputDStream[K,V,KD,VD,R](
      ssc,kafkaParams,fromOffsets,cleanedHandler
    )
  }

  def createDirectStream[K:ClassTag,V:ClassTag,KD<:Decoder[K]:ClassTag,VD<:Decoder[V]:ClassTag,R:ClassTag](
                                                                                                          ssc:StreamingContext,
                                                                                                          kafkaParams:Map[String,String],
                                                                                                          fromOffsets:Map[TopicAndPartition,Long],
                                                                                                          messgeHandler:MessageAndMetadata[K,V]=>R
                                                                                                          ):InputDStream[R] = {
    val cleanedHandler: MessageAndMetadata[K, V] => R = ssc.sc.clean(messgeHandler)
    new DirectKafkaInputDStream[K,V,KD,VD,R ](ssc,kafkaParams,fromOffsets,cleanedHandler)
  }

  def createDirectStream[K:ClassTag,V:ClassTag,KD<:Decoder[K]:ClassTag,VD<:Decoder[V]:ClassTag](
                                                                                               ssc:StreamingContext,
                                                                                               kafkaParams:Map[String,String],
                                                                                               topics:Set[String]
                                                                                               ):InputDStream[(K,V)] = {
    val messageHandler = (mmd:MessageAndMetadata[K,V])=>(mmd.key,mmd.message)
    val fromOffsets: Map[TopicAndPartition, Long] = getStartOffsets(kc, kafkaParams, topics)
    fromOffsets.foreach{
      case (tp,offset) =>
        logInfo(s"SimpleConsumer init offset info: $tp offset: $offset")
    }
    new DirectKafkaInputDStream[K,V,KD,VD,(K,V) ](ssc,kafkaParams,fromOffsets,messageHandler)
  }

  def createJavaDirectStream[K,V,KD<:Decoder[K],VD<:Decoder[V],R](
                                                                 jssc:JavaStreamingContext,
                                                                 keyClass:Class[K],
                                                                 valueClass:Class[V],
                                                                 keyDecoderClass:Class[KD],
                                                                 valueDecoderClass:Class[VD],
                                                                 recordClass:Class[R],
                                                                 kafkaParams:JMap[String,String],
                                                                 fromOffsets:JMap[TopicAndPartition,JLong],
                                                                 messageHandler: JFunction[MessageAndMetadata[K,V],R]
                                                                 ):JavaInputDStream[R]={
    implicit val keyCmt:ClassTag[K] = ClassTag(keyClass)
    implicit val valueCmt:ClassTag[V] = ClassTag(valueClass)
    implicit val keyDecoderCmt:ClassTag[KD] = ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt:ClassTag[VD] = ClassTag(valueClass)
    implicit val recordCmt:ClassTag[R] = ClassTag(recordClass)
    val cleanedHandler: MessageAndMetadata[K, V] => R = jssc.sparkContext.clean(messageHandler.call _)
    createDirectStream[K,V,KD,VD,R](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq:_*),
      Map(fromOffsets.asScala.mapValues(_.longValue() ).toSeq:_*),
      cleanedHandler
    )
  }

  def createJavaDirectStream[K,V,KD<:Decoder[K],VD<:Decoder[V]](
                                                               jssc:JavaStreamingContext,
                                                               keyClass:Class[K],
                                                               valueClass:Class[V],
                                                               keyDecoderClass:Class[KD],
                                                               valueDecoderClass:Class[VD],
                                                               kafkaParams:JMap[String,String],
                                                               topics:JSet[String]
                                                               ):JavaPairDStream[K,V] = {
    implicit val keyCmt:ClassTag[K]=ClassTag(keyClass)
    implicit val valueCmt:ClassTag[V]=ClassTag(valueClass)
    implicit val keyDecoderCmt:ClassTag[KD]=ClassTag(keyDecoderClass)
    implicit val valueDecoderCmt:ClassTag[VD]=ClassTag(valueDecoderClass)
    createDirectStream[K,V,KD,VD](
      jssc.ssc,
      Map(kafkaParams.asScala.toSeq:_*),
      Set(topics.asScala.toSeq:_*)
    )
  }

  private def getStartOffsets(kc:KafkaCluster,
                              kafkaParams:Map[String,String],
                              topics:Set[String]):Map[TopicAndPartition,Long] = {
    topics.flatMap {
      topic =>
        if (groupId.isEmpty || ignoreZKOffsets)
          KafkaUtils.getFromOffsets(kc,kafkaParams,Set(topic))
        else {
          val partitions: Set[TopicAndPartition] = kc.getPartitions(Set(topic)).fold(
            errs => throw new SparkException(errs.mkString("\n")),
            ok => ok
          )
          kc.getConsumerOffsets(groupId.get,partitions  ).fold(
            errs => KafkaUtils.getFromOffsets(kc,kafkaParams,Set(topic) ),
            ok => checkConsumerOffsets(partitions,ok)
          )
        }
    }.toMap
  }

  private def checkConsumerOffsets(partitions:Set[TopicAndPartition],
                                   consumerOffsets:Map[TopicAndPartition,Long]) = {
    val low: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = kc.getEarliestLeaderOffsets(partitions).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
    val high: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = kc.getLatestLeaderOffsets(partitions).fold(
      errs => throw new SparkException(errs.mkString("\n")),
      ok => ok
    )
    consumerOffsets.map{
      case (tp,offset) =>
        val lowOffset: Long = low(tp).offset
        val highOffset: Long = high(tp).offset
        (tp,if (lowOffset<=offset && offset<=highOffset) offset
        else if (lowOffset>offset) {
          logWarning(s"Topic $tp consumer offset: $offset is less than kafka smallest offset: $lowOffset, use kafka smallest offset")
          lowOffset
        } else {
          logWarning(s"Topic $tp consumer offset: $offset is bigger than kafka largest offset: $highOffset, use kafka largest offset")
          highOffset
        })
    }
  }

  def commitOffsetsToZK(offsetRanges:Array[OffsetRange]):Unit = {
    require(groupId.isDefined,"Commit offsets to zookeeper but group.id is empty")
    val offsets: Map[TopicAndPartition, Long] = offsetRanges.map(
      offset => (offset.topicAndPartition, offset.untilOffset)
    ).toMap
    kc.setConsumerOffsets(groupId.get,offsets ).fold(
      errs=>logError(errs.mkString("\n" )),
      ok=>logDebug(
        s"""Success commit offset to zookeeper cluster: ${offsetRanges.mkString(",")}"""
      )
    )
  }
}

object KafkaManager {
  def apply(jKafkaParams:JMap[String,String],ignoreZKOffsets: Boolean = false):KafkaManager = {
    new KafkaManager(jKafkaParams.asScala.toMap,ignoreZKOffsets)
  }
}
