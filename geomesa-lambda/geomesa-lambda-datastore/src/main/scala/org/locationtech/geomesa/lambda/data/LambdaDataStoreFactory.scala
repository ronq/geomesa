/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import java.awt.RenderingHints.Key
import java.io.{Serializable, StringReader}
import java.time.Clock
import java.util.Properties

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore
import org.locationtech.geomesa.lambda.stream.{OffsetManager, ZookeeperOffsetManager}
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

class LambdaDataStoreFactory extends DataStoreFactorySpi {

  import LambdaDataStoreFactory.Params._
  import LambdaDataStoreFactory.parsePropertiesParam

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    val brokers = Kafka.BrokersParam.lookup(params)
    val expiry = ExpiryParam.lookup(params)

    val partitions = Kafka.PartitionsParam.lookup(params).intValue
    val consumers = Kafka.ConsumersParam.lookup(params).intValue
    val persist = PersistParam.lookup(params).booleanValue
    val defaultVisibility = VisibilitiesParam.lookupOpt(params)

    val consumerConfig = parsePropertiesParam(Kafka.ConsumerOptsParam.lookup(params)) ++ Map("bootstrap.servers" -> brokers)
    val producer = {
      val producerConfig = parsePropertiesParam(Kafka.ProducerOptsParam.lookup(params)) ++ Map("bootstrap.servers" -> brokers)
      KafkaStore.producer(producerConfig)
    }

    // TODO GEOMESA-1891 attribute level vis
    val persistence = new AccumuloDataStoreFactory().createDataStore(LambdaDataStoreFactory.filter(params))

    val zkNamespace = s"gm_lambda_${persistence.config.catalog}"

    val zk = Kafka.ZookeepersParam.lookup(params)

    val offsetManager = OffsetManagerParam.lookupOpt(params).getOrElse(new ZookeeperOffsetManager(zk, zkNamespace))

    val clock = ClockParam.lookupOpt(params).getOrElse(Clock.systemUTC())

    val config = LambdaConfig(zk, zkNamespace, partitions, consumers, expiry, defaultVisibility, persist)

    new LambdaDataStore(persistence, producer, consumerConfig, offsetManager, config)(clock)
  }

  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(LambdaDataStoreFactory.filter(params)) &&
        Seq(ExpiryParam, Kafka.BrokersParam, Kafka.ZookeepersParam).forall(_.exists(params))

  override def getParametersInfo: Array[Param] = Array(
    Accumulo.InstanceParam,
    Accumulo.ZookeepersParam,
    Accumulo.CatalogParam,
    Accumulo.UserParam,
    Accumulo.PasswordParam,
    Accumulo.KeytabParam,
    Kafka.BrokersParam,
    Kafka.ZookeepersParam,
    ExpiryParam,
    PersistParam,
    AuthsParam,
    ForceEmptyAuthsParam,
    QueryTimeoutParam,
    QueryThreadsParam,
    Accumulo.RecordThreadsParam,
    Accumulo.WriteThreadsParam,
    Kafka.PartitionsParam,
    Kafka.ConsumersParam,
    Kafka.ProducerOptsParam,
    Kafka.ConsumerOptsParam,
    VisibilitiesParam,
    LooseBBoxParam,
    GenerateStatsParam,
    AuditQueriesParam,
    NamespaceParam
  )

  override def getDisplayName: String = LambdaDataStoreFactory.DisplayName

  override def getDescription: String = LambdaDataStoreFactory.Description

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[Key, _] = java.util.Collections.emptyMap()
}

object LambdaDataStoreFactory {

  private val DisplayName = "Kafka/Accumulo Lambda (GeoMesa)"

  private val Description = "Hybrid store using Kafka for recent events and Accumulo for long-term storage"

  object Params extends GeoMesaDataStoreParams with SecurityParams {

    // noinspection TypeAnnotation
    object Accumulo {
      val InstanceParam      = copy(AccumuloDataStoreParams.InstanceIdParam)
      val ZookeepersParam    = copy(AccumuloDataStoreParams.ZookeepersParam)
      val UserParam          = copy(AccumuloDataStoreParams.UserParam)
      val PasswordParam      = copy(AccumuloDataStoreParams.PasswordParam)
      val KeytabParam        = copy(AccumuloDataStoreParams.KeytabPathParam)
      val RecordThreadsParam = copy(AccumuloDataStoreParams.RecordThreadsParam)
      val WriteThreadsParam  = copy(AccumuloDataStoreParams.WriteThreadsParam)
      val MockParam          = copy(AccumuloDataStoreParams.MockParam)
      val CatalogParam       = copy(AccumuloDataStoreParams.CatalogParam)
    }

    object Kafka {
      val BrokersParam      = new GeoMesaParam[String]("lambda.kafka.brokers", "Kafka brokers", required = true, deprecated = Seq("kafka.brokers"))
      val ZookeepersParam   = new GeoMesaParam[String]("lambda.kafka.zookeepers", "Kafka zookeepers", required = true, deprecated = Seq("kafka.zookeepers"))
      val PartitionsParam   = new GeoMesaParam[Integer]("lambda.kafka.partitions", "Number of partitions to use in kafka topics", default = Int.box(1), deprecated = Seq("kafka.partitions"))
      val ConsumersParam    = new GeoMesaParam[Integer]("lambda.kafka.consumers", "Number of kafka consumers used per feature type", default = Int.box(1), deprecated = Seq("kafka.consumers"))
      val ProducerOptsParam = new GeoMesaParam[String]("lambda.kafka.producer.options", "Kafka producer configuration options, in Java properties format", metadata = Map(Parameter.IS_LARGE_TEXT -> java.lang.Boolean.TRUE), deprecated = Seq("kafka.producer.options"))
      val ConsumerOptsParam = new GeoMesaParam[String]("lambda.kafka.consumer.options", "Kafka consumer configuration options, in Java properties format'", metadata = Map(Parameter.IS_LARGE_TEXT -> java.lang.Boolean.TRUE), deprecated = Seq("kafka.consumer.options"))
    }

    val ExpiryParam        = new GeoMesaParam[Duration]("lambda.expiry", "Duration before features expire from transient store. Use 'Inf' to prevent this store from participating in feature expiration", required = true, default = Duration("1h"), deprecated = Seq("expiry"))
    val PersistParam       = new GeoMesaParam[java.lang.Boolean]("lambda.persist", "Whether to persist expired features to long-term storage", default = java.lang.Boolean.TRUE, deprecated = Seq("persist"))

    // test params
    val ClockParam         = new GeoMesaParam[Clock]("lambda.clock", "Clock instance to use for timing", deprecated = Seq("clock"))
    val OffsetManagerParam = new GeoMesaParam[OffsetManager]("lamdab.offset-manager", "Offset manager instance to use", deprecated = Seq("offsetManager"))
  }

  private def copy[T <: AnyRef](p: GeoMesaParam[T])(implicit ct: ClassTag[T]): GeoMesaParam[T] = {
    import scala.collection.JavaConversions._
    new GeoMesaParam[T](s"lambda.${p.key}", p.description.toString, required = p.required, default = p.default, metadata = p.metadata.toMap, deprecated = p.deprecated)
  }

  private def filter(params: java.util.Map[String, Serializable]): java.util.Map[String, Serializable] = {
    // note: includes a bit of redirection to allow us to pass non-serializable values in to tests
    import scala.collection.JavaConverters._
    Map[String, Any](params.asScala.toSeq: _ *)
        .map { case (k, v) => (if (k.startsWith("lambda.")) { k.substring(7) } else { k }, v) }
        .asJava.asInstanceOf[java.util.Map[String, Serializable]]
  }

  def parsePropertiesParam(value: String): Map[String, String] = {
    import scala.collection.JavaConversions._
    if (value == null || value.isEmpty) { Map.empty } else {
      val props = new Properties()
      props.load(new StringReader(value))
      props.entrySet().map(e => e.getKey.asInstanceOf[String] -> e.getValue.asInstanceOf[String]).toMap
    }
  }
}
