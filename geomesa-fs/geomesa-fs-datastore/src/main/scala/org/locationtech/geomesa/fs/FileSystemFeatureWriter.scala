/************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

import java.util.concurrent.atomic.AtomicLong

import org.geotools.data.FeatureWriter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.api.{FileSystemWriter, PartitionScheme}
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}


abstract class FileSystemFeatureWriter(private val sft: SimpleFeatureType, partitionScheme: PartitionScheme) extends FeatureWriter[SimpleFeatureType, SimpleFeature] {

  def newWriter(typeName: String, partition: String): FileSystemWriter

  private val typeName = sft.getTypeName

  private val writers = scala.collection.mutable.Map.empty[String, FileSystemWriter]


  private val featureIds = new AtomicLong(0)
  private var feature: SimpleFeature = _

  override def getFeatureType: SimpleFeatureType = sft
  override def hasNext: Boolean = false

  override def next(): SimpleFeature = {
    feature = new ScalaSimpleFeature(sft, featureIds.getAndIncrement().toString)
    feature
  }

  override def write(): Unit = {
    val partition = partitionScheme.getPartitionName(feature)
    val writer = writers.getOrElseUpdate(partition, newWriter(typeName, partition))
    writer.write(feature)
    feature = null
  }

  // Additional method to ensure there is only a single FileSystemWriter at a time
  // This ought to be used for collections sorted by partition
  // TODO: Consider moving the partition into the argument rather than extracting from the feature
  def serialPartitionWrite(): Unit = {
    val partition = partitionScheme.getPartitionName(feature)
    val singleWriter =  getSinglePartitionWriter(partition)
    singleWriter.write(feature)
    feature = null
  }

  // Clear the writer Map of all entries except for the given partition, creating it if needed
  def getSinglePartitionWriter(partition: String): FileSystemWriter = {
    val unwantedWriters = writers.filterNot{ case (k: String, _) => k != partition }
    unwantedWriters.foreach { case (_, fsw: FileSystemWriter) => closeWriter(fsw) }
    val unwantedKeys = unwantedWriters.keysIterator
    writers --= unwantedKeys
    writers.getOrElseUpdate(partition, newWriter(typeName, partition))
  }

  override def remove(): Unit = throw new NotImplementedError()

  def closeWriter(writer: FileSystemWriter): Unit = {
    writer.flush()
    CloseWithLogging(writer)
  }

  // TODO: Think hard about whether the metadata should be updated within this method
  override def close(): Unit = writers.foreach { case (_, writer) => closeWriter(writer) }

}
