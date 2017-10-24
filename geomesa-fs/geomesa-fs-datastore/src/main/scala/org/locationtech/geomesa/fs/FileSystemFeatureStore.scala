/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, Query}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemWriter}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class FileSystemFeatureStore(entry: ContentEntry,
                             query: Query,
                             fs: FileSystem,
                             storage: FileSystemStorage,
                             readThreads: Int) extends ContentFeatureStore(entry, query) with LazyLogging {
  private val _sft = storage.getFeatureType(entry.getTypeName)

  override def getWriterInternal(query: Query, flags: Int): FileSystemFeatureWriter = {
    require(flags != 0, "no write flags set")
    require((flags | WRITER_ADD) == WRITER_ADD, "Only append supported")

    val thePartitionScheme = storage.getPartitionScheme(entry.getTypeName)
    new FileSystemFeatureWriter(_sft, thePartitionScheme ) {
        override def newWriter(typeName: String, partition: String): FileSystemWriter =  storage.getWriter(typeName, partition)
    }
  }

  override def getBoundsInternal(query: Query): ReferencedEnvelope = ReferencedEnvelope.EVERYTHING
  override def buildFeatureType(): SimpleFeatureType = _sft
  override def getCountInternal(query: Query): Int = -1

  override def getReaderInternal(original: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val query = new Query(original)
    // The type name can sometimes be empty such as Query.ALL
    query.setTypeName(_sft.getTypeName)

    // Set Transforms if present
    import org.locationtech.geomesa.index.conf.QueryHints._
    QueryPlanner.setQueryTransforms(query, _sft)
    val transformSft = query.getHints.getTransformSchema.getOrElse(_sft)

    val scheme = storage.getPartitionScheme(_sft.getTypeName)
    val iter = new FileSystemFeatureIterator(fs, scheme, _sft, query, readThreads, storage)
    new DelegateSimpleFeatureReader(transformSft, new DelegateSimpleFeatureIterator(iter))
  }


  override def canLimit: Boolean = false
  override def canTransact: Boolean = false
  override def canEvent: Boolean = false
  override def canReproject: Boolean = false
  override def canRetype: Boolean = true
  override def canSort: Boolean = true
  override def canFilter: Boolean = true

}
