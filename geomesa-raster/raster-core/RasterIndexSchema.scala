/*
* Copyright 2014-2014 Commonwealth Computer Research, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/



package org.locationtech.geomesa.core.index


import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, Point, Polygon}
import org.apache.accumulo.core.data.Key
import org.geotools.data.Query
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.util._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.annotation.tailrec
import scala.util.parsing.combinator.RegexParsers

// A secondary index consists of interleaved elements of a composite key stored in
// Accumulo's key (row, column family, and column qualifier)
//
// A spatio-temporal index utilizes the location and the time of an entry to construct
// the secondary index.
//
// An index schema consists of the mapping of the composite key (time,space) to the three
// elements of a Accumulo key.  The mapping is specified using the printf-like format string.
// The format string consists of an entry for each of the row, column family, and column
// qualifier.  The entry consists of formatting directives of the composite key.  A directive
// has the following form:
//
// %[formatting options]#[formatting code]
//
// The following format codes are available
// s    => the separator character
// r    => a random partitioner - creates shards on [0, option], that is
//         (option + 1) separate partitions
// gh   => geohash formatter - options are the start character and number of characters
// d    => date formatter - options are any properly formed date format string
// cstr => constant string formatter
//
// An entry consists of a sequence of formatting directives with and must start with the
// separator directive.  For example, the following entry format:
//
// %~#s%999#r%0,4#gh%HHmm#d
//
// specifies that the separator character is a '~', then a random partition element between 000
// and 999, then the first four characters of the geohash, then the hours and minutes of the time
// of the entry.  The resulting Accumulo key element might look like "342~tmw1~1455"
//
// A full schema consists of 3 entry formatting directives separated by '::'.  The following is
// an example of a fully specified index schema:
//
// %~#s%999#r%0,4#gh%HHmm#d::%~#s%4,2#gh::%~#s%6,1#gh%yyyyMMdd#d

case class RasterIndexSchema(//encoder: CQEntryEncoder,
                       //decoder: CQEntryDecoder,
                       planner: RasterQueryPlanner,
                       coverageName: String) extends ExplainingLogging {
  // this is where Andrew's work gets plugged in
  //def encode(entry: SimpleFeature, visibility: String = "") = encoder.encode(entry, visibility)
  //def decode(key: Key): SimpleFeature = decoder.decode(key)
}

//TODO: leverage the existing IndexSchema object to build the RasterIndexSchema
// could either move most methods into a IndexSchemaMethods trait and extend that in the IndexSchema and RasterIndexSchema,
// or just define an extra apply with a different signature that returns a RasterIndexSchema
object RasterIndexSchema {

  //we need an apply method
  // builds a RasterIndexSchema
  // most of these arguments are not correct
  def apply(s: String,
            featureType: SimpleFeatureType,
            featureEncoder: SimpleFeatureEncoder): IndexSchema = {
    val keyEncoder        = buildKeyEncoder(s, featureEncoder)
    val geohashDecoder    = buildGeohashDecoder(s)
    val dateDecoder       = buildDateDecoder(s)
    val keyPlanner        = buildKeyPlanner(s)
    val cfPlanner         = buildColumnFamilyPlanner(s)
    val indexEntryDecoder = IndexEntryDecoder(geohashDecoder, dateDecoder)
    val queryPlanner      = QueryPlanner(s, featureType, featureEncoder.encoding)
    IndexSchema(keyEncoder, indexEntryDecoder, queryPlanner, featureType)
  }
  // we may need something like this as well.

  def getIndexEntryDecoder(s: String) = {
    val geohashDecoder    = buildGeohashDecoder(s)
    val dateDecoder       = buildDateDecoder(s)
    IndexEntryDecoder(geohashDecoder, dateDecoder)
  }
}



