/**
 * Copyright (c) Commonwealth Computer Research, Inc. 2006 - 2012
 * All Rights Reserved, www.ccri.com
 *
 * Developed under contracts for:
 * US Army / RDECOM / CERDEC / I2WD and
 * SBIR Contract for US Navy / Office of Naval Research
 *
 * This code may contain SBIR protected information.  Contact CCRi prior to
 * distribution.
 *
 * Begin SBIR Data Rights Statement
 * Contract No.:  N00014-08-C-0254
 * Contractor Name:  Commonwealth Computer Research, Inc
 * Contractor Address:  1422 Sachem Pl, Unit #1, Charlottesville, VA 22901
 * Expiration of SBIR Data Rights Period:  5/9/2017
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose technical data or computer software marked with this legend are
 * restricted during the period shown as provided in paragraph (b)(4) of the
 * Rights in Noncommercial Technical Data and Computer Software--Small Business
 * Innovative Research (SBIR) Program clause contained in the above identified
 * contract. No restrictions apply after the expiration date shown above. Any
 * reproduction of technical data, computer software, or portions thereof
 * marked with this legend must also reproduce the markings.
 * End SBIR Data Rights Statement
 *
 *
 * User: mronquest
 * Date: 11/4/14
 * Time: 9:23 AM
 **/


package org.locationtech.geomesa.core.index

import java.util.Map.Entry

import com.vividsolutions.jts.geom.{Polygon, Geometry}
import org.apache.accumulo.core.data.{Value, Key}
import org.geotools.data.{DataUtilities, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.joda.time.Interval
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data.FeatureEncoding._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.iterators.{DensityIterator, DeDuplicatingIterator}
import org.locationtech.geomesa.core.util.{SelfClosingIterator, CloseableIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.sort.{SortOrder, SortBy}

import scala.reflect.ClassTag

object RasterQueryPlanner {
  val iteratorPriority_RowRegex                        = 0
  val iteratorPriority_AttributeIndexFilteringIterator = 10
  val iteratorPriority_AttributeIndexIterator          = 200
  val iteratorPriority_AttributeUniqueIterator         = 300
  val iteratorPriority_ColFRegex                       = 100
  val iteratorPriority_SpatioTemporalIterator          = 200
  val iteratorPriority_SimpleFeatureFilteringIterator  = 300
  val iteratorPriority_AnalysisIterator                = 400
}

case class RasterQueryPlanner(schema: String,
                        featureType: SimpleFeatureType,
                        featureEncoding: FeatureEncoding) extends ExplainingLogging {
  // this is in QueryPlanner, and can be used without modification
  /**
  def buildFilter(geom: Geometry, interval: Interval): KeyPlanningFilter =
    (IndexSchema.somewhere(geom), IndexSchema.somewhen(interval)) match {
      case (None, None)       =>    AcceptEverythingFilter
      case (None, Some(i))    =>
        if (i.getStart == i.getEnd) DateFilter(i.getStart)
        else                        DateRangeFilter(i.getStart, i.getEnd)
      case (Some(p), None)    =>    SpatialFilter(p)
      case (Some(p), Some(i)) =>
        if (i.getStart == i.getEnd) SpatialDateFilter(p, i.getStart)
        else                        SpatialDateRangeFilter(p, i.getStart, i.getEnd)
    }
  def netPolygon(poly: Polygon): Polygon = poly match {
    case null => null
    case p if p.covers(IndexSchema.everywhere) =>
      IndexSchema.everywhere
    case p if IndexSchema.everywhere.covers(p) => p
    case _ => poly.intersection(IndexSchema.everywhere).
      asInstanceOf[Polygon]
  }

  def netGeom(geom: Geometry): Geometry =
    Option(geom).map(_.intersection(IndexSchema.everywhere)).orNull

  def netInterval(interval: Interval): Interval = interval match {
    case null => null
    case _    => IndexSchema.everywhen.overlap(interval)
  }
  **/

  // As a pre-processing step, we examine the query/filter and split it into multiple queries.
  // TODO: Work to make the queries non-overlapping.
  def getIterator(acc: AccumuloConnectorCreator,
                  //sft: SimpleFeatureType,
                  query: Query,
                  output: ExplainerOutputType = log): CloseableIterator[Entry[Key,Value]] = {

    output(s"Running ${ExplainerOutputType.toString(query)}")
    val ff = CommonFactoryFinder.getFilterFactory2
    val isDensity = query.getHints.containsKey(BBOX_KEY)
    val duplicatableData = IndexSchema.mayContainDuplicates(featureType)

    def flatten(queries: Seq[Query]): CloseableIterator[Entry[Key, Value]] =
      queries.toIterator.ciFlatMap(configureScanners(acc, sft, _, isDensity, output))

    // in some cases, where duplicates may appear in overlapping queries or the data itself, remove them
    def deduplicate(queries: Seq[Query]): CloseableIterator[Entry[Key, Value]] = {
      val flatQueries = flatten(queries)
      val decoder = SimpleFeatureDecoder(getReturnSFT(query), featureEncoding)
      new DeDuplicatingIterator(flatQueries, (key: Key, value: Value) => decoder.extractFeatureId(value))
    }

    if(isDensity) {
      val env = query.getHints.get(BBOX_KEY).asInstanceOf[ReferencedEnvelope]
      val q1 = new Query(featureType.getTypeName, ff.bbox(ff.property(featureType.getGeometryDescriptor.getLocalName), env))
      val mixedQuery = DataUtilities.mixQueries(q1, query, "geomesa.mixed.query")
      if (duplicatableData) {
        deduplicate(Seq(mixedQuery))
      } else {
        flatten(Seq(mixedQuery))
      }
    } else {
      val rawQueries = splitQueryOnOrs(query, output)
      if (rawQueries.length > 1 || duplicatableData) {
        deduplicate(rawQueries)
      } else {
        flatten(rawQueries)
      }
    }
  }



  /**
   * Helper method to execute a query against an AccumuloDataStore
   *
   * If the query contains ONLY an eligible LIKE
   * or EQUALTO query then satisfy the query with the attribute index
   * table...else use the spatio-temporal index table
   *
   * If the query is a density query use the spatio-temporal index table only
   */
  private def configureScanners(acc: AccumuloConnectorCreator,
                                sft: SimpleFeatureType,
                                derivedQuery: Query,
                                isDensity: Boolean,
                                output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    output(s"Transforms: ${derivedQuery.getHints.get(TRANSFORMS)}")
    val strategy = QueryStrategyDecider.chooseStrategy(acc.catalogTableFormat(sft), sft, derivedQuery)

    output(s"Strategy: ${strategy.getClass.getCanonicalName}")
    strategy.execute(acc, this, sft, derivedQuery, output)
  }

  def query(query: Query, acc: AccumuloConnectorCreator): CloseableIterator[SimpleFeature] = {
    // Perform the query
    val accumuloIterator = getIterator(acc, featureType, query)

    // Convert Accumulo results to SimpleFeatures
    adaptIterator(accumuloIterator, query)
  }

  // This function decodes/transforms that Iterator of Accumulo Key-Values into an Iterator of SimpleFeatures.
  def adaptIterator(accumuloIterator: CloseableIterator[Entry[Key,Value]], query: Query): CloseableIterator[SimpleFeature] = {
    // Perform a projecting decode of the simple feature
    val returnSFT = getReturnSFT(query)
    val decoder = SimpleFeatureDecoder(returnSFT, featureEncoding)

    // Decode according to the SFT return type.
    // if this is a density query, expand the map
    if (query.getHints.containsKey(DENSITY_KEY)) {
      accumuloIterator.flatMap { kv: Entry[Key, Value] =>
        DensityIterator.expandFeature(decoder.decode(kv.getValue))
      }
    } else {
      val features = accumuloIterator.map { kv => decoder.decode(kv.getValue) }
      if(query.getSortBy != null && query.getSortBy.length > 0) sort(features, query.getSortBy)
      else features
    }
  }
}

