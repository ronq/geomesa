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

import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, GeometryCollection, Polygon}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.accumulo.core.iterators.user.RegExFilter
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.Interval
import org.joda.time.Interval
import org.locationtech.geomesa.core.GEOMESA_ITERATORS_IS_DENSITY_TYPE
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.data.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.core.data.FeatureEncoding._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators.IndexIterator
import org.locationtech.geomesa.core.iterators.IndexOnlyIterator
import org.locationtech.geomesa.core.iterators.IteratorConfig
import org.locationtech.geomesa.core.iterators.IteratorTrigger
import org.locationtech.geomesa.core.iterators.SpatioTemporalIntersectingIterator
import org.locationtech.geomesa.core.iterators.SpatioTemporalIterator
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.util.SelfClosingBatchScanner
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.core.util.{SelfClosingBatchScanner, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression
import org.opengis.filter.expression.Literal
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.expression.{Expression, Literal, PropertyName}
import org.opengis.filter.spatial.BBOX
import org.opengis.filter.spatial.BinarySpatialOperator
import org.opengis.filter.spatial.{BBOX, BinarySpatialOperator}
import org.opengis.parameter.GeneralParameterValue

class RasterIdxStrategy extends Strategy with Logging {

  def execute(acc: AccumuloConnectorCreator,
              iqp: QueryPlanner,
              featureType: SimpleFeatureType,
              queryParameters: Array[GeneralParameterValue],
              output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    // this needs to be implemented in the CoverageStore
    // it creates the connection to Accumulo and perhaps the correct table
    val bs = acc.createRasterIdxScanner(featureType)

    val qp = buildRasterIdxQueryPlan(queryParameters, iqp, featureType, output)
    configureBatchScanner(bs, qp)
    // NB: Since we are (potentially) gluing multiple batch scanner iterators together,
    //  we wrap our calls in a SelfClosingBatchScanner.
    SelfClosingBatchScanner(bs)
  }

  def buildRasterIdxQueryPlan(queryParameters: Array[GeneralParameterValue],
                          iqp: QueryPlanner,
                          featureType: SimpleFeatureType,
                          output: ExplainerOutputType) = {
    val schema          = iqp.schema
    val featureEncoding = iqp.featureEncoding
    val keyPlanner      = IndexSchema.buildKeyPlanner(iqp.schema)
    val cfPlanner       = IndexSchema.buildColumnFamilyPlanner(iqp.schema)

    output(s"Scanning ST index table for feature type ${featureType.getTypeName}")
    output(s"Filter: ${query.getFilter}")

    val parsedRequest = RasterRequest(queryParameters)

    // TODO: Select only the geometry filters which involve the indexed geometry type.
    // https://geomesa.atlassian.net/browse/GEOMESA-200
    // Simiarly, we should only extract temporal filters for the index date field.
    //val (geomFilters, otherFilters) = partitionGeom(query.getFilter)
    //val (temporalFilters, ecqlFilters) = partitionTemporal(otherFilters, getDtgFieldName(featureType))

    //val ecql = filterListAsAnd(ecqlFilters).map(ECQL.toCQL)

    //output(s"Geometry filters: $geomFilters")
    //output(s"Temporal filters: $temporalFilters")
    //output(s"Other filters: $ecqlFilters")

    //val tweakedGeoms = geomFilters.map(updateTopologicalFilters(_, featureType))

    //output(s"Tweaked geom filters are $tweakedGeoms")

    // standardize the two key query arguments:  polygon and date-range
    /**
    val geomsToCover = tweakedGeoms.flatMap {
      case bbox: BBOX =>
        val bboxPoly = bbox.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])
        Seq(bboxPoly)
      case gf: BinarySpatialOperator =>
        extractGeometry(gf)
      case _ => Seq()
    }
    **/
    /**
    val collectionToCover: Geometry = geomsToCover match {
      case Nil => null
      case seq: Seq[Geometry] => new GeometryCollection(geomsToCover.toArray, geomsToCover.head.getFactory)
    }
    **/
    //val temporal = extractTemporal(temporalFilters)
    //val interval = netInterval(temporal)
    //val geometryToCover = netGeom(collectionToCover)
    //val filter = buildFilter(geometryToCover, interval)

    //output(s"GeomsToCover: $geomsToCover")

    //val ofilter = filterListAsAnd(tweakedGeoms ++ temporalFilters)
    //if (ofilter.isEmpty) logger.warn(s"Querying Accumulo without ST filter.")

    //val oint  = IndexSchema.somewhen(interval)

    // set up row ranges and regular expression filter
    val qp = planQuery(filter, output, keyPlanner, cfPlanner)

    //output(s"STII Filter: ${ofilter.getOrElse("No STII Filter")}")
    //output(s"Interval:  ${oint.getOrElse("No interval")}")
    //output(s"Filter: ${Option(filter).getOrElse("No Filter")}")

    //val iteratorConfig = IteratorTrigger.chooseIterator(ecql, query, featureType)

    //val stiiIterCfg = getSTIIIterCfg(iteratorConfig, query, featureType, ofilter, schema, featureEncoding)

    //val sffiIterCfg = getSFFIIterCfg(iteratorConfig, featureType, ecql, schema, featureEncoding, query)

    //val topIterCfg = getTopIterCfg(query, geometryToCover, schema, featureEncoding, featureType)

    qp.copy(iterators = qp.iterators ++ List(Some(stiiIterCfg), sffiIterCfg, topIterCfg).flatten)
  }
  /**
  def getSTIIterCfg(iteratorConfig: IteratorConfig,
                     query: Query,
                     featureType: SimpleFeatureType,
                     ofilter: Option[Filter],
                     schema: String,
                     featureEncoding: FeatureEncoding): IteratorSetting = {
    iteratorConfig.iterator match {
      case IndexOnlyIterator =>
        configureIndexIterator(ofilter, query, schema, featureEncoding, featureType)
      case SpatioTemporalIterator =>
        val isDensity = query.getHints.containsKey(DENSITY_KEY)
        configureSpatioTemporalIntersectingIterator(ofilter, featureType, schema, isDensity)
    }
  }
  **/

  // establishes the regular expression that defines (minimally) acceptable rows
  def configureRowRegexIterator(regex: String): IteratorSetting = {
    val name = "regexRow-" + randomPrintableString(5)
    val cfg = new IteratorSetting(iteratorPriority_RowRegex, name, classOf[RegExFilter])
    RegExFilter.setRegexs(cfg, regex, null, null, null, false)
    cfg
  }
  /**
  // returns an iterator over [key,value] pairs where the key is taken from the index row and the value is a SimpleFeature,
  // which is either read directory from the data row  value or generated from the encoded index row value
  // -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureIndexIterator(filter: Option[Filter],
                             query: Query,
                             schema: String,
                             featureEncoding: FeatureEncoding,
                             featureType: SimpleFeatureType): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),classOf[IndexIterator])
    IndexIterator.setOptions(cfg, schema, filter)
    // the transform will have already been set in the query hints
    val testType = query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
    configureFeatureType(cfg, testType)
    configureFeatureEncoding(cfg, featureEncoding)
    cfg
  }

  // returns only the data entries -- no index entries -- for items that either:
  // 1) the GeoHash-box intersects the query polygon; this is a coarse-grained filter
  // 2) the DateTime intersects the query interval; this is a coarse-grained filter
  def configureSpatioTemporalIntersectingIterator(filter: Option[Filter],
                                                  featureType: SimpleFeatureType,
                                                  schema: String,
                                                  isDensity: Boolean): IteratorSetting = {
    val cfg = new IteratorSetting(iteratorPriority_SpatioTemporalIterator,
      "within-" + randomPrintableString(5),
      classOf[SpatioTemporalIntersectingIterator])
    SpatioTemporalIntersectingIterator.setOptions(cfg, schema, filter)
    configureFeatureType(cfg, featureType)
    if (isDensity) cfg.addOption(GEOMESA_ITERATORS_IS_DENSITY_TYPE, "isDensity")
    cfg
  }
  **/
  //TODO : these should come from a trait shared with QueryPlanner and RasterQueryPlanner
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

  def planQuery(filter: KeyPlanningFilter, output: ExplainerOutputType, keyPlanner: KeyPlanner, cfPlanner: ColumnFamilyPlanner): QueryPlan = {
    output(s"Planning query")
    val keyPlan = keyPlanner.getKeyPlan(filter, output)

    val columnFamilies = cfPlanner.getColumnFamiliesToFetch(filter)

    // always try to use range(s) to remove easy false-positives
    val accRanges: Seq[org.apache.accumulo.core.data.Range] = keyPlan match {
      case KeyRanges(ranges) => ranges.map(r => new org.apache.accumulo.core.data.Range(r.start, r.end))
      case _ => Seq(new org.apache.accumulo.core.data.Range())
    }

    output(s"Total ranges: ${accRanges.size}")

    // always try to set a RowID regular expression
    //@TODO this is broken/disabled as a result of the KeyTier
    val iters =
      keyPlan.toRegex match {
        case KeyRegex(regex) => Seq(configureRowRegexIterator(regex))
        case _               => Seq()
      }

    // if you have a list of distinct column-family entries, fetch them
    val cf = columnFamilies match {
      case KeyList(keys) =>
        output(s"ColumnFamily Planner: ${keys.size} : ${keys.take(20)}")
        keys.map { cf => new Text(cf) }

      case _ =>
        Seq()
    }
    // do CQ stuff here

    RasterQueryPlan(iters, accRanges, cf, cq)
  }
}


// nothing in this object may be needed.
object RasterIdxStrategy {

  import org.locationtech.geomesa.core.filter.spatialFilters
  import org.locationtech.geomesa.utils.geotools.Conversions._

  def getSTIdxStrategy(filter: Filter, sft: SimpleFeatureType): Option[Strategy] =
    if(!spatialFilters(filter)) None
    else {
      val e1 = filter.asInstanceOf[BinarySpatialOperator].getExpression1
      val e2 = filter.asInstanceOf[BinarySpatialOperator].getExpression2
      if(isValidSTIdxFilter(sft, e1, e2)) Some(new STIdxStrategy) else None
    }

  /**
   * Ensures the following conditions:
   *   - there is exactly one 'property name' expression
   *   - the property is indexed by GeoMesa
   *   - all other expressions are literals
   *
   * @param sft
   * @param exp
   * @return
   */
  private def isValidSTIdxFilter(sft: SimpleFeatureType, exp: Expression*): Boolean = {
    val (props, lits) = exp.partition(_.isInstanceOf[PropertyName])

    props.length == 1 &&
      props.map(_.asInstanceOf[PropertyName].getPropertyName).forall(sft.getDescriptor(_).isIndexed) &&
      lits.forall(_.isInstanceOf[Literal])
  }

}
