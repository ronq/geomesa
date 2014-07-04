package geomesa.core.process.knn

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{CalcResult, FeatureCalc, AbstractCalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Geomesa-enabled K Nearest Neighbor Search",
  description = "Performs a K Nearest Neighbor search on a Geomesa feature collection using another feature collection as input"
)
class KNearestNeighborSearchProcess {

  private val log = Logger.getLogger(classOf[KNearestNeighborSearchProcess])

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "Input feature collection that defines the KNN search")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "numDesired",
                 description = "K: number of nearest neighbors to return")
               numDesired: java.lang.Integer,

               @DescribeParameter(
                 name = "bufferDistance",
                 description = "Buffer size in meters")
               bufferDistance: java.lang.Double

               ): SimpleFeatureCollection = {

    log.info("Attempting Geomesa K-Nearest Neighbor Search on collection type " + dataFeatures.getClass.getName)

    if(!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support geomesa KNN search: "+dataFeatures.getClass.getName)
    }
    if(dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }
    val visitor = new KNNVisitor(inputFeatures, dataFeatures, numDesired, bufferDistance)
    dataFeatures.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[KNNResult].results
  }
}

class KNNVisitor( inputFeatures: SimpleFeatureCollection,
                               dataFeatures: SimpleFeatureCollection,
                               numDesired: java.lang.Integer,
                               bufferDistance: java.lang.Double
                             ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[KNNVisitor])

  val geoFac = new GeometryFactory
  val ff = CommonFactoryFinder.getFilterFactory2

  var manualFilter: Filter = _
  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // Called for non AccumuloFeactureCollections - here we use degrees for our filters
  // since we are manually evaluating them.
  def visit(feature: Feature): Unit = {
    manualFilter = Option(manualFilter).getOrElse(dwithinFilters("degrees"))
    val sf = feature.asInstanceOf[SimpleFeature]

    if(manualFilter.evaluate(sf)) {
      manualVisitResults.add(sf)
    }
  }

  var resultCalc: KNNResult = new KNNResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = KNNResult(r)

  def kNNSearch(source: SimpleFeatureSource, query: Query) = {
    log.info("Running Geomesa K-Nearest Neighbor Search on source type "+source.getClass.getName)
    //query.setSortBy()
    // we have numDesired
    // we have the starting bufferRadius
    // now we need to start a loop or a recursion
    //     with the current bufferRadius, generate the query:
    //                                      for now, it should be BBOX(sides of bufferRadius) and NOT old BBOX
    //     run the query
    //     pass the query results into the sorterClass
    //     ask if we have k results:
    //               if yes, get the largest distance:
    //                                ensure that we have properly covered a search radius out to the largest distance
    //                                           --> generate new query if not
    //                                           --> done if yes
    //               if no, generate a new query and begin again.



   // for now, cycle through each of the inputFeatures
   val inputList = inputFeatures.features.toList // the user should NOT be feeding in a large number of query points.
   inputList.flatMap(anFeatureForSearch => runNewKNNQuery(source,query, anFeatureForSearch))



    val combinedFilter = ff.and(query.getFilter, dwithinFilters("meters"))
    source.getFeatures(combinedFilter)
  }
  def runNewKNNQuery(source:SimpleFeatureSource, query: Query, theSearchFeature:SimpleFeature) = {
    val distanceCalculator
    = new GeodeticDistanceCalc(theSearchFeature.getDefaultGeometryProperty.getValue.asInstanceOf[Geometry])
    val distanceVisitor = new GeodeticVisitor(distanceCalculator)
    runKNNQuery(source,query,distanceVisitor,searchRadius, currentKNN)
  }
  def runKNNQuery(source: SimpleFeatureSource, query: Query, distanceVisitor:GeodeticVisitor, searchRadius: Float kNN: NearestNeighbors) = {
    kNN match {
      case null =>
    }
  }
  def dwithinFilters(requestedUnit: String) = {
    import geomesa.utils.geotools.Conversions.RichGeometry
    import scala.collection.JavaConversions._

    val geomProperty = ff.property(dataFeatures.getSchema.getGeometryDescriptor.getName)
    val geomFilters = inputFeatures.features().map { sf =>
      val dist: Double = requestedUnit match {
        case "degrees" => sf.geometry.distanceDegrees(bufferDistance)
        case _         => bufferDistance
      }
      ff.dwithin(geomProperty, ff.literal(sf.geometry), dist, "meters")
    }
    ff.or(geomFilters.toSeq)
  }
}

case class KNNResult(results: SimpleFeatureCollection) extends AbstractCalcResult
