package geomesa.core.process.knn

import collection.JavaConverters._
import com.vividsolutions.jts.geom.GeometryFactory
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
               bufferDistance: java.lang.Double,

               @DescribeParameter(
                 name = "maxSearchRadius",
                 description = "Maximum search radius in meters---used to prevent runaway queries of the entire table")
               maxSearchRadius: java.lang.Double
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

/**
 *  The main visitor class for the KNN search process
 */

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

  // called for non AccumuloFeatureCollections
  // does this warrant an actual implementation?
  def visit(feature: Feature): Unit = {}

  var resultCalc: KNNResult = new KNNResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = KNNResult(r)

  /** The KNN-Search interface for the WPS process.
    *
    * Takes as input a Query and SimpleFeatureSource, in addition to
    *  inputFeatures which define one or more SimpleFeatures for which to find KNN of each
    *
    *  Note that the results are NOT de-duplicated!
    *
    */
  def kNNSearch(source: SimpleFeatureSource, query: Query) = {
    log.info("Running Geomesa K-Nearest Neighbor Search on source type "+source.getClass.getName)

   // collection approach to creating a new feature collection, and filling it with the results of
   // a KNN search for each feature in the inputFeatures
   // comment out as this requires multiple lines
   /**
    new DefaultFeatureCollection {
       inputFeatures.features.map {
         aFeatureForSearch =>addAll(
           KNNQuery.runNewKNNQuery(source, query, numDesired, bufferDistance, aFeatureForSearch ).dequeueAll.asJava )
       } }
    **/
   // for-loop implementation
   for {
     resultsCollection:DefaultFeatureCollection <- new DefaultFeatureCollection()
     aFeatureForSearch <- inputFeatures.features
     theKNN = KNNQuery.runNewKNNQuery(source, query, numDesired, bufferDistance, aFeatureForSearch ).dequeueAll.asJava
     _      <- resultsCollection.addAll(theKNN)
   } yield resultsCollection
  }
}

case class KNNResult(results: SimpleFeatureCollection) extends AbstractCalcResult
