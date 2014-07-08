package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.opengis.feature.simple.SimpleFeature

/**
 * Created by ronquest on 7/7/14.
 */
object KNNQuery {
  def runNewKNNQuery(source:SimpleFeatureSource, query: Query, numDesired: Int, searchRadius: Double, anFeatureForSearch:SimpleFeature ) ={
    // setup parameters for the iterative query
    val distanceCalculator
    = new GeodeticDistanceCalc(anFeatureForSearch.getDefaultGeometryProperty.getValue.asInstanceOf[Geometry])
    val distanceVisitor = new GeodeticVisitor(distanceCalculator)

    val geohashPQ   = ???(anFeatureForSearch) // at the beginning of each search, this is the list of geohashes to query
    // may need to setup the SF PQ here as well.
    theKNNResults: SimpleFeatureCollection(source, query, numDesired, searchRadius, geoHashPQ, sfPQ)
  }
  def runKNNQuery(source: SimpleFeatureSource, query: Query, distanceVisitor:GeodeticVisitor, searchRadius: Float, kNN: NearestNeighbors) = {
    kNN match {
      case null =>
    }
  }

}
