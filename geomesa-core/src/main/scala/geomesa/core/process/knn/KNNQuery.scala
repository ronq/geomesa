package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Geometry
import geomesa.utils.geotools
 import geomesa.utils.geotools.Conversions

import scala.collection.mutable

// implicits
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.opengis.feature.simple.SimpleFeature

object KNNQuery {
  def runNewKNNQuery(source:SimpleFeatureSource,
                     query: Query,
                     numDesired: Int,
                     searchRadius: Double,
                     aFeatureForSearch: SimpleFeature )
  : NearestNeighbors = {
    import geomesa.utils.geotools.Conversions.RichSimpleFeature
    // setup parameters for the iterative query
    // I very well may be able to use a Transform to compute the distances instead of a visitor
    // I may be able to setup a filter too
    // will need to generate transforms and filters on the fly then
    val distanceCalculator
    = new GeodeticDistanceCalc(aFeatureForSearch.getDefaultGeometryProperty.getValue.asInstanceOf[Geometry])
    val distanceVisitor = new GeodeticVisitor(distanceCalculator)
    // setup the GH iterator here -- it needs to use the search point and the searchRadius
    val geohashPQ   = NearestGeoHashes(aFeatureForSearch, searchRadius) // at the beginning of each search, this is the list of geohashes to query
    // may need to setup the SF PQ here as well.
    // Initialize Priority Queue of Geohashes to search
    val sfPQ = new NearestNeighbors(aFeatureForSearch, numDesired)


    // now begin recursion
    runKNNQuery(source, query, distanceVisitor,  geoHashPQ, sfPQ)
  }
  def runKNNQuery(source: SimpleFeatureSource,
                  query: Query,
                  distanceVisitor:GeodeticVisitor,
                  ghPQ: Any,
                  kNN: NearestNeighbors)
    : NearestNeighbors   = {
    // add a filter to the ghPQ if we've already found kNN
    val newghPQ = if (kNN.foundK) ghPQ.withFilter(thing(kNN.maxDistance)) else ghPQ
    if (!newghPQ.hasNext) kNN // return what we've got
    else {
      // generate the new query
      // maybe generate a new feature filter as well if the kNN.isFull
      // getFeatures
      // compute distances if not done already or done below
      // throw the features into the (a?) priority queue
      //kNN ++ runKNNQuery
    }
  }

}
