package geomesa.core.process.knn

import geomesa.core.filter._
import geomesa.utils.geohash.GeoHash
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.simple.SimpleFeature

/**
 *  This object contains the main algorithm for the GeoHash-based iterative KNN search.
 */


object KNNQuery {
  /**
   * Method to kick off a new KNN query about aFeatureForSearch
   */
  def runNewKNNQuery(source:SimpleFeatureSource,
                     query: Query,
                     numDesired: Int,
                     searchRadius: Double,
                     aFeatureForSearch: SimpleFeature )
  : NearestNeighbors = {
    // setup the GH iterator -- it requires the search point and the searchRadius
    // use the horrible implementation first
    val geoHashPQ   = SomeGeoHashes(aFeatureForSearch, searchRadius)
    // setup the stateful object for record keeping
    val searchStatus = KNNSearchStatus(numDesired, geoHashPQ.statefulMaxRadius )
    // begin the search with the recursive method
    runKNNQuery(source, query, geoHashPQ, aFeatureForSearch, searchStatus)
  }

  /**
   * Recursive function to query a single GeoHash and insert its contents into a PriorityQueue
   */
  def runKNNQuery(source: SimpleFeatureSource,
                  query: Query,
                  ghPQ: SomeGeoHashes,
                  queryFeature:SimpleFeature,
                  status: KNNSearchStatus)
    : NearestNeighbors   = {
    import geomesa.utils.geotools.Conversions.toRichSimpleFeatureIterator
    // filter the ghPQ if we've already found kNN
    if (status.foundK) ghPQ.mutateMaxRadius(status.currentMaxRadius)
    ghPQ.next match {
      case None => new NearestNeighbors(queryFeature, status.numDesired)
      case Some(newGH) =>
        // copy the query in order to pass the original to the next recursion
        val newQuery = generateKNNQuery(newGH, query, source)
        val newFeatures = source.getFeatures(newQuery).features.toList
        status.updateNum(newFeatures.length) // increment number found
        val newNeighbors = new NearestNeighbors(queryFeature, status.numDesired) ++= newFeatures
        // apply filter to ghPQ if we've found k neighbors
        if (status.foundK) newNeighbors.maxDistance.foreach {
          status.updateDistance
        }
        newNeighbors ++= runKNNQuery(source, query, ghPQ, queryFeature, status)
    }
  }

  /**
   * Generate a new query by narrowing another down to a single GeoHash
   */
  def generateKNNQuery(gh: GeoHash, oldQuery: Query, source: SimpleFeatureSource): Query = {
    // setup a new BBOX filter to add to the original suite
    val geomProp = ff.property(source.getSchema.getGeometryDescriptor.getName)
    val newGHEnv = new ReferencedEnvelope(gh.bbox, oldQuery.getCoordinateSystem)
    val newGHFilter = ff.bbox(geomProp,newGHEnv)
    // could ALSO apply a dwithin filter if k neighbors have been found.
    // copy the original query before mutation, then AND the new GeoHash filter with the original filter
    new Query(oldQuery) { setFilter(ff.and(oldQuery.getFilter,newGHFilter)) }
  }

}
