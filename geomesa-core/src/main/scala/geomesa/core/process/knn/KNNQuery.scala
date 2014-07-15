package geomesa.core.process.knn

import com.vividsolutions.jts.geom.{Envelope, Polygon, Geometry}
import geomesa.core.index.{QueryHints, SpatialFilter}
import geomesa.utils.geohash.GeoHash
import geomesa.core.filter._
import geomesa.utils.geotools
import geomesa.utils.geotools.Conversions
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS

import scala.collection.mutable



// implicits
import org.geotools.data.{DataUtilities, Query}
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
    // use the horrible implementation first
    val geohashPQ   = SomeGeoHashes(aFeatureForSearch, searchRadius) // at the beginning of each search, this is the list of geohashes to query
    //setup the SimpleFeature PQ as well
    //val sfPQ = new NearestNeighbors(aFeatureForSearch, numDesired)

    // now kickoff recursion
    val numFound = 0
    runKNNQuery(source, query, geohashPQ, aFeatureForSearch, numDesired, numFound)
  }
  def runKNNQuery(source: SimpleFeatureSource,
                  query: Query,
                  ghPQ: SomeGeoHashes,
                  queryFeature:SimpleFeature,
                  numDesired: Int,
                  numFound: Int)
    : NearestNeighbors   = {
    import geomesa.utils.geotools.Conversions.toRichSimpleFeatureIterator
    // add a filter to the ghPQ if we've already found kNN
    //val newghPQ = if (numDesired <= numFound) ghPQ.withFilter(thing(kNN.maxDistance)) else ghPQ
    if (!ghPQ.hasNext) new NearestNeighbors(queryFeature, numDesired)
    else {
      val newGH = ghPQ.next
      // copy the query in order to pass the original to the next recursion
      val newQuery = generateKNNQuery(newGH,query,source)
      val newFeatures = source.getFeatures(newQuery).features.toList
      val numFoundNow = numFound + newFeatures.length // increment number found
      val newNeighbors = new NearestNeighbors(queryFeature, numDesired) ++= newFeatures
      // apply filter to ghPQ if we've found k neighbors
      if (numDesired <= numFound) newNeighbors.maxDistance.foreach{x:Double=>ghPQ.mutateMaxRadius(x)}
      newNeighbors ++= runKNNQuery(source,query,ghPQ,queryFeature,numDesired, numFoundNow)
    }
  }
  def generateKNNQuery(gh: GeoHash, oldQuery: Query, source: SimpleFeatureSource): Query = {
    // setup a new filter to add to the original suite
    val geomProp = ff.property(source.getSchema.getGeometryDescriptor.getName)
    val newGHEnv = new ReferencedEnvelope(gh.bbox, oldQuery.getCoordinateSystem)
    val newGHFilter = ff.bbox(geomProp,newGHEnv)
    // copy the original query before mutation, then AND the new GeoHash filter with the original filter
    new Query(oldQuery) { setFilter(ff.and(oldQuery.getFilter,newGHFilter)) }
  }

}
