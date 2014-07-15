package geomesa.core.process.knn


import geomesa.core.process.knn.EnrichmentPatch._
import geomesa.utils.geohash._
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

/**
 * Object and Class to mock up a generator for the GeoHash "Spiral"
 *
 * For now, we use the BoundindBoxIterator as the GeoHash iterator.
 * Later, this will switch to the Touching GeoHash Iterator
 */
object SomeGeoHashes {
  def apply(centerPoint: SimpleFeature, distanceGuess: Double): SomeGeoHashes = {

    // take the center point and extract some silly bounds.....
    val maxDistanceGuess = 10 * distanceGuess
    val (llGH, urGH) = GeoHashIterator.getBoundingGeoHashes(List(centerPoint.point), 30, maxDistanceGuess)
    val bBox = TwoGeoHashBoundingBox(llGH, urGH)
    val ghIt = new BoundingBoxGeoHashIterator(bBox)
    // feed the center point and iterator to the class, and enqueue the first GH.
    new SomeGeoHashes(centerPoint, ghIt, maxDistanceGuess) { enqueue(ghIt.next()) }
  }
}

class SomeGeoHashes(val aFeatureForSearch: SimpleFeature,
                    val theGHIterator: BoundingBoxGeoHashIterator,
                    val maxRadius: Double) extends mutable.PriorityQueue[GeoHash] {
  // define the ordering for the PriorityQueue
  val ord: Ordering[GeoHash] = Ordering.by { distanceCalc }

  // helper method for distance. NOTE: this likely returns values in degrees
  def distanceCalc(gh: GeoHash) = aFeatureForSearch.point.distance(GeohashUtils.getGeohashGeom(gh))

  // this must be a var since it has state
  var statefulMaxRadius = maxRadius

  // primed with the maximum radius on input, which is a sanity check
  // removes GeoHashes that are further than a certain distance from the aFeatureForSearch
  def statefulDistanceFilter(gh: GeoHash): Boolean = { distanceCalc(gh) < statefulMaxRadius }

  // provides external interface to set the maximum radius
  // FIXME this method has 'code smell'
  def mutateMaxRadius(radiusCandidate: Double): Unit = if (radiusCandidate < statefulMaxRadius)
    statefulMaxRadius = radiusCandidate

  /** returns the next element in the queue, and enqueues the next entry in the iterator
   *  this will return None if no elements in the queue pass the filter
   **/
  def next: Option[GeoHash] =
    for {
      newGH  <- this.find { statefulDistanceFilter }          // get the next element in the queue that passes the filter
      nextGH <- theGHIterator.find { statefulDistanceFilter } // if that was successful, get the same in the iterator
           _ <- this.enqueue(nextGH)                          // if that was successful, add it to the queue
    } yield newGH
}

object EnrichmentPatch {

  implicit class EnrichedBBGHI(bbghi: BoundingBoxGeoHashIterator) {
    def find(func: GeoHash => Boolean): Option[GeoHash] = {
      if (!bbghi.hasNext) None
      else {
        if (func(bbghi.queue.head)) Option(bbghi.next())
        else bbghi.find(func)
      }
    }
  }

}