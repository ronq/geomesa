package geomesa.core.process.knn


import geomesa.utils.geohash._
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable
import EnrichmentPatch._

/**
 * Object and Class to mock up a generator for the GeoHash "Spiral"
 *
 * For now, we use the BoundindBoxIterator as the GeoHash iterator.
 * Later, this will switch to the Touching GeoHash Iterator
 */
object SomeGeoHashes {
  def apply(centerPoint:SimpleFeature, distanceGuess: Double) ={

     // take the center point and extract some silly bounds.....
     val maxDistanceGuess = 10 * distanceGuess
     val (llGH, urGH) = GeoHashIterator.getBoundingGeoHashes(List(centerPoint.point),30, maxDistanceGuess)
     val bBox = TwoGeoHashBoundingBox(llGH, urGH)
     val ghIt = new BoundingBoxGeoHashIterator(bBox)
     // feed the center point and iterator to the class, and enqueue the first GH.
     new SomeGeoHashes(centerPoint, ghIt, maxDistanceGuess).enqueue(ghIt.next())
  }
}
class SomeGeoHashes(val aFeatureForSearch:SimpleFeature,
                    val theGHIterator: BoundingBoxGeoHashIterator,
                    val maxRadius: Double ) extends mutable.PriorityQueue[GeoHash] {
  // define the ordering for the PriorityQueue
  val ord: Ordering[GeoHash] = Ordering.by {distanceCalc}
  // helper method for distance. NOTE: this likely returns values in degrees
  def distanceCalc(gh:GeoHash) = aFeatureForSearch.point.distance(GeohashUtils.getGeohashGeom(gh))
  // this must be a var since it has state
  var statefulMaxRadius = maxRadius // primed with the maximum radius on input, which is a sanity check
  val statefulMinRadius = 0.0
  // removes GeoHashes that are further than a certain distance from the aFeatureForSearch
  def statefulDistanceFilter(gh:GeoHash):Boolean = {distanceCalc(gh) < statefulMaxRadius  &&
                                                    distanceCalc(gh) > statefulMinRadius }

  def mutateMaxRadius(radiusCandidate: Double):Unit = if (radiusCandidate < statefulMaxRadius)
                     statefulMaxRadius = radiusCandidate // this has code smell
  def hasNext: Boolean = this.nonEmpty
  // this should get the first element,
  // then find the touching GeoHashes
  // and for each touching GeoHash, add it IF the distance is LARGER than the
  // new closest geohash
  // if the distance is smaller, log a warning!
  def next: GeoHash = {
    if (this.nonEmpty) {
      // get the current return element out of the PriorityQueue
      // this is done here since we may want to use it to do filtering on the GHIterator
      val retVal = this.dequeue()
      // access the GHIterator
      if (theGHIterator.hasNext()) {
        // stream in the next value(s) from the iterator. We may need retVal to generate this
        val theNext = theGHIterator.find{gh:GeoHash =>statefulDistanceFilter(gh)}
        theNext.foreach{nextGH => this.enqueue(nextGH)} // load them into the PQ
      }
      retVal
    }
    else throw new NoSuchElementException("No more geohashes available in PriorityQueue")
  }

}

object EnrichmentPatch{
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