package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Point
import geomesa.utils.geotools.GeometryUtils.distanceDegrees

//import collection.JavaConverters._
import geomesa.core.process.knn.EnrichmentPatch._
import geomesa.utils.geohash._
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Object and Class to mock up a generator for the GeoHash "Spiral"
 *
 * For now, we use the BoundindBoxIterator as the GeoHash iterator.
 * Later, this will switch to the Touching GeoHash Iterator
 */
trait NearestGeoHash {
  def distance: GeoHash => Double
}

trait GeoHashDistanceFilter extends NearestGeoHash {
  var statefulFilterDistance: Double


  // removes GeoHashes that are further than a certain distance from the aFeatureForSearch
  // as long as distance is in cartesian degrees, it needs to be compared to the
  // statefulFilterDistance converted to degrees
  def statefulDistanceFilter(gh: GeoHash): Boolean = { distance(gh) < distanceConversion(statefulFilterDistance) }

  // this is the conversion from distance in meters to the maximum distance in degrees
  // this can be removed once GEOMESA-226 is resolved
  def distanceConversion: Double => Double

  // modify the minimum
  def mutateFilterDistance(theNewMaxDistance: Double) {
    if (theNewMaxDistance < statefulFilterDistance) statefulFilterDistance = theNewMaxDistance
  }
}
trait GeoHashAutoSize {
  // typically 25 bits are encoded in the Index Key
  val allowablePrecisions = List(25,30,35,40) // should be useful for
  // find the smallest GeoHash whose minimumSize is larger than the desiredSizeInMeters
  def geoHashToSize(pointInside: Point, desiredSizeInMeters: Double ): GeoHash = {
    val variablePrecGH = allowablePrecisions.reverse.map { prec => GeoHash(pointInside,prec) }
    val largeEnoughGH = variablePrecGH.filter { gh => GeohashUtils.getGeohashMinDimensionMeters(gh) > desiredSizeInMeters  }
    largeEnoughGH.head
  }
}

object GeoHashSpiral extends GeoHashAutoSize {
  def apply(centerPoint: SimpleFeature, distanceGuess: Double, maxDistance: Double) = {
    val seedGH = geoHashToSize(centerPoint.point, distanceGuess)
    // take the center point and use the supplied bounds..

    //val ghIt = new BoundingBoxGeoHashIterator(bBox).asScala

    // These are helpers for distance calculations and ordering.
    // FIXME: using JTS distance returns the cartesian distance only, and does NOT handle wraps correctly
    // also, the units are degrees, while meters are used elsewhere. So this won't even work.
    // see GEOMESA-226
    def distanceCalc(gh: GeoHash) = centerPoint.point.distance(gh.geom)
    def orderedGH: Ordering[GeoHash] = Ordering.by { gh: GeoHash => distanceCalc(gh)}
    def metersConversion(meters: Double) =  distanceDegrees(centerPoint.point, meters)

    // Create a new GeoHash PriorityQueue and enqueue the first GH from the iterator as a seed.
    val ghPQ = new mutable.PriorityQueue[GeoHash]()(orderedGH) { enqueue(seedGH) }
    new GeoHashSpiral(ghPQ, distanceCalc, maxDistance, metersConversion)
  }
}
// from jnh5y: this should just implement Iterator
class GeoHashSpiral(pq: mutable.PriorityQueue[GeoHash],
                     val distance: (GeoHash) => Double,
                     var statefulFilterDistance: Double,
                     val distanceConversion: (Double) => Double) extends GeoHashDistanceFilter with Iterator[GeoHash] {

  // this is not efficient, look at on deck pattern instead
  // check that at least one element in pq will pass the distance filter
  def hasNext: Boolean = pq.exists{statefulDistanceFilter}
  // this is not efficient, look at on deck pattern instead
  def next(): GeoHash =
  { for {
      newGH <- pq.dequeuingFind { statefulDistanceFilter } // get the next element in the queue that passes the filter
      _      = TouchingGeoHashes.touching(newGH).filter { statefulDistanceFilter } foreach {gh:GeoHash => pq.enqueue(gh)} // insert the same from the generator
    } yield newGH}.head
}


object EnrichmentPatch {
  // It might be nice to make this more general and dispense with the GeoHash type.
  implicit class EnrichedPQ[A](pq: mutable.PriorityQueue[A]) {
    @tailrec
    final def dequeuingFind(func: A => Boolean): Option[A] = {
      if (pq.isEmpty) None
      else {
        val theHead = pq.dequeue()
        if (func(theHead)) Option(theHead)
        else dequeuingFind(func)
      }
    }
  }
}