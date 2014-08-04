package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Point
import geomesa.utils.geotools.GeometryUtils.distanceDegrees

//import collection.JavaConverters._
//import geomesa.core.process.knn.EnrichmentPatch._
import geomesa.utils.geohash._
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.mutable

case class GeoHashWithDistance(gh: GeoHash, dist: Double)
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
  def statefulDistanceFilter(x: GeoHashWithDistance): Boolean = { x.dist < distanceConversion(statefulFilterDistance) }

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
    val seedWithDistance = GeoHashWithDistance(seedGH, 0.0)
    // take the center point and use the supplied bounds..

    //val ghIt = new BoundingBoxGeoHashIterator(bBox).asScala

    // These are helpers for distance calculations and ordering.
    // FIXME: using JTS distance returns the cartesian distance only, and does NOT handle wraps correctly
    // also, the units are degrees, while meters are used elsewhere. So this won't even work.
    // see GEOMESA-226
    def distanceCalc(gh: GeoHash) = centerPoint.point.distance(gh.geom)
    //def orderedGH: Ordering[GeoHashWithDistance] = Ordering.by { gh: GeoHash => distanceCalc(gh)}
    //def orderedGH: Ordering[GeoHashWithDistance] = Ordering.by {_.dist}.reverse
    def orderedGH: Ordering[GeoHashWithDistance] = Ordering.by { _.dist}
    def metersConversion(meters: Double) =  distanceDegrees(centerPoint.point, meters)

    // Create a new GeoHash PriorityQueue and enqueue the first GH from the iterator as a seed.
    val ghPQ = new mutable.PriorityQueue[GeoHashWithDistance]()(orderedGH.reverse) { enqueue(seedWithDistance) }
    new GeoHashSpiral(ghPQ, distanceCalc, maxDistance, metersConversion)
  }
}

class GeoHashSpiral(pq: mutable.PriorityQueue[GeoHashWithDistance],
                     val distance: (GeoHash) => Double,
                     var statefulFilterDistance: Double,
                     val distanceConversion: (Double) => Double) extends GeoHashDistanceFilter with BufferedIterator[GeoHash] {
  // I may not want to see the oldGH just yet
  val oldGH = new mutable.HashSet[GeoHash] ++= pq.toSet[GeoHashWithDistance].map{_.gh}

  var onDeck: Option[GeoHash] = None
  var nextGHFromPQ: Option[GeoHash] = None
  var nextGHFromTouching: Option[GeoHash] = None

  @tailrec
  private def loadNextGHFromPQ() {
    if (pq.isEmpty) nextGHFromPQ = None
    else {
        val theHead = pq.dequeue()
        //oldGH += theHead.gh
        if (statefulDistanceFilter(theHead)) nextGHFromPQ = Option(theHead.gh)
        else loadNextGHFromPQ()
    }
  }

  private def loadNextGHFromTouching() {
    // may want to seed from onDeck instead
    nextGHFromPQ.foreach { newSeedGH =>
      // obtain only new GeoHashes that touch
      val newTouchingGH = TouchingGeoHashes.touching(newSeedGH).filterNot(oldGH contains)
      // enrich the GeoHashes with distances
      val withDistance = newTouchingGH.map { aGH => GeoHashWithDistance(aGH, distance(aGH))}
      val withinDistance = withDistance.filter(statefulDistanceFilter)
      // add all GeoHashes which pass the filter to the PQ
      withinDistance.foreach { ghWD => pq.enqueue(ghWD)}
      // also add the GeoHashes to the set of old GeoHashes
      // note: we add newTouchingGH now, since the cost of having many extra GeoHashes will likely
      // be less than that of computing the distance for the same GeoHash multiple times,
      // which is what happens if withinDistance is used.
      oldGH ++= newTouchingGH
    }
    //println("pq :" + pq.map{_.gh.hash} )
    //println("oldGH :" + oldGH.map{_.hash} )
  }

  private def loadNext() {
    nextGHFromPQ match {
      case (None) => onDeck = None // nothing left in the priorityQueue
      case (Some(x)) => loadNextGHFromPQ(); loadNextGHFromTouching(); onDeck = Some(x)
    }
  }

  def head = onDeck match {
    case Some(nextGH) => nextGH
    case None => throw new Exception
  }

  def hasNext = onDeck.isDefined

  def next() = head match {case nextGH:GeoHash => loadNext() ; nextGH }

  loadNextGHFromPQ()
  loadNextGHFromTouching()
  loadNext()
}

/**
object EnrichmentPatch {
  // It might be nice to make this more general and dispense with the GeoHash type.
  implicit class EnrichedPQ[A](pq: mutable.PriorityQueue[A]) {
    //@tailrec
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
**/