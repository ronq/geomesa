package geomesa.core.process.knn


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
  var statefulFilterRadius: Double
  // removes GeoHashes that are further than a certain distance from the aFeatureForSearch
  def statefulDistanceFilter(gh: GeoHash): Boolean = { distance(gh) < statefulFilterRadius }

  def mutateFilterRadius(radiusCandidate: Double): Unit = 
  {statefulFilterRadius = math.min(radiusCandidate, statefulFilterRadius)}
}

object SomeGeoHashes {
  def apply(centerPoint: SimpleFeature, distanceGuess: Double, maxDistance: Double) = {

    // take the center point and use the supplied bounds..
    val (llGH, urGH) = GeoHashIterator.getBoundingGeoHashes(List(centerPoint.point), 30, maxDistance)
    val bBox = TwoGeoHashBoundingBox(llGH, urGH)
    val ghIt = new BoundingBoxGeoHashIterator(bBox)

    // These are helpers for distance calculations and ordering.
    // FIXME: using JTS distance returns the cartesian distance only, and does NOT handle wraps correctly
    // also, the units are degrees, while meters are used elsewhere. So this won't even work
    def distanceCalc(gh: GeoHash) = centerPoint.point.distance(GeohashUtils.getGeohashGeom(gh))
    def orderedGH: Ordering[GeoHash] = Ordering.by { gh: GeoHash => distanceCalc(gh)}

    // Create a new GeoHash PriorityQueue and enqueue the first GH from the iterator as a seed.
    val ghPQ = new mutable.PriorityQueue[GeoHash]()(orderedGH) { enqueue(ghIt.next()) }
    new SomeGeoHashes(ghPQ, ghIt, distanceCalc, maxDistance)
    }
}

class SomeGeoHashes(pq: mutable.PriorityQueue[GeoHash],
                     it: BoundingBoxGeoHashIterator,
                     distanceDef: GeoHash => Double,
                     maxDistance: Double  ) extends GeoHashDistanceFilter {
  override def distance = distanceDef
  override var statefulFilterRadius = maxDistance
  def updateDistance(theNewMaxDistance: Double) {
    if (theNewMaxDistance < statefulFilterRadius) statefulFilterRadius = theNewMaxDistance
  }
  // Note that next returns an Option. There is then no need to define hasNext
  def next: Option[GeoHash] =
    for {
      newGH  <- pq.dequeuingFind { statefulDistanceFilter }          // get the next element in the queue that passes the filter
      _      = println("test gh:"+ newGH.hash)
      nextGH <- it.find { statefulDistanceFilter }          // if that was successful, get the same in the iterator
      _ = pq.enqueue(nextGH)                                // if that was successful, add it to the queue
    } yield newGH
  def toList: List[GeoHash] = {getNext(List[GeoHash]()) }
  def getNext(ghList: List[GeoHash]): List[GeoHash] = {
      next match {
        case None => ghList
        case Some(element) => getNext(element::ghList)
      }
    }
  def dequeuingFind(pred: GeoHash => Boolean) = {
    // remove all leading elements that do NOT match the predicate
    // could just do pq = pq.dropWhile(!pred)
    //                    Some(pq.dequeue)


  }

}

object EnrichmentPatch {
  // It might be nice to make this more general and dispense with the GeoHash type.
  implicit class EnrichedPQ(pq: mutable.PriorityQueue[GeoHash]) {
    @tailrec
    final def dequeuingFind(func: GeoHash => Boolean): Option[GeoHash] = {
      if (pq.isEmpty) None
      else {
        val theHead = pq.dequeue()
        if (func(theHead)) Option(theHead)
        else dequeuingFind(func)
      }
    }
  }

  implicit class EnrichedBBGHI(bbghi: BoundingBoxGeoHashIterator) {
    @tailrec
    final def find(func: GeoHash => Boolean): Option[GeoHash] = {
      if (!bbghi.hasNext) None
      else {
        if (func(bbghi.queue.head)) Option(bbghi.next())
        else bbghi.find(func)
      }
    }
  }

}