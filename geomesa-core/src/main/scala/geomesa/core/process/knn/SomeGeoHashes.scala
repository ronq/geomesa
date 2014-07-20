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
  def apply(centerPoint: SimpleFeature, distanceGuess: Double) = {

    // take the center point and extract some silly bounds.....
    val maxDistanceGuess = 10 * distanceGuess
    val (llGH, urGH) = GeoHashIterator.getBoundingGeoHashes(List(centerPoint.point), 30, maxDistanceGuess)
    val bBox = TwoGeoHashBoundingBox(llGH, urGH)
    val ghIt = new BoundingBoxGeoHashIterator(bBox)

    // helper method for distance. NOTE: this likely returns values in degrees
    def distanceCalc(gh: GeoHash) = centerPoint.point.distance(GeohashUtils.getGeohashGeom(gh))
    def orderedGH: Ordering[GeoHash] = Ordering.by { gh: GeoHash => distanceCalc(gh)}

    // feed the center point and iterator to the class, and enqueue the first GH.
    //new SomeGeoHashes(centerPoint, ghIt, maxDistanceGuess) { enqueue(ghIt.next()) }

    val ghPQ = new mutable.PriorityQueue[GeoHash]()(orderedGH)
      /**
      with GeoHashDistanceFilter{
      override def distance(gh:GeoHash) = distanceCalc(gh)
      override var statefulFilterRadius = maxDistanceGuess
      override val ord: Ordering[GeoHash] = Ordering.by { distanceCalc }
      **/
      new SomeGeoHashes2(ghPQ, ghIt, distanceCalc, maxDistanceGuess)
    }
}
class SomeGeoHashes2(pq: mutable.PriorityQueue[GeoHash],
                     it: BoundingBoxGeoHashIterator,
                     distanceDef: GeoHash => Double,
                     maxRadius: Double  ) extends GeoHashDistanceFilter {
  override def distance = distanceDef
  override var statefulFilterRadius = maxRadius
  statefulFilterRadius = maxRadius

  var onDeck: Option[GeoHash] = None
  var nextPQ: Option[GeoHash] = None
  var nextIt: Option[GeoHash] = None

  private def loadNextPQ() {
    nextPQ = pq.find { statefulDistanceFilter }
  }

  private def loadNextIt() {
    nextIt =  it.find { statefulDistanceFilter }.map{pq.enqueue}
  }

  private def loadNext() {
    def next: Option[GeoHash] =
      for {
        newGH  <- pq.find { statefulDistanceFilter }          // get the next element in the queue that passes the filter
        nextGH <- it.find { statefulDistanceFilter } // if that was successful, get the same in the iterator
        _ = pq.enqueue(nextGH)                          // if that was successful, add it to the queue
      } yield newGH
    /**
    (nextPQ, nextIt) match {
      case (None, _) => onDeck = None  // nothing left in the priorityQueue
      //case (_, None) => onDeck = None  // nothing left in the Iterator
      //case (Some(x), Some(y)) if x < y => loadNextX(); loadNext()
      //case (Some(x), Some(y)) if x > y => loadNextY(); loadNext()
      case (Some(x), Some(t)) =>  loadNextPQ(); loadNextIt(); onDeck = Some(x)
    }
    **/
  }

  def hasNext() = onDeck.isDefined
  def next() = {loadNext()
               onDeck.getOrElse(throw new Exception)  }
  loadNextPQ()
  loadNextIt()
  loadNext()
}
/**
class SomeGeoHashes(val aFeatureForSearch: SimpleFeature,
                    val theGHIterator: BoundingBoxGeoHashIterator,
                    val maxRadius: Double) extends mutable.PriorityQueue[GeoHash] {


  /** returns the next element in the queue, and enqueues the next entry in the iterator
   *  this will return None if no elements in the queue pass the filter
   **/
  def next: Option[GeoHash] =
    for {
      newGH  <- this.find { statefulDistanceFilter }          // get the next element in the queue that passes the filter
      nextGH <- theGHIterator.find { statefulDistanceFilter } // if that was successful, get the same in the iterator
           _ = this.enqueue(nextGH)                          // if that was successful, add it to the queue
    } yield newGH
}
**/
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