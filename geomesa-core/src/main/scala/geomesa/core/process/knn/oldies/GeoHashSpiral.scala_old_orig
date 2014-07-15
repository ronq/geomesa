package geomesa.core.process.knn

import geomesa.utils.geohash.{GeohashUtils, GeoHash}
import scala.collection.mutable
import com.vividsolutions.jts.geom.{Geometry, Point}
//import scala.collection.mutable.PriorityQueue
/**
 * This class generates a Priority Queue Of GeoHashes that "spiral out" from a central point.
 *
 * This is useful for itrative searches from a point, such as KNN searches
 */
object GeoHashSpiral{
  // stub. Not used yet
  //def setResolution(radiusInMeters:Double): Int = ???

  def apply (centerPoint: Point, radiusInMeters:Double) = {
  // define the ordering for the GeoHashes in the PriorityQueue
  def distance(gh:GeoHash) = centerPoint.distance(GeohashUtils.getGeohashGeom(gh))
  val ghPtOrdering: Ordering[GeoHash] = Ordering.by {distance}
  //val ghRes = setResolution(radiusInMeters)
  val ghRes = 35 // bits
  val firstGH = GeoHash(centerPoint,ghRes)
  // may want to apply WithFilter here
  new mutable.PriorityQueue[GeoHash]()(ghPtOrdering) += firstGH

  implicit def nextGH(ghQueue: mutable.PriorityQueue[GeoHash]):GeoHash = {
    val thisGH = ghQueue.dequeue() // this will remove the head, and is dangerous for record-keeping if we can ever reach a point CLOSER to the center than the rest
                       // as a result of crawling the spiral
    ghQueue.enqueue(thisGH.immediateNeighbors.filter(thatGH:GeoHash => thisGH.distance < thatGH.distance))
    thisGH
  }

  }
}
