package geomesa.core.process.knn


import geomesa.utils.geohash.{GeoHash, GeohashUtils}
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable

object NearestGeoHashes {
  def apply(centerPoint:SimpleFeature, distanceGuess: Double) ={
     // obtain the geohash that includes centerPoint
     //we will want to use distanceGuess to find the right size later
     val gh30 = GeoHash(centerPoint.point, 30)
     // I should feed in a geohash iterator too at some point
     new NearestGeoHashes(centerPoint).enqueue(gh30)
  }
}
class NearestGeoHashes(val aFeatureForSearch:SimpleFeature) extends mutable.PriorityQueue[GeoHash] {
  val ord: Ordering[GeoHash] = Ordering.by {
    gh: GeoHash => aFeatureForSearch.point.distance(GeohashUtils.getGeohashGeom(gh))
  }
  // this should check to see if the queue has been exhausted
  def hasNext: Boolean = false // FIXME
  // this should get the first element,
  // then find the touching GeoHashes
  // and for each touching GeoHash, add it IF the distance is LARGER than the
  // new closest geohash
  // if the distance is smaller, log a warning!
  def next: GeoHash = this.dequeue()  //FIXME

}