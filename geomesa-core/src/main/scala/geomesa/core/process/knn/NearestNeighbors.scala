package geomesa.core.process.knn

import geomesa.utils.geohash.VincentyModel
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

trait NearestNeighbors[T] extends BoundedPriorityQueue[T]  {
   def distance(sf: SimpleFeature):Double
   def maxDistance: Option[Double]
}

object NearestNeighbors {
  def apply(aFeatureForSearch: SimpleFeature, numDesired:Int) = {
    //def distanceCalc(geom: Geometry) = aFeatureForSearch.point.distance(geom)
    def distanceCalc(sf: SimpleFeature) = VincentyModel.getDistanceBetweenTwoPoints(aFeatureForSearch.point, sf.point).getDistanceInMeters
    def orderedSF: Ordering[SimpleFeature] = Ordering.by { sf: SimpleFeature => distanceCalc(sf)}.reverse
    new mutable.PriorityQueue[SimpleFeature]()(orderedSF) with NearestNeighbors[SimpleFeature] {
      override val maxSize = numDesired
      implicit override val ord = orderedSF
      override def distance(sf: SimpleFeature) = distanceCalc(sf)
      override def maxDistance = getLast.
                 map { theFeature:SimpleFeature => distance(theFeature)}
    }
  }
}

/**
  // this should include a guard against adding two NearestNeighbor collections which are for different points
  // override def ++ (that: NearestNeighbors ) =  that.dequeueAll
  // should override enqueue to prevent more than k elements from being contained
}
**/