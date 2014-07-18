package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Geometry
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

trait NearestNeighbors[T] extends BoundedPriorityQueue[T]  {
   def distance(geom:Geometry):Double
   def maxDistance: Option[Double]
}

object NearestNeighbors {
  def apply(aFeatureForSearch: SimpleFeature, numDesired:Int) = {
    def distanceCalc(geom: Geometry) = aFeatureForSearch.point.distance(geom)
    def orderedSF: Ordering[SimpleFeature] = Ordering.by { sf: SimpleFeature => distanceCalc(sf.geometry)}.reverse
    new mutable.PriorityQueue[SimpleFeature]()(orderedSF) with NearestNeighbors[SimpleFeature] {
      override val maxSize = numDesired
      implicit override val ord = orderedSF
      override def distance(geom:Geometry) = distanceCalc(geom)
      override def maxDistance = getLast.
                 map { theFeature:SimpleFeature => distance(theFeature.geometry)}
    }
  }
}

/**
class NearestNeighbors(val aFeatureForSearch: SimpleFeature, val numDesired: Int) extends mutable.PriorityQueue[SimpleFeature] {
  implicit override val ord: Ordering[SimpleFeature] = Ordering.by { sf: SimpleFeature => distanceCalc(sf.geometry)}.reverse

  def distanceCalc(geom: Geometry) = aFeatureForSearch.point.distance(geom)

  def foundK: Boolean = !(this.length < numDesired)

  def getLast: Option[SimpleFeature] = this.take(numDesired).lastOption

  def maxDistance = getLast.map { sf => distanceCalc(sf.geometry)}

  // this should include a guard against adding two NearestNeighbor collections which are for different points
  // override def ++ (that: NearestNeighbors ) =  that.dequeueAll
  // should override enqueue to prevent more than k elements from being contained
}
**/