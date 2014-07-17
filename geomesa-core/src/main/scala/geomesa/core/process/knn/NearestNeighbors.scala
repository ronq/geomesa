package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Geometry
import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable

class NearestNeighbors(val aFeatureForSearch: SimpleFeature, val numDesired: Int) extends mutable.PriorityQueue[SimpleFeature] {
  val ord: Ordering[SimpleFeature] = Ordering.by { sf: SimpleFeature => distanceCalc(sf.geometry)}.reverse

  def distanceCalc(geom: Geometry) = aFeatureForSearch.point.distance(geom)

  def foundK: Boolean = !(this.length < numDesired)

  def getLast: Option[SimpleFeature] = this.take(numDesired).lastOption

  def maxDistance = getLast.map { sf => distanceCalc(sf.geometry)}

  // this should include a guard against adding two NearestNeighbor collections which are for different points
  // override def ++ (that: NearestNeighbors ) =  that.dequeueAll
  // should override enqueue to prevent more than k elements from being contained
}