package geomesa.core.process.knn


import geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable



class NearestNeighbors(val aFeatureForSearch:SimpleFeature, val numDesired: Int ) extends mutable.PriorityQueue[SimpleFeature] {
  val ord: Ordering[SimpleFeature] = Ordering.by {
    sf: SimpleFeature => aFeatureForSearch.point.distance(sf.geometry)
  }

  def foundK: Boolean = !(this.length < numDesired)

  def getLast: SimpleFeature = this.take(numDesired).last

  def maxDistance = if (foundK) aFeatureForSearch.point.distance(getLast.geometry)
  else aFeatureForSearch.point.distance(this.last.geometry) // this can be refactored

  // should override enqueue to prevent more than k elements from being contained
}