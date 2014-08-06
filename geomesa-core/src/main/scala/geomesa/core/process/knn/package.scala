package geomesa.core.process

import org.opengis.feature.simple.SimpleFeature

/**
 * Created by ronquest on 8/6/14.
 */
package object knn {
  type BoundedNearestNeighbors[T] = BoundedPriorityQueue[T] with NearestNeighbors
  //type   BoundedPriorityQueue[(SimpleFeature, Double)](numDesired)(orderedSF)
  //with NearestNeighbors[(SimpleFeature, Double)]

}
