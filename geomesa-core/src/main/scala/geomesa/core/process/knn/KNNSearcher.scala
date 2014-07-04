package geomesa.core.process.knn

import com.vividsolutions.jts.geom.{Geometry, Point}
import geomesa.utils.geohash.{GeohashUtils, GeoHash}
import geomesa.utils.geotools.Conversions._
import org.geotools.data.{Query, FeatureSource}
import org.geotools.feature.FeatureIterator
import org.opengis.feature.simple.SimpleFeature
import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

class KNNSearcher(pt: Point, fs: FeatureSource, q: Query) {


  // Initialize Priority Queue of Geohashes to search
  val ghPtOrdering: Ordering[GeoHash] = Ordering.by{ gh: GeoHash =>
    pt.distance(GeohashUtils.getGeohashGeom(gh))
  }

  val searchPQ = new mutable.PriorityQueue[GeoHash]()(ghPtOrdering)

  //val gh35 = GeoHash(pt, 35)
  val gh30 = GeoHash(pt, 30)
  val gh25 = GeoHash(pt, 25)

  val base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

  val neighbors35 = base32.map(c => GeoHash(s"${gh30.hash}$c"))
  val neighbors30 = base32.map(c => GeoHash(s"${gh25.hash}$c"))

  searchPQ.enqueue(neighbors30 ++ neighbors35: _*)


  val ghSFOrdering: Ordering[SimpleFeature] = Ordering.by{ sf: SimpleFeature =>
    pt.distance(sf.getDefaultGeometry.asInstanceOf[Geometry])
  }

  val returnPQ = new mutable.PriorityQueue[SimpleFeature]()(ghSFOrdering)


  def doSearch(): Iterator[SimpleFeature] = {

    findTop()

    new Iterator[SimpleFeature] {
      override def hasNext: Boolean = returnPQ.nonEmpty

      override def next(): SimpleFeature = {
        val ret = returnPQ.dequeue()
        findTop()
        ret
      }
    }
  }

  def findTop() {
    if(returnPQ.isEmpty) {

      if(searchPQ.nonEmpty)
        runInternalSearch(searchPQ.dequeue())
      // else
      //   Quit?  get more GeoHashes?  How far do we go?

      findTop()
    } else {
      // What if searchPQ is empty?

      // Compare distances between returnPQ and searchPQ
      val returnDist = pt.distance(returnPQ.head.getDefaultGeometry.asInstanceOf[Geometry])
      val searchDist = pt.distance(searchPQ.head)  // Which implicit is this using?

      if( returnDist > searchDist) {
        runInternalSearch(searchPQ.dequeue())
        findTop()
      }
    }
  }

  def runInternalSearch(gh: GeoHash) = {
    // GH => BBOX
    // ff.and(query.filter, BBOX)?

    val fi = fs.getFeatures.features()
    while(fi.hasNext) { returnPQ.enqueue(fi.next.asInstanceOf[SimpleFeature]) }
  }


}
