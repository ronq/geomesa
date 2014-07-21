package geomesa.core.process.knn

//import geomesa.core.index

import com.vividsolutions.jts.operation.distance.DistanceOp
import geomesa.utils.geohash.GeoHash

import collection.JavaConversions._
import com.vividsolutions.jts.geom.Coordinate
//import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
import geomesa.core._
import geomesa.core.index.Constants
//import geomesa.utils.geotools.GeometryUtils
import geomesa.utils.text.WKTUtils
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SomeGeoHashesTest extends Specification {

  sequential

  def diagonalFeatureCollection: DefaultFeatureCollection = {
    val sftName = "geomesaKNNTestDiagonalFeature"
    val sft = DataUtilities.createType(sftName, index.spec)
    sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = DEFAULT_DTG_PROPERTY_NAME

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    val constantDate = new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate
    // generate a range of Simple Features along a "diagonal"
    Range(0, 91).foreach { lat =>
      val sf = SimpleFeatureBuilder.build(sft, List(), lat.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute(DEFAULT_DTG_PROPERTY_NAME, constantDate)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
    featureCollection
  }

  def polarFeatureCollection: DefaultFeatureCollection = {
    val sftName = "geomesaKNNTestPolarFeature"
    val sft = DataUtilities.createType(sftName, index.spec)
    sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = DEFAULT_DTG_PROPERTY_NAME

    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    val constantDate = new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate
    val polarLat = 89.9
    // generate a range of Simple Features along a "diagonal"
    Range(-180, 180).foreach { lon =>
      val sf = SimpleFeatureBuilder.build(sft, List(), lon.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lon%d $polarLat)"))
      sf.setAttribute(DEFAULT_DTG_PROPERTY_NAME, constantDate)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
    featureCollection
  }

  val sftName = "geomesaKNNTestQueryFeature"
  val sft = DataUtilities.createType(sftName, index.spec)

  val ccriSF = SimpleFeatureBuilder.build(sft, List(), "equator")
  ccriSF.setDefaultGeometry(WKTUtils.read(f"POINT(-78.4953560 38.0752150)"))
  ccriSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

  "Geomesa SomeGeoHashes PriorityQueue" should {
    "find things close by the equator" in {
      import geomesa.utils.geotools.Conversions._
      val ccriGH =  GeoHash(ccriSF.point,30)
      println (ccriGH.hash)
      val ccriPQ = SomeGeoHashes(ccriSF, 1000.0)
      println(ccriPQ.next)
      println(ccriPQ.next)
      val ccriPQ2List = ccriPQ.toList
      //val distances = equatorPQ {newGH => (newGH.hashCode, equatorPQ.distance(newGH) ) }
      //val results = ccriPQ2List
      //println(results)

      println(ccriPQ.next)

      val results = for {
        newGH <- ccriPQ2List
        hashCode = newGH.hash
        dist     = ccriPQ.distance(newGH)
      } yield (hashCode,dist)



      //println(results)
      ccriPQ2List.nonEmpty must beTrue
      //equatorPQ ++= diagonalFeatureCollection.features.toList

      //equatorPQ.dequeue().getID must equalTo("0")
    }


  }
}
