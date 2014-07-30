package geomesa.core.process.knn

import geomesa.feature.AvroSimpleFeatureFactory
import geomesa.utils.geotools.Conversions._
import collection.JavaConversions._

import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
import geomesa.core.index.{IndexSchemaBuilder, Constants}

import geomesa.utils.text.WKTUtils
import org.geotools.data.{DataUtilities, DataStoreFinder}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.Random

case class TestEntry(wkt: String, id: String, dt: DateTime = new DateTime())

@RunWith(classOf[JUnitRunner])
class KNearestNeighborSearchProcessTest extends Specification {

  sequential

  val dtgField = geomesa.core.process.tube.DEFAULT_DTG_FIELD

  val geotimeAttributes = s"*geom:Geometry:srid=4326,$dtgField:Date"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user" -> "myuser",
      "password" -> "mypassword",
      "auths" -> "A,B,C",
      "tableName" -> "testwrite",
      "useMock" -> "true",
      "indexSchemaFormat" -> new IndexSchemaBuilder("~").randomNumber(3).constant("TEST").geoHash(0, 3).date("yyyyMMdd").nextPart().geoHash(3, 2).nextPart().id().build(),
      "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]


  val sftName = "geomesaKNNTestType"

  val sft = DataUtilities.createType(sftName, s"type:String,$geotimeAttributes")
  sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

  val ds = createStore
  ds.createSchema(sft)

  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)


  val clusterOfPoints = List[TestEntry](
    TestEntry("POINT( -78.503547 38.035475 )", "rotunda"),
    TestEntry("POINT( -78.503923 38.035536 )", "pavilion I"),
    TestEntry("POINT( -78.504059 38.035308 )", "pavilion I"),
    TestEntry("POINT( -78.504276 38.034971 )", "pavilion III"),
    TestEntry("POINT( -78.504424 38.034628 )", "pavilion V"),
    TestEntry("POINT( -78.504617 38.034208 )", "pavilion VII"),
    TestEntry("POINT( -78.503833 38.033938 )", "pavilion IX"),
    TestEntry("POINT( -78.503601 38.034343 )", "pavilion X"),
    TestEntry("POINT( -78.503424 38.034721 )", "pavilion VIII"),
    TestEntry("POINT( -78.503180 38.035039 )", "pavilion VI"),
    TestEntry("POINT( -78.503109 38.035278 )", "pavilion IV"),
    TestEntry("POINT( -78.505152 38.032704 )", "pavilion II"),
    TestEntry("POINT( -78.510295 38.034283 )", "cabell"),
    TestEntry("POINT( -78.522288 38.032844 )", "beams"),
    TestEntry("POINT( -78.520019 38.034511 )", "hep")
  )

  val distributedPoints = generateTestData(1000,38.149894,-79.073639,0.30)
  // add the test points to the feature collection
  addTestData(clusterOfPoints)
  addTestData(distributedPoints)

  // write the feature to the store
  val res = fs.addFeatures(featureCollection)

  // utility method to generate random points about a central point
  // note that these points with be uniform in cartesian space only
  def generateTestData(num: Int, centerLat: Double, centerLon: Double, width:Double) = {
      val rng = new Random(0)
      (1 to num).map(i => {
        val wkt = "POINT(" +
          (centerLon + width * (rng.nextDouble() - 0.5)).toString + " " +
          (centerLat + width * (rng.nextDouble() - 0.5)).toString + " " +
          ")"
        val dt = new DateTime()
        TestEntry(wkt, (100000 + i).toString, dt)
      }).toList
  }

  def addTestData(points: List[TestEntry]) = {
    points.foreach { case e: TestEntry =>
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), e.id)
      sf.setDefaultGeometry(WKTUtils.read(e.wkt))
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
  }

  def queryFeature(label:String, lat:Double, lon: Double) = {
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), label)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lon $lat)"))
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      sf
  }

  // begin tests 

  "GeoMesaKNearestNeighborSearch" should {
    "find nothing within 10km of a single query point " in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      inputFeatures.add( queryFeature("fan mountain",37.878219,-78.692649 ))
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
        knn.execute(inputFeatures, dataFeatures, 5, 50,  10000.0).size must equalTo(0)
    }


    "find 11 points within 400m of a point when k is set to 15 " in {
       val inputFeatures = new DefaultFeatureCollection(sftName, sft)
       inputFeatures.add( queryFeature("madison", 38.036871, -78.502720))
       val dataFeatures = fs.getFeatures()
       val knn = new KNearestNeighborSearchProcess
       knn.execute(inputFeatures, dataFeatures, 15, 50,  400.0).size should be equalTo 11
    }

    // this will not work until the KNN search is completely functional
    // need bounded NearestNeighborPQ
    // need fully working distance filter as well, perhaps
    /**
    "return the five true nearest neighbors" in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      inputFeatures.add( queryFeature("madison", 38.036871, -78.502720))
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      val knnFeatureList = knn.execute(inputFeatures, dataFeatures, 5, 50,  400.0).features().toList
      println(knnFeatureList.map{_.getID})
      knnFeatureList.head.getID must equalTo("rotunda")
      //println(knnFeatureList.map{_.getID}.toString)
    }
    **/
    "handle three query points, one of which will return nothing" in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      inputFeatures.add( queryFeature("madison", 38.036871, -78.502720) )
      inputFeatures.add( queryFeature("fan mountain",37.878219,-78.692649 ) )
      inputFeatures.add( queryFeature("blackfriars", 38.149185, -79.070569 ) )
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      knn.execute(inputFeatures, dataFeatures, 5, 50,  5000.0).size must greaterThan(0)
    }

    "handle an empty query point collection" in {
      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      val dataFeatures = fs.getFeatures()
      val knn = new KNearestNeighborSearchProcess
      knn.execute(inputFeatures, dataFeatures, 5, 50,  5000.0).size must equalTo(0)
    }
  }





 }
