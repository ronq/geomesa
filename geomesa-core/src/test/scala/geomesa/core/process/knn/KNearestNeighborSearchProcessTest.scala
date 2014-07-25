package geomesa.core.process.knn

import collection.JavaConversions._
import com.vividsolutions.jts.geom.Coordinate
import geomesa.core.data.{AccumuloFeatureStore, AccumuloDataStore}
import geomesa.core.index.Constants
import geomesa.utils.geotools.GeometryUtils
import geomesa.utils.text.WKTUtils
import org.geotools.data.{DataUtilities, DataStoreFinder}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

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
       "user"       -> "myuser",
       "password"   -> "mypassword",
       "auths"      -> "A,B,C",
       "tableName"  -> "testwrite",
       "useMock"    -> "true",
       "featureEncoding" -> "avro")).asInstanceOf[AccumuloDataStore]

     val sftName = "geomesaKNNTestType"
     val sft = DataUtilities.createType(sftName, s"type:String,$geotimeAttributes")
     sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

     val ds = createStore

     ds.createSchema(sft)
     val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

    val featureCollection = new DefaultFeatureCollection(sftName, sft)

    List("a", "b").foreach { name =>
      List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
        val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
        sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
        sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type", name)
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        featureCollection.add(sf)
      }
    }

    // write the feature to the store
    val res = fs.addFeatures(featureCollection)

    val geoFactory = JTSFactoryFinder.getGeometryFactory

    def getPoint(lat: Double, lon: Double, meters: Double) =
      GeometryUtils.farthestPoint(geoFactory.createPoint(new Coordinate(lat, lon)), meters)

   "GeomesaKNearestNeighborSearch" should {
     "find things close by" in {
       import geomesa.utils.geotools.Conversions._
       val p1 = getPoint(45, 45, 99)
       WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) should be equalTo true
       WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) should be equalTo true
       WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) should be equalTo false
       val p2 = getPoint(46, 46, 99)
       val p3 = getPoint(47, 47, 99)


       val inputFeatures = new DefaultFeatureCollection(sftName, sft)
       List(1,2,3).zip(List(p1, p2, p3)).foreach  { case (i, p) =>
         val sf = SimpleFeatureBuilder.build(sft, List(), i.toString)
         sf.setDefaultGeometry(p)
         sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
         sf.setAttribute("type", "fake")
         sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
         inputFeatures.add(sf)
       }

       val dataFeatures = fs.getFeatures()

       dataFeatures.size should be equalTo 8
       val prox = new KNearestNeighborSearchProcess
       prox.execute(inputFeatures, dataFeatures, 5, 50,  5000.0).size should be equalTo 0
       prox.execute(inputFeatures, dataFeatures, 5, 90  ,5000.0).size should be equalTo 0
       prox.execute(inputFeatures, dataFeatures, 5, 99.1,5000.0).size should be equalTo 6
       prox.execute(inputFeatures, dataFeatures, 5, 100 ,5000.0).size should be equalTo 6
       prox.execute(inputFeatures, dataFeatures, 5, 101, 5000.0).size should be equalTo 6
     }
   }

  "GeomesaKNearestNeighborSearch" should {
    "work on non-accumulo feature sources" in {
      import geomesa.utils.geotools.Conversions._
      val p1 = getPoint(45, 45, 99)
      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) should be equalTo true
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) should be equalTo true
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) should be equalTo false
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)


      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      List(1,2,3).zip(List(p1,p2,p3)).foreach  { case (i, p) =>
        val sf = SimpleFeatureBuilder.build(sft, List(), i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type", "fake")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        inputFeatures.add(sf)
      }

      val nonAccumulo = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = SimpleFeatureBuilder.build(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(geomesa.core.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          nonAccumulo.add(sf)
        }
      }

      nonAccumulo.size should be equalTo 8
      val prox = new KNearestNeighborSearchProcess
      prox.execute(inputFeatures, nonAccumulo, 5, 30, 5000.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 5, 98,5000.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 5, 99.0001,5000.0).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 5, 100,5000.0).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 5, 101,5000.0).size should be equalTo 6
    }
  }

 }
