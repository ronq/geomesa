package geomesa.core.process.knn

import collection.JavaConversions._
import geomesa.core._
import geomesa.utils.text.WKTUtils
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SomeGeoHashesTest extends Specification {

  sequential

  def generateCvilleSF = {
    val sftName = "geomesaKNNTestQueryFeature"
    val sft = DataUtilities.createType(sftName, index.spec)


    val cvilleSF = SimpleFeatureBuilder.build(sft, List(), "equator")
    cvilleSF.setDefaultGeometry(WKTUtils.read(f"POINT(-78.4953560 38.0752150 )"))
    cvilleSF.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    cvilleSF
  }

  "Geomesa SomeGeoHashes PriorityQueue" should {

    "order GeoHashes correctly around Charlottesville" in {
      val cvilleSF = generateCvilleSF
      val cvillePQ = SomeGeoHashes(cvilleSF, 1000.0, 1000.0)
      cvillePQ.exhaustIterator() // call this so that the PriorityQueue can order ALL geohashes
      val cvillePQ2List = cvillePQ.toList()
      val nearest9ByCalculation = cvillePQ2List.take(9).map{_.hash}

      // the below are ordered by the cartesian distances, NOT the geodetic distances
      val nearest9ByVisualInspection = List (
      "dqb0tg",
      "dqb0te",
      "dqb0tf",
      "dqb0td",
      "dqb0tu",
      "dqb0ts",
      "dqb0tc",
      "dqb0t9",
      "dqb0tv")
      nearest9ByCalculation must equalTo(nearest9ByVisualInspection)
    }


    "use the statefulDistanceFilter around Charlottesville correctly" in {
      val cvilleSF = generateCvilleSF
      val cvillePQ = SomeGeoHashes(cvilleSF, 1000.0, 1000.0)
      cvillePQ.updateDistance(0.004)  // units are degrees, so distance is cartesian
      cvillePQ.exhaustIterator() // call this so that the PriorityQueue can order ALL geohashes
      val numHashesAfterFilter = cvillePQ.toList().length
      numHashesAfterFilter must equalTo(6)
    }


    "use the statefulDistanceFilter around Charlottesville correctly when the PriorityQueue is fully loaded" in {
      val cvilleSF = generateCvilleSF
      val cvillePQ = SomeGeoHashes(cvilleSF, 1000.0, 1000.0)
      cvillePQ.exhaustIterator() // call this so that the PriorityQueue can order ALL geohashes
      cvillePQ.updateDistance(0.004)  // units are degrees, so distance is cartesian
      val numHashesAfterFilter = cvillePQ.toList().length
      numHashesAfterFilter must equalTo(6)

    }

  }
}
