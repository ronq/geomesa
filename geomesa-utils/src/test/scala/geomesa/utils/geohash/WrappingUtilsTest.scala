/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.utils.geohash

import com.vividsolutions.jts.geom._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.typesafe.scalalogging.slf4j.Logging

@RunWith(classOf[JUnitRunner])
class WrappingUtilsTest extends Specification with Logging {

  import WrappingUtils._
  import GeohashUtils._

  val internationalDateLineSafeGeometryTestData : Map[String, (String, String, String)] = Map(
    "[LINE] no span" -> ("LINESTRING(50 50, 60 60)", "POINT(55 55)", "POINT(30 30)"),
    "[LINE] span" -> ("LINESTRING(160 50, -160 60)", "POINT(170 52.5)", "POINT(30 30)"),
    "[POLYGON] span" -> ("POLYGON((-170 80, 170 80, 170 70, -170 70, -170 80))", "POINT(175 75)", "POINT(165 75)"),
    "[POLYGON] span transform right" -> ("POLYGON((-170 80, 170 80, 170 70, -170 70, -170 80))", "POINT(-175 75)", "POINT(165 75)"),
    "[POLYGON] no span" -> ("POLYGON((-170 80, 0 80, 170 80, 170 70, 0 70, -170 70, -170 80))", "POINT(165 75)", "POINT(175 75)"),
    "[MULTILINE] span" -> ("MULTILINESTRING((170 40, -170 40),(40 50, 45 55))", "POINT(175 40)", "POINT(165 40)"),
    "[MULTIPOLYGON] span" -> ("MULTIPOLYGON(((-170 80, 170 80, 170 70, -170 70, -170 80)),((30 30, 30 40, 40 40, 40 30, 30 30)))", "POINT(175 75)", "POINT(30 25)"),
    "[MULTIPOINT] span" -> ("MULTIPOINT(160 50, 40 20)", "POINT(160 50)", "POINT(159 60)"),
    "[MULTIPOINT] no span" -> ("MULTIPOINT(80 50, 40 20)", "POINT(80 50)", "POINT(159 60)"),
    "[MULTIPOINT] no span2" -> ("MULTIPOINT(80 50, 40 20)", "POINT(40 20)", "POINT(159 60)"),
    "[POINT] point no span" -> ("POINT(170 40)", "POINT(170 40)", "POINT(40 40)"),
    "[POLYGON] antarctica" -> ("POLYGON((-179.99900834195893 -89.99982838943765, -179.99900834195893 -84.35288625138337, -164.16219722595443 -78.99626230923768, -158.26351334731075 -77.08619801855957, -158.2192008065469 -77.0736923216127, -55.37731278554597 -61.07063812275956, -45.934374960003026 -60.524831644861884, -45.7198660894195 -60.51625335736412, 135.3432939050984 -66.10066701248235, 135.4887113859158 -66.11100229913357, 143.50863529466415 -66.84460093230936, 153.878803345093 -68.27893198676848, 167.70100874912129 -70.79128509537779, 170.26488326045944 -71.29213307737697, 170.28265995306788 -71.29874766015023, 170.8138936774078 -71.69965342205771,170.88500044784155 -71.75887461315726, 170.96623579935533 -71.83452891065282, 180.0000000000001 -84.35286778378045, 180.0000000000001 -88.38070249172895, 179.99989628106425 -89.99982838943765, -179.99900834195893 -89.99982838943765))", "POINT(-179.999999 -89.99)", "POINT(40 50)"),
    "[LINE] out span" -> ("LINESTRING(-200 50, -160 60)", "POINT(170 52.5)", "POINT(30 30)"),
    "[POLYGON] out span" -> ("POLYGON((-170 80, -190 80, -190 70, -170 70, -170 80))", "POINT(175 75)", "POINT(165 75)"),
    "[POLYGON] out span transform right" -> ("POLYGON((190 80, 170 80, 170 70, 190 70, 190 80))", "POINT(-175 75)", "POINT(165 75)"),
    "[MULTILINE] out span" -> ("MULTILINESTRING((-190 40, -170 40),(40 50, 45 55))", "POINT(175 40)", "POINT(165 40)"),
    "[MULTIPOLYGON] out span" -> ("MULTIPOLYGON(((-170 80, -190 80, -190 70, -170 70, -170 80)),((30 30, 30 40, 40 40, 40 30, 30 30)))", "POINT(175 75)", "POINT(30 25)"),
    "[MULTIPOINT] out span" -> ("MULTIPOINT(-200 50, 40 20)", "POINT(160 50)", "POINT(159 60)"),
    "[POINT] out point no span" -> ("POINT(-190 40)", "POINT(170 40)", "POINT(40 40)")
  )

  internationalDateLineSafeGeometryTestData.map { case (name, (geomString, incl, excl)) =>
      "getInternationalDateLineSafeGeometry" should { s"work for $name" in {
        val geom = wkt2geom(geomString)
        val includedPoint = wkt2geom(incl).asInstanceOf[Point]
        val excludedPoint = wkt2geom(excl).asInstanceOf[Point]
        val decomposed = getInternationalDateLineSafeGeometry(geom)
        logger.debug("International Date Line test geom: " + geom.toText)
        decomposed match {
          case g: GeometryCollection =>
            val coords = (0 until g.getNumGeometries).map { i => g.getGeometryN(i) }
            coords.find(_.contains(includedPoint)).size must beGreaterThan(0)
            coords.find(_.contains(excludedPoint)).size must equalTo(0)
          case _ =>
            decomposed.contains(includedPoint) must equalTo(true)
            decomposed.contains(excludedPoint) must equalTo(false)
        }
      }
    }
  }
}
