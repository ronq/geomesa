/*
 * Copyright 2013-2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.index

import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.Id
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

//Expand the test - https://geomesa.atlassian.net/browse/GEOMESA-308
@RunWith(classOf[JUnitRunner])
class QueryStrategyDeciderTest extends Specification {

  val sftIndex = SimpleFeatureTypes.createType("feature", "id:Integer:index=false,*geom:Point:srid=4326:index=true,dtg:Date,attr1:String:index=false,attr2:String:index=true")
  val sftNonIndex = SimpleFeatureTypes.createType("featureNonIndex", "id:Integer,*geom:Point:srid=4326,dtg:Date,attr1:String,attr2:String")

  sftIndex.getUserData.put(SF_PROPERTY_START_TIME, "dtg")
  sftNonIndex.getUserData.put(SF_PROPERTY_START_TIME, "dtg")

  def getStrategy(filterString: String, isCatalogTable: Boolean = true): Strategy = {
    val sft = if (isCatalogTable) sftIndex else sftNonIndex
    val filter = ECQL.toFilter(filterString)
    val query = new Query(sft.getTypeName)
    query.setFilter(filter)
    QueryStrategyDecider.chooseStrategy(isCatalogTable, sft, query)
  }

  "Spatio-temporal filters" should {
    "get the stidx strategy" in {
      val fs = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

      getStrategy(fs) must beAnInstanceOf[STIdxStrategy]
    }
  }

  "Attribute filters" should {
    "get the attribute equals strategy" in {
      val fs = "attr2 = val56"

      getStrategy(fs) must beAnInstanceOf[AttributeIdxEqualsStrategy]
    }

    "get the attribute equals strategy" in {
      val fs = "attr1 = val56"

      getStrategy(fs) must beAnInstanceOf[STIdxStrategy]
    }

    "get the attribute likes strategy" in {
      val fs = "attr2 ILIKE '2nd1%'"

      getStrategy(fs) must beAnInstanceOf[AttributeIdxLikeStrategy]
    }

    "get the stidx strategy if attribute non-indexed" in {
      val fs = "attr1 ILIKE '2nd1%'"

      getStrategy(fs) must beAnInstanceOf[STIdxStrategy]
    }
  }

  "Attribute filters" should {
    "get the stidx strategy if not catalog" in {
      val fs = "attr1 ILIKE '2nd1%'"

      getStrategy(fs, isCatalogTable = false) must beAnInstanceOf[STIdxStrategy]
    }
  }

  "Id filters" should {
    "get the attribute equals strategy" in {
      val fs = "IN ('val56')"

      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }

    "get the stidx strategy if not catalog" in {
      val fs = "IN ('val56')"

      getStrategy(fs, isCatalogTable = false) must beAnInstanceOf[STIdxStrategy]
    }
  }

  "Id and Spatio-temporal filters" should {
    "get the records strategy" in {
      val fs = "IN ('val56') AND INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }

  "Id and Attribute filters" should {
    "get the records strategy" in {
      val fs = "IN ('val56') AND attr2 = val56"

      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }
  "Really complicated Id AND * filters" should {
    "get the records strategy" in {
      val fsFragment1="INTERSECTS(geom, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))"
      val fsFragment2="AND IN ('val56','val55') AND attr2 = val56 AND IN('val59','val54') AND attr2 = val60"
      val fs = s"$fsFragment1 $fsFragment2"
      getStrategy(fs) must beAnInstanceOf[RecordIdxStrategy]
    }
  }


  // TODO: GEOMESA-311
  "Anded Attribute filters" should {
    "get an attribute strategy" in {
      val fs = "attr2 = val56 AND attr1 = val3"

      getStrategy(fs) must beAnInstanceOf[AttributeIdxEqualsStrategy]
    }.pendingUntilFixed
  }

}
