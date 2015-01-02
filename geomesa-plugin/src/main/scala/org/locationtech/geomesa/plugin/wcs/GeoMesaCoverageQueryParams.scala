/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.plugin.wcs

import org.geotools.coverage.grid.GridGeometry2D
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.parameter.Parameter
import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageReader._
import org.locationtech.geomesa.raster.data.RasterQuery
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds}
import org.opengis.parameter.GeneralParameterValue

/**
 * Takes the Array[GeneralParameterValue] from the read() function of GeoMesaCoverageReader and pulls
 * out the gridGeometry, envelope, height and width, resolution, and bounding box from the query
 * parameters. These are then used to query Accumulo and retrieve out the correct raster information.
 * @param parameters the Array of GeneralParameterValues from the GeoMesaCoverageReader read() function.
 * @param coverageName: the name of the coverage
 */
class GeoMesaCoverageQueryParams(parameters: Array[GeneralParameterValue], coverageName:String) {
  val paramsMap = parameters.map { gpv => (gpv.getDescriptor.getName.getCode, gpv) }.toMap
  val gridGeometry = paramsMap(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString)
                     .asInstanceOf[Parameter[GridGeometry2D]].getValue
  val envelope = gridGeometry.getEnvelope
  val dim = gridGeometry.getGridRange2D.getBounds
  val width = gridGeometry.getGridRange2D.getWidth
  val height = gridGeometry.getGridRange2D.getHeight
  val resX = (envelope.getMaximum(0) - envelope.getMinimum(0)) / width
  val resY = (envelope.getMaximum(1) - envelope.getMinimum(1)) / height
  val accResolution = determineQueryResolution(coverageName, resX, resY)
  val min = Array(Math.max(envelope.getMinimum(0), -180) + .00000001,
                  Math.max(envelope.getMinimum(1), -90) + .00000001)
  val max = Array(Math.min(envelope.getMaximum(0), 180) - .00000001,
                  Math.min(envelope.getMaximum(1), 90) - .00000001)
  val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))

  def toRasterQuery: RasterQuery = RasterQuery(bbox, accResolution, None, None)
  // determine the resolution to use in the query by looking for the hint in the coverageName,
  // and if not present, using the resolution of the request and finding a match in the table
  def determineQueryResolution(coverageName:String,resXRequest:Double, resYRequest:Double): Double = {
    // parse the coverageName for a resolution component
    val coverageNameFORMAT(tableName, resolutionTag) = coverageName
    // if found return the resolution component
    resolutionTag.toDouble
    // if not, select from the request resolutions and take the smallest of the two
  }
  // using the request resolution, find the best match in the table (closest but no lower in resolution to data present
  // in the table
  def matchAvailableResolutions(resXRequest:Double, resYRequest:Double): Double = ???
}
