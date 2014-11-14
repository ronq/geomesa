/*
* Copyright 2014-2014 Commonwealth Computer Research, Inc.
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

import java.util.{List, Date}
import javax.swing.JList

import org.geotools.coverage.grid.GridGeometry2D
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.util.DateRange
import org.opengis.geometry.Envelope
import org.opengis.parameter.{InvalidParameterValueException, GeneralParameterValue}
import org.geotools.parameter.Parameter

trait GeneralRequest {
   def geomParam: Envelope
   def dtgParam: Option[Either[Date, DateRange]]
}



class WCS10Request(requestParameters: Array[GeneralParameterValue]) extends GeneralRequest {
    // generate a map from the array of parameters
    val paramsMap = requestParameters.map(gpv => (gpv.getDescriptor.getName.getCode, gpv)).toMap

    // extract the geometry of the request
    val gridGeoOption = paramsMap.get( AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString )
                                 .map (
                                    _.asInstanceOf[Parameter[GridGeometry2D]]
                                     .getValue
                                     .getEnvelope )
    val geomParam = gridGeoOption.getOrElse ( throw new IllegalArgumentException("Invalid geometry in request" ))


    def IPVE(unknownParameter: Parameter[Any]): InvalidParameterValueException = {
      val message = s"Invalid value for parameter TIME: ${unknownParameter.toString}"
      new InvalidParameterValueException(message, "TIME", unknownParameter)
    }

    val dtgParam = paramsMap
                        .get ( AbstractGridFormat.TIME.getName.toString )
                        .map {
                               case dateParameter: Parameter[Date]           => Left ( dateParameter.getValue )
                               case dateRangeParameter: Parameter[DateRange] => Right ( dateRangeParameter.getValue )
                               case other: Parameter[Any]                    => throw IPVE(other)
                        }
}


object RasterRequest {
  def apply(requestParameters: Array[GeneralParameterValue]): GeneralRequest = {
    // case matching to get the proper version of parameters (may not be needed)
    new WCS10Request(requestParameters: Array[GeneralParameterValue])
  }
}


