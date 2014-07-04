package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Geometry
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

case class GeodeticDistanceCalc(referencePoint:Geometry) {
  val crs = sridToCRS(referencePoint.getSRID)
  val firstCoords = referencePoint.getCoordinate
  def sridToCRS(sridInt: Int) = CRS.decode(s"EPSG:$sridInt")
  def computeDistanceInMeters(newPoint: Geometry):Double = JTS.orthodromicDistance(firstCoords, newPoint.getCoordinate, crs)
}
