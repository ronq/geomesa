package geomesa.utils.geohash

import com.spatial4j.core.context.jts.JtsSpatialContext
import com.vividsolutions.jts.geom._

object WrappingUtils  {
  // default precision model
  val maxRealisticGeoHashPrecision : Int = 45
  val numDistinctGridPoints: Long = 1L << ((maxRealisticGeoHashPrecision+1)/2).toLong
  val defaultPrecisionModel = new PrecisionModel(numDistinctGridPoints.toDouble)

  // default factory for WGS84
  val defaultGeometryFactory : GeometryFactory = new GeometryFactory(defaultPrecisionModel, 4326)


  /**
   * Transforms a geometry with lon in (-inf, inf) and lat in [-90,90] to a geometry in whole earth BBOX
   * 1) any coords of geometry outside lon [-180,180] are transformed to be within [-180,180]
   *    (to avoid spatial4j validation errors)
   * 2) use spatial4j to create a geometry with inferred International Date Line crossings
   *    (if successive coordinates longitudinal difference is greater than 180)
   * Parts of geometries with lat outside [-90,90] are ignored.
   * To represent a geometry with successive coordinates having lon diff > 180 and not wrapping
   * the IDL, you must insert a waypoint such that the difference is less than 180
   */
  def getInternationalDateLineSafeGeometry(targetGeom: Geometry): Geometry = {
    val withinBoundsGeom =
      if (crossesIDL(targetGeom))
        translateGeometry(targetGeom)
      else
        targetGeom

    val shape = JtsSpatialContext.GEO.makeShape(withinBoundsGeom, true, true)
    shape.getGeom
  }

  def crossesIDL(geometry: Geometry): Boolean =
    geometry.getEnvelopeInternal.getMinX < -180 || geometry.getEnvelopeInternal.getMaxX > 180

  def crossesPole(geometry: Geometry): Boolean =
    geometry.getEnvelopeInternal.getMinY < -90 || geometry.getEnvelopeInternal.getMaxY > 90

  def degreesLonTranslation(lon: Double): Double = (((lon + 180) / 360.0).floor * -360).toInt

  /**
   * "unwrap" latitudes to account for pole crossings. Note that the behavior here is very different than
   *  that for degreesLonTranslation
   **/
  def translateLat(lat: Double): Double = math.asin(math.sin(lat.toRadians)).toDegrees

  def translateCoord(coord: Coordinate): Coordinate = {
    new Coordinate(coord.x + degreesLonTranslation(coord.x), coord.y)
  }

  def translatePolygon(geometry: Geometry): Geometry =
    defaultGeometryFactory.createPolygon(geometry.getCoordinates.map(c => translateCoord(c)))

  def translateLineString(geometry: Geometry): Geometry =
    defaultGeometryFactory.createLineString(geometry.getCoordinates.map(c => translateCoord(c)))

  def translateMultiLineString(geometry: Geometry): Geometry = {
    val coords = (0 until geometry.getNumGeometries).map { i => geometry.getGeometryN(i) }
    val translated = coords.map { c => translateLineString(c).asInstanceOf[LineString] }
    defaultGeometryFactory.createMultiLineString(translated.toArray)
  }

  def translateMultiPolygon(geometry: Geometry): Geometry = {
    val coords = (0 until geometry.getNumGeometries).map { i => geometry.getGeometryN(i) }
    val translated = coords.map { c => translatePolygon(c).asInstanceOf[Polygon] }
    defaultGeometryFactory.createMultiPolygon(translated.toArray)
  }

  def translateMultiPoint(geometry: Geometry): Geometry =
    defaultGeometryFactory.createMultiPoint(geometry.getCoordinates.map(c => translateCoord(c)))

  def translatePoint(geometry: Geometry): Geometry = {
    defaultGeometryFactory.createPoint(translateCoord(geometry.getCoordinate))
  }

  def translateGeometry(geometry: Geometry): Geometry = {
    geometry match {
      case p: Polygon =>          translatePolygon(geometry)
      case l: LineString =>       translateLineString(geometry)
      case m: MultiLineString =>  translateMultiLineString(geometry)
      case m: MultiPolygon =>     translateMultiPolygon(geometry)
      case m: MultiPoint =>       translateMultiPoint(geometry)
      case p: Point =>            translatePoint(geometry)
    }
  }
}
