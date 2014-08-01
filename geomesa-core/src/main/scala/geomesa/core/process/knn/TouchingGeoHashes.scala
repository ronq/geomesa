package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Coordinate
import geomesa.utils.geohash.GeoHash

/**
 * This object provides a method for obtaining a set of GeoHashes which are in "contact" with a seed GeoHash.
 * Sets are used throughout to avoid duplication.
 *
 * These methods exploit the symmetry of GeoHashes to ensure that they are both antimeridian (aka Internaional Data Line)
 * and "pole" safe. For the latter, any GeoHash that touches the pole has all such GeoHashes as "touching" neighbors
 */

object TouchingGeoHashes {

  // generates directions for stepping N,NE,E, etc...
  val shiftPattern = for {
      i <- Set(-1, 0, 1)
      j <- Set(-1, 0, 1)
    } yield new Coordinate(i, j)

  // generates a set of all touching neighbors
  def touching(gh:GeoHash): Set[GeoHash] = {

    val thisLocVector = gh.getPoint.getCoordinate

    val thisPrec = gh.prec

    val thisPrecVector = new Coordinate (GeoHash.longitudeDeltaForPrecision(thisPrec),
      GeoHash.latitudeDeltaForPrecision(thisPrec))

    // these are actually coordinate candidates, some may be over the poles or past the antimeridian
    val newCoords = for {
      jumpUnitVector <- shiftPattern
      newX = thisLocVector.x + (jumpUnitVector.x * thisPrecVector.x)
      newY = thisLocVector.y + (jumpUnitVector.y * thisPrecVector.y)
    } yield new Coordinate(newX,newY)

    // now process the coordinate candidates, treating both the antimeridian and the polar regions
    val safeCoords = newCoords.flatMap { generateIDLSafeCoordinates }.flatMap
                                                      { generatePolarSafeCoordinates(_,thisPrecVector,thisLocVector) }
    // use the now safe coordinates to generate GeoHashes, removing the seed GeoHash

    val newGH = safeCoords.map{ coord => GeoHash(coord.x,coord.y,thisPrec)}.filter{_ != gh}
    println {gh.hash + " -- " + newGH.map{_.hash}}
    newGH
  }

  // handles cases where the seed geohash is in contact with the antimeridian
  def generateIDLSafeCoordinates(xyPair: Coordinate): Set[Coordinate]  =
             if (math.abs(xyPair.x) > 180.0) generateIDLMirrorPair(xyPair) else Set(xyPair)

  // handles cases where the seed geohash is in contact with a pole
  def generatePolarSafeCoordinates(xyPair: Coordinate,
                                   precVector:Coordinate,
                                   startingPair:Coordinate ): Set[Coordinate] =
             if (math.abs(xyPair.y) > 90.0) polarCap(xyPair, precVector,startingPair) else Set(xyPair)

  // use symmetry about the antimeridian to find the correct coordinate on the other side
  def generateIDLMirrorPair(xyPair:Coordinate): Set[Coordinate] = {
      val newLat= xyPair.y
      val newLon= xyPair.x + degreesLonTranslation(xyPair.x)
      Set(new Coordinate (newLon,newLat))
  }

  // generate a Set of GeoHashes which all touch this pole, at the same precision as the seed
  def polarCap(xyPair: Coordinate,precVector: Coordinate,startingPair: Coordinate): Set[Coordinate] = {
    val newLat = startingPair.y
    val begin: Double = -180.0 + (0.5 * precVector.x)
    val end: Double = 180.0 - (0.5 * precVector.x)
    val step: Double = precVector.x
    val newLons = begin to(end, step)
    newLons.map(newLon => new Coordinate(newLon, newLat)).toSet
  }

  // taken from inside  getInternationalDateLineSafeGeometry
  // FIXME refactor the above to expose the method
  def degreesLonTranslation(lon: Double): Double = (((lon + 180) / 360.0).floor * -360).toInt
}




