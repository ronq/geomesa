package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Point
import geomesa.utils.geohash.GeoHash
import breeze.linalg._

// FIXME: type alias for DenseVector[Double] and List[DenseVector[Double]]


object TouchingGeoHashes {
  val shiftPattern = for {
      i <- List(-1, 0, 1)
      j <- List(-1, 0, 1)
    } yield DenseVector[Double](i, j)
  def touching(gh:GeoHash): Seq[GeoHash] = {
    val thisLoc = gh.getPoint
    val thisLocVector = DenseVector[Double](thisLoc.getX,thisLoc.getY)
    val thisPrec = gh.prec
    val thisLatPrec = GeoHash.latitudeDeltaForPrecision(thisPrec)
    val thisLonPrec = GeoHash.longitudeDeltaForPrecision(thisPrec)
    val thisPrecVector = DenseVector[Double](thisLonPrec,thisLatPrec)
    val newCoords = shiftPattern.map {jumpUnitVector => thisLocVector + (jumpUnitVector:* thisPrecVector :* 2.0)}


    for {
      xyPair <- newCoords
      safePair <- generateSafeCoordinates(xyPair)
      // extract coordinates from safe pair back to Points
      newGH <- GeoHash(safePair)
    } yield {newGh}.toSeq
  }
  def generateIDLSafeCoordinates(xyPair: DenseVector[Double]): List[DenseVector[Double]] = { }
             if (math.abs(xyPair(0)) > 180.0) generateIDLMirrorPair() else List(xyPair)
  def generatePolarSafeCoordinates(xyPair: DenseVector[Double] ,
                                   precVector:DenseVector[Double] ,
                                   startingPair:DenseVector[Double] ): List[DenseVector[Double]] = {
             if (math.abs(xyPair(1)) > 90.0) polarCap() else List(xyPair)
  }
  def generateIDLMirrorPair(xyPair:DenseVector[Double]): List[DenseVector[Double]] = {
      val newLat= xyPair(1)
      val newLon= xyPair(0) + degreesLonTranslation(xyPair(0))
      List(DenseVector[Double] (newLon,newLat))
  }
  def generatePolarSafeCoordinates(xyPair, :DenseVector[Double],
                                   precVector:DenseVector[Double] ,
                                   startingPair:DenseVector[Double] )   = {
      val newLat = startingPair(1)  // should be the original
      val newLons = NumericRange(-180 + precVector(0), 180.- precVector(0), 2.* precVector(0)) // should be the centers of all GHs in lat
      //val allLongtitudes = NumericRange(-180 + lonDelta, 180.- lonDelta, 2.* lonDelta)
  }
}

  // taken from inside  getInternationalDateLineSafeGeometry
  // FIXME refactor the above to expose the method
  def degreesLonTranslation(lon: Double): Double = (((lon + 180) / 360.0).floor * -360).toInt

}


    //val newCoords = shiftPattern.map {jumpUnitVector => safeJump(jumpUnitVector, thisLonPrec, thisLatPrec, thisLoc) }

    //val newCoords = shiftPattern.map { jumpVector => safeJump(2.0 * thisLonPrec * x, 2.0 * thisLatPrec * y)}
  }



  def safeJump(jumpUnitVector: DenseVector[Double], precVector: DenseVector[Double] , oldLocation: DenseVector[Double]) = {
    val scaledJumpVector = jumpUnitVector :* precVector :* 2.0
    val newLocation = oldLocation + scaledJumpVector

}

object OldTouchingMethod {
  // return all GeoHashes of the same precision that are in contact with this one,
  def touching(gh: GeoHash): Seq[GeoHash] = {
    val thisLoc = gh.getPoint
    val thisPrec = gh.prec
    val thisLatPrec = GeoHash.latitudeDeltaForPrecision(thisPrec)
    val thisLonPrec = GeoHash.longitudeDeltaForPrecision(thisPrec)
    // get the precision (is lat and lon degrees the same?)
    // get the GeoHashes that lie -1<x<1 and -1<y<1 from this one,
    // treating the bonds at the poles and antimeridian correctly
    // this needs to filter out the (0,0) case
    val shiftPattern = for {
      i <- List(-1, 0, 1)
      j <- List(-1, 0, 1)
    } yield (i, j)
    // get a list of new coordinates
    // NOTE THAT BLINDY JUMPING BY TWICE THE PRECISION IS NOT GOING TO PLACE
    // the position squarely into the next geohash.
    // I need to check that there are no overshoots.
    val newCoordShifts = shiftPattern.map { shift => (2.0 * thisLonPrec * x, 2.0 * thisLatPrec * y)}
    // apply the coordinate shifts to the center point
    // if there are any that extend to the pole, I need to wrap around the pole completely
    // by taking the original latitude and cycling through all longs
    // if there are any that jump over the antimeridian, I need to
    // just flip the sign of the longitude.

    def handlePoles(newPoint: Point): List[Point] = {
      if poleCrossing(newPoint) {
        generatePoleWrap(thisLoc, thisPrec) // we actually don't care if we've also jumped over the meridian in this case
      }
      else if antimeridianCrossing(newPoint)
      else
        List(newPoint)
    }
    def poleCrossing(aPoint: Point): Boolean = math.abs(aPoint.getY) > 90
    def antimeridianCrossing(aPoint: Point): Boolean = math.abs(aPoint.getX) > 180
    def generatePoleWrap(aPoint: Point, aPrec: Int): List[Point] = {
      val aLat = aPoint.getY // extract  the latitude of the original point -- it will be used for the wrap
      val lonDelta = GeoHash.lonDeltaMap(aPrec) // get a array of all longitude centers
      //powersOf2Map(aPrec)//
      // I need to confirm that this works.
      val allLongtitudes = NumericRange(-180 + lonDelta, 180.- lonDelta, 2.* lonDelta)
      // generate the list
    }