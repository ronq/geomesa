package geomesa.core.process.knn

import geomesa.utils.geohash.GeoHash

case class KNNSearchStatus(numDesired: Int, startingDistance: Double) {
  private var smallestMaxDistance: Double = startingDistance
  private var numFound: Int = 0
  private var visitedGeoHashes: List[String] = List()

  def foundK: Boolean = numFound >= numDesired

  def updateNum(newNum: Int) { numFound += newNum }

  def updateDistance(theNewMaxDistance: Double) {
    if (theNewMaxDistance < smallestMaxDistance) smallestMaxDistance = theNewMaxDistance
  }

  def addGeoHash(gh: GeoHash) {
    visitedGeoHashes = gh.hash :: visitedGeoHashes
  }

  def visited(gh: GeoHash): Boolean = visitedGeoHashes contains gh

  def currentMaxRadius = smallestMaxDistance
}
