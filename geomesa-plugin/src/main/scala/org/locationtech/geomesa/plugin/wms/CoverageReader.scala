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

package org.locationtech.geomesa.plugin.wms

import java.awt.image.BufferedImage
import java.awt.{AlphaComposite, Color, Graphics2D, Rectangle}
import java.util.{Date, List => JList}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{IteratorSetting, Scanner, ZooKeeperInstance}
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.{AbstractGridCoverage2DReader, AbstractGridFormat}
import org.geotools.coverage.grid.{GridCoverage2D, GridEnvelope2D, GridGeometry2D}
import org.geotools.geometry.GeneralEnvelope
import org.geotools.parameter.Parameter
import org.geotools.util.{DateRange, Utilities}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.iterators.{AggregatingKeyIterator, SurfaceAggregatingIterator, TimestampRangeIterator, TimestampSetIterator}
import org.locationtech.geomesa.core.util.{BoundingBoxUtil, SelfClosingBatchScanner}
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds, GeoHash, TwoGeoHashBoundingBox}
import org.opengis.geometry.Envelope
import org.opengis.parameter.{GeneralParameterValue, InvalidParameterValueException}

import scala.collection.JavaConversions._
import scala.util.Random


object CoverageReader {
  val GeoServerDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val DefaultDateString = GeoServerDateFormat.print(new DateTime(DateTimeZone.forID("UTC")))
}

import org.locationtech.geomesa.plugin.wms.CoverageReader._

class CoverageReader(val url: String) extends AbstractGridCoverage2DReader() with Logging {

  logger.debug(s"""creating coverage reader for url "${url.replaceAll(":.*@", ":********@").replaceAll("#auths=.*","#auths=********")}"""")

  val FORMAT = """accumulo://(.*):(.*)@(.*)/(.*)#columns=(.*)#resolution=([0-9]*)#zookeepers=([^#]*)(?:#auths=)?(.*)$""".r
  val FORMAT(user, password, instanceId, table, columnsStr, resolutionStr, zookeepers, authtokens) = url

  logger.debug(s"extracted user $user, password ********, instance id $instanceId, table $table, columns $columnsStr, " +
               s"resolution $resolutionStr, zookeepers $zookeepers, auths ********")

  coverageName = table + ":" + columnsStr
  val columns = columnsStr.split(",").map(_.split(":").take(2) match {
    case Array(columnFamily, columnQualifier, _) => (columnFamily, columnQualifier)
    case Array(columnFamily) => (columnFamily, "")
    case _ =>
  })
  val metaRows = columns.map{
    case (columnFamily, columnQualifier) => new Text("~" + columnFamily + "~" + columnQualifier)
  }

  this.crs = AbstractGridFormat.getDefaultCRS
  this.originalEnvelope = new GeneralEnvelope(Array(-180.0, -90.0), Array(180.0, 90.0))
  this.originalEnvelope.setCoordinateReferenceSystem(this.crs)
  this.originalGridRange = new GridEnvelope2D(new Rectangle(0, 0, 1024, 512))
  this.coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(this.hints)

  val zkInstance = new ZooKeeperInstance(instanceId, zookeepers)
  val connector = zkInstance.getConnector(user, new PasswordToken(password.getBytes))

  // When parsing an old-form Accumulo layer URI the authtokens field matches the empty string, requesting no authorizations
  val auths = new Authorizations(authtokens.split(","): _*)

  val aggPrefix = AggregatingKeyIterator.aggOpt

  lazy val metaData: Map[String,String] = {
    val scanner: Scanner = connector.createScanner(table, auths)
    for(metaRow <- metaRows) {
      scanner.setRange(new org.apache.accumulo.core.data.Range(metaRow))
    }
    scanner.iterator()
    .map(entry => (entry.getKey.getColumnFamily.toString, entry.getKey.getColumnQualifier.toString))
    .toMap
  }

  /**
   * Default implementation does not allow a non-default coverage name
   * @param coverageName
   * @return
   */
  override protected def checkName(coverageName: String) = {
    Utilities.ensureNonNull("coverageName", coverageName)
    true
  }

  override def getFormat = new CoverageFormat

  def toTimestampString(date: Date) = java.lang.Long.toString(date.getTime/1000)

  def getGeohashPrecision = resolutionStr.toInt

  def read(parameters: Array[GeneralParameterValue]): GridCoverage2D = {
    val paramsMap = parameters.map(gpv => (gpv.getDescriptor.getName.getCode, gpv)).toMap
    val gg = paramsMap(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString).asInstanceOf[Parameter[GridGeometry2D]].getValue
    val env = gg.getEnvelope

    val timeParam: Option[Either[Date, DateRange]] =
      parameters
        .find { _.getDescriptor.getName.getCode == AbstractGridFormat.TIME.getName.toString }
        .flatMap { case p: Parameter[JList[AnyRef]] => p.getValue.lift(0) }
        .map {
          case date: Date => Left(date)
          case dateRange: DateRange => Right(dateRange)
          case x => throw new InvalidParameterValueException(s"Invalid value for parameter TIME: ${x.toString}", "TIME", x)
        }

    val tile = getImage(timeParam, env, gg.getGridRange2D.getSpan(0), gg.getGridRange2D.getSpan(1))
    this.coverageFactory.create(coverageName, tile, env)
  }


  def getImage(timeParam: Option[Either[Date, DateRange]], env: Envelope, xDim:Int, yDim:Int) = {
    val min = Array(Math.max(env.getMinimum(0), -180) + .00000001, Math.max(env.getMinimum(1), -90) + .00000001)
    val max = Array(Math.min(env.getMaximum(0), 180) - .00000001, Math.min(env.getMaximum(1), 90) - .00000001)
    val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))
    val ghBbox = TwoGeoHashBoundingBox(bbox,getGeohashPrecision)
    val xdim = math.max(1, math.min(xDim, math.round(ghBbox.bbox.longitudeSize / ghBbox.ur.bbox.longitudeSize - 1).toInt))
    val ydim = math.max(1, math.min(yDim, math.round(ghBbox.bbox.latitudeSize / ghBbox.ur.bbox.latitudeSize - 1).toInt))

    val bufferList: List[Array[Byte]] =
      getScanBuffers(bbox, timeParam, xdim, ydim).map(_.getValue.get()).toList ++ List(Array.ofDim[Byte](xdim*ydim))
    val buffer = bufferList.reduce((a, b) => {
      for (i <- 0 to a.length - 1) {
        a(i) = math.max(a(i) & 0xff, b(i) & 0xff).asInstanceOf[Byte]
      }
      a
    })
    ImageUtils.drawImage(Array(buffer),xdim, ydim)
  }

  def getScanBuffers(bbox: BoundingBox, timeParam: Option[Either[Date, DateRange]], xDim:Int, yDim:Int) = {
    val scanner = connector.createBatchScanner(table, auths, 10)
    columns.foreach{ case (cf: String, cq: String) => scanner.fetchColumn(new Text(cf), new Text(cq))}

    val ranges = BoundingBoxUtil.getRangesByRow(BoundingBox.getGeoHashesFromBoundingBox(bbox))
    scanner.setRanges(ranges)

    timeParam match {
      case Some(Left(date)) =>
        TimestampSetIterator.setupIterator(scanner, date.getTime / 1000)
      case Some(Right(dateRange)) =>
        val startDate = dateRange.getMinValue
        val endDate = dateRange.getMaxValue
        TimestampRangeIterator.setupIterator(scanner, startDate, endDate)
      case None =>
        val name = "version-" + Random.alphanumeric.take(5).mkString
        val cfg = new IteratorSetting(2, name, classOf[VersioningIterator])
        VersioningIterator.setMaxVersions(cfg, 1)
        scanner.addScanIterator(cfg)
    }

    AggregatingKeyIterator.setupAggregatingKeyIterator(scanner,
                                                       1000,
                                                       classOf[SurfaceAggregatingIterator],
                                                       Map[String,String](aggPrefix + "bottomLeft" -> GeoHash(bbox.ll, getGeohashPrecision).hash,
                                                                          aggPrefix + "topRight" -> GeoHash(bbox.ur,getGeohashPrecision).hash,
                                                                          aggPrefix + "precision" -> getGeohashPrecision.toString,
                                                                          aggPrefix + "dims" -> (xDim +","+yDim)))

    SelfClosingBatchScanner(scanner)
  }

  def getEmptyImage = {
    val emptyImage = new BufferedImage(256, 256, BufferedImage.TYPE_4BYTE_ABGR)
    val g2D = emptyImage.getGraphics.asInstanceOf[Graphics2D]
    val save = g2D.getColor
    g2D.setColor(Color.WHITE)
    g2D.setComposite(AlphaComposite.Clear)
    g2D.fillRect(0, 0, emptyImage.getWidth, emptyImage.getHeight)
    g2D.setColor(save)
    emptyImage
  }

  val LOG180 = math.log(180.0)
  val LOG2 = math.log(2)

  def fromBoundingBox(minY: Double, maxY: Double) =
    math.round((LOG180 - math.log(maxY - minY)) / LOG2).intValue()

  import org.geotools.coverage.grid.io.GridCoverage2DReader._

  override def getMetadataNames: Array[String] = Array[String](TIME_DOMAIN, HAS_TIME_DOMAIN)

  override def getMetadataValue(name: String): String = name match{
    case TIME_DOMAIN => {
      // fetch the list, formatted for GeoServer, of all of the date/times
      // for which the current Accumulo surface is available
      // (NB:  that this should be a list is dictated by the code that
      // originally registered the surface with GeoServer)

      // short-cut:  each of the surface-dates will have a separate "count"
      // entry among the metadata; this provides a single list of contiguous
      // entries to scan for timestamps
      val scanner: Scanner = connector.createScanner(table, auths)
      scanner.setRange(new org.apache.accumulo.core.data.Range("~METADATA"))
      columns.foreach{ case (cf: String, _) => scanner.fetchColumn(new Text(cf), new Text("count"))}

      val dtListString =
        scanner
          .iterator()
          .map(entry => entry.getKey.getTimestamp * 1000L)
          .map(millis => new DateTime(millis, DateTimeZone.forID("UTC")))
          .map(dt => GeoServerDateFormat.print(dt))
          .toList
          .distinct
          .mkString(",")

      // ensure that at least one (albeit, dummy) date is returned
      if (dtListString.trim.length < 1) DefaultDateString else dtListString
    }
    case HAS_TIME_DOMAIN => "true"
    case  _ => null
  }
}
