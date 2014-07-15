package geomesa.core.process.knn

import com.vividsolutions.jts.geom.{Geometry, GeometryFactory}
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.collection.AbstractFeatureVisitor
import org.geotools.feature.visitor.{FeatureAttributeVisitor, CalcResult, FeatureCalc}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter
import geomesa.core.process.knn

import util.Try

/**
 * Created by ronquest on 7/3/14.
 */
class GeodeticVisitor(geodeticCalculator: GeodeticDistanceCalc) extends AbstractFeatureVisitor{
    private val log = Logger.getLogger(classOf[GeodeticVisitor])
    def visit(feature: Feature): Unit = {
      val sf = feature.asInstanceOf[SimpleFeature]
      val distance =
        geodeticCalculator.computeDistanceInMeters(sf.getDefaultGeometryProperty.getValue.asInstanceOf[Geometry])
      Try{sf.setAttribute(REFERENCE_POINT_ATTRIBUTE, geodeticCalculator.referencePoint )}
      Try{sf.setAttribute(GEODETIC_DISTANCE_ATTRIBUTE, distance)}
    }

}