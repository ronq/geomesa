package geomesa.core.process.knn

import com.vividsolutions.jts.geom.Geometry
import geomesa.core.process.knn
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.collection.SortedSimpleFeatureCollection
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.{CRS, GeodeticCalculator}
import org.geotools.util.NullProgressListener
import org.opengis.filter.expression.PropertyName
import org.opengis.filter.sort.{SortOrder, SortBy}

object NearestNeighbors {

      def apply(inputCollection: SimpleFeatureCollection, geodeticDistanceCalc: GeodeticDistanceCalc) = {
        val mutatedCollection = inputCollection.accepts(knn.geodeticVisitor(geodeticDistanceCalc),new NullProgressListener() )
        val nearestDistanceSorting =  Array[SortBy]{ff.sort("geodeticDistance", SortOrder.DESCENDING )}

        new NearestNeighbors(inputCollection,)
      }
}


class NearestNeighbors(inputCollection: SimpleFeatureCollection, geodeticSorting: Array[SortBy]) extends SortedSimpleFeatureCollection(inputCollection: SimpleFeatureCollection, geodeticSorting: Array[SortBy]) {

}
