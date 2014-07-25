package geomesa.core.process.knn

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GenerateKNNQueryTest extends Specification {

    val newQuery = generateKNNQuery(newGH, query, source)


   sequential
   /**
      nonAccumulo.size should be equalTo 8
      val prox = new KNearestNeighborSearchProcess
      prox.execute(inputFeatures, nonAccumulo, 5, 30, 5000.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 5, 98,5000.0).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 5, 99.0001,5000.0).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 5, 100,5000.0).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 5, 101,5000.0).size should be equalTo 6
    **/


 }
