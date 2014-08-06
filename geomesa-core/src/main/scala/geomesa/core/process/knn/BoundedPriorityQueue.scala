package geomesa.core.process.knn

import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.generic.CanBuildFrom
import scala.collection.GenTraversableOnce

import scala.collection.JavaConverters._

/**
 * Stub for a Bounded Priority Queue
 */
/**
trait BoundedPriorityQueue[T] extends mutable.PriorityQueue[T] {
  def maxSize: Int

  def isFull: Boolean = !(length < maxSize)

  def getLast = take(maxSize).lastOption

  /**
  override def +=(single: T) = {
    if (isFull) injectWithDequeue(single)
    else super.+=(single)

  }
  def injectWithDequeue(single)
    * */
}
**/

class BoundedPriorityQueue[T](val maxSize: Int)(implicit ord: Ordering[T])
  extends Iterable[T] {

  val corePQ = new MinMaxPriorityQueue.Builder[T](ord).maximumSize(maxSize).create[T]()

  override def isEmpty = !(corePQ.size > 0 )

  override def size = corePQ.size

  override def iterator = corePQ.iterator.asScala

  def +=(single: T): BoundedPriorityQueue[T] = {corePQ.add(single) ; this}

  def ++(xs: GenTraversableOnce[T]): BoundedPriorityQueue[T] = { this.clone() ++= xs.seq }

  def enqueue(elems: T*): Unit = { this ++= elems }

  def ++=(xs: GenTraversableOnce[T]):  BoundedPriorityQueue[T] = {
    //corePQ.addAll(xs.toList); this
    xs.foreach {this += _} ; this
  }

  def dequeue() = corePQ.poll()


  def dequeueAll[T1 >: T, That](implicit bf: CanBuildFrom[_, T1, That]): That = {
      val b = bf.apply()
      while (nonEmpty) {
        b += dequeue()
      }
      b.result()
    }

  override def last = corePQ.peekLast

  override def head = corePQ.peek

  def clear(): Unit = corePQ.clear()

  override def toList = this.iterator.toList

    /** This method clones the priority queue.
      *
      *  @return  a priority queue with the same elements.
      */
  override def clone(): BoundedPriorityQueue[T] = new BoundedPriorityQueue[T](maxSize)(ord) ++= this.iterator

  def isFull = !(size < maxSize)

}