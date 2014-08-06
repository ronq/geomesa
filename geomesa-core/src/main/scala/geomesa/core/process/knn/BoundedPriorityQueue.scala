package geomesa.core.process.knn

import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom

/**
 * A simple implementation of a bounded priority queue, as a wrapper for the Guava MinMaxPriorityQueue
 *
 * Some methods have been added to make this appear similar to the Scala collections PriorityQueue
 *
 */

class BoundedPriorityQueue[T](val maxSize: Int)(implicit ord: Ordering[T])
  extends Iterable[T] {

  // note that ord.reverse is used in the constructor -- MinMaxPriorityQueue has natural ordering (min first)
  // while the Scala collections PriorityQueue uses reverse natural ordering (max first)
  val corePQ = MinMaxPriorityQueue.orderedBy(ord.reverse).maximumSize(maxSize).create[T]()

  override def isEmpty = !(corePQ.size > 0 )

  override def size = corePQ.size

  def iterator = corePQ.iterator.asScala

  def +=(single: T)= {corePQ.add(single) ; this}

  def ++(xs: GenTraversableOnce[T]) = { this.clone() ++= xs.seq }

  def enqueue(elems: T*) = { this ++= elems }

  def ++=(xs: GenTraversableOnce[T]) = {
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

  def clear() = corePQ.clear()

  override def toList = this.iterator.toList

  override def clone() = new BoundedPriorityQueue[T](maxSize)(ord) ++= this.iterator

  def isFull = !(size < maxSize)

}