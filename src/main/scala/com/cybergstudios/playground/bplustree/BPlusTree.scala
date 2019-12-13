package com.cybergstudios.playground.bplustree

import scala.annotation.tailrec
import scala.reflect.ClassTag

/** This is B+Tree where the data is in the leaf nodes.
  * The max number of keys are determined by `ncount`. The max number of leaves
  * follows ncount + 1.
  */
case class BPlusTree[K : Ordering : ClassTag, V](ncount: Int, root: Node[K, V])
{
  def find(k: K): Option[V] = None

  def insert(k: K, v: V): BPlusTree[K, V] = {
    val node = insertH(root, Entry(k, v))
    BPlusTree[K, V](ncount, node)
  }

  override def toString(): String = s"B+Tree(${root.toString})"

  /** Given a Leaf node:
    *
    *   If there's space for the entry we find its place and return a new Leaf node.
    *   If there's no space we split it into two Leaf nodes and tying it together
    *   under an Internal node.
    *
    * Given an Internal node:
    *
    *   We find the bucket and recurse to insert the entry. If we get back a Leaf
    *   node we're done.
    *
    *   On the other hand, if get back an Internal node we merge it with our current
    *   internal node and split it if have keys.size > ncount.
    */
  private def insertH(node: Node[K, V], entry: Entry[K, V]): Node[K, V] = node match {
    case Leaf(leaves) =>
      // create new array of leaves ordered by k.
      // If k exists in the current leaves the new one overwrites the old one.
      val newLeaves: Vector[Entry[K, V]] = (leaves.filter(_.k != entry.k) ++ Vector(entry)).sortBy(_.k)
      if (newLeaves.length <= ncount + 1) Leaf(newLeaves)
      else splitLeaf(newLeaves)
    case i1 @ Internal(keys, children) =>
      val pos = findBucket(entry.k, keys, 0, implicitly[Ordering[K]])
      insertH(children(pos), entry) match {
        case l @ Leaf(_) =>
          Internal(keys, children.updated(pos, l))
        case i2 @ Internal(_, _) =>
          val merged = merge(i1, i2, pos, implicitly[Ordering[K]])
          if (merged.keys.length <= ncount) merged
          else splitInternal(merged)
      }
  }

  @tailrec
  private def findBucket(k: K, keys: Vector[K], pos: Int, o: Ordering[K]): Int = keys match {
    case Vector() => pos
    case Vector(key, _*) if o.gteq(k, key) => findBucket(k, keys.tail, pos + 1, o)
    case _ => pos
  }

  private def merge(i1: Internal[K, V], i2: Internal[K, V], pos: Int, o: Ordering[K]): Internal[K, V] = {
    val keys =
      if (pos > 0) i1.keys.slice(0, pos) ++ i2.keys ++ i1.keys.slice(pos, i1.keys.length)
      else i2.keys ++ i1.keys

    val children =
      if (pos > 0) i1.children.slice(0, pos) ++ i2.children ++ i1.children.slice(pos + 1, i1.children.length)
      else i2.children ++ i1.children.slice(1, i1.children.length)

    Internal(keys, children)
  }

  /** The assumption is that leaves.size == ncount + 2 */
  private def splitLeaf(leaves: Vector[Entry[K, V]]): Internal[K, V] = {
    val (left, right) = leaves.splitAt(leaves.length / 2)
    val key = right(0).k
    Internal(Vector(key), Vector(Leaf(left), Leaf(right)))
  }

  /** The assumption is that i.keys.size > ncount */
  private def splitInternal(i: Internal[K, V]): Internal[K, V] = {
    val keySplit: (Vector[K], Vector[K]) = i.keys.splitAt(i.keys.length / 2)
    val (keyLeft: Vector[K], keyInternal: K, keyRight: Vector[K]) = keySplit match {
      case (x, Vector(h, tail @ _*)) => (x, h, tail)
      // TODO: unsafe case below
    }

    val (childrenLeft, childrenRight) = i.children.splitAt((i.keys.length / 2) + 1)

    val left = Internal[K, V](keyLeft, childrenLeft)
    val right = Internal[K, V](keyRight, childrenRight)

    Internal[K, V](Vector(keyInternal), Vector(left, right))
  }
}

object BPlusTree {
  def apply[K : Ordering : ClassTag, V](ncount: Int): BPlusTree[K, V] =
    BPlusTree(ncount, Leaf[K, V](Vector()))
}

sealed trait Node[K, V]

case class Internal[K : Ordering, V](keys: Vector[K], children: Vector[Node[K, V]]) extends Node[K, V] {
  override def toString(): String = s"Internal((${keys.mkString(",")}), (${children.mkString(",")}))"
}

case class Leaf[K : Ordering, V](leaves: Vector[Entry[K, V]]) extends Node[K, V] {
  override def toString(): String = s"Leaf(${leaves.mkString(",")})"
}

case class Entry[K : Ordering, V](k: K, v: V)

