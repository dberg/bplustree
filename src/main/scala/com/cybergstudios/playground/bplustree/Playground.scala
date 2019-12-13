package com.cybergstudios.playground.bplustree

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.annotation.tailrec
import scala.util.Random

object Playground {

  def main(args: Array[String]): Unit = {
    //val btree = buildTree()
    val btree = buildRandomTree(100, 1024, BPlusTree[Int, String](3))
    println(btree)

    val dot = Graphviz.buildGraph(btree)
    println(dot)

    val path = "/tmp/tree.dot"
    Files.write(Paths.get(path), dot.getBytes(StandardCharsets.UTF_8))

    println(s"The dot has been written to $path")
    println(s"To render and visualize as a PNG file you can run")
    println("dot -Tpng /tmp/tree.dot > /tmp/tree.png && open /tmp/tree.png")
  }

  def buildTree(): BPlusTree[Int, String] = {
    val btree = BPlusTree[Int, String](3)
      .insert(10, "A")
      .insert(20, "B")
      .insert(30, "C")
      .insert(40, "D")
      .insert(50, "E")
      .insert(60, "F")
      .insert(70, "G")
    btree
  }

  @tailrec
  def buildRandomTree(count: Int, maxValue: Int, tree: BPlusTree[Int, String]): BPlusTree[Int, String] = {
    if (count <= 0) tree
    else buildRandomTree(
      count - 1,
      maxValue,
      tree.insert(Random.nextInt(maxValue), Random.nextString(5))
    )
  }

}
