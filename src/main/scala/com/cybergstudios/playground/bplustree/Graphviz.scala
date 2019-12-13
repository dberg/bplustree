package com.cybergstudios.playground.bplustree

/**
  * dot -Tpng /tmp/test.dot > /tmp/test.png && open /tmp/test.png
  *
  * digraph {
  *
  * "30_1" [label=30];
  * "50_1" [label=50];
  * "10_2" [label=10 color=grey];
  * "30_2" [label=30 color=grey];
  * "50_2" [label=50 color=grey];
  * "20_2" [label=20 color=grey];
  * "40_2" [label=40 color=grey];
  * "60_2" [label=60 color=grey];
  * "70_2" [label=70 color=grey];
  *
  * { rank=same; "30_1" "50_1" }
  * { rank=same; "10_2" "30_2" "50_2" }
  * { rank=same; "20_2" "40_2" "60_2" }
  * { rank=same; "70_2" }
  *
  * "30_1" -> "10_2";
  * "30_1" -> "30_2";
  * "50_1" -> "50_2";
  *
  * subgraph {
  *   "30_1" -> "50_1" [color=grey arrowhead=none];
  *   "10_2" -> "20_2" [color=grey arrowhead=none];
  *   "30_2" -> "40_2" [color=grey arrowhead=none];
  *   "50_2" -> "60_2" [color=grey arrowhead=none];
  *   "60_2" -> "70_2" [color=grey arrowhead=none];
  * }
  *
  * }
  */
object Graphviz {

  // Vertices: Key_Level [label=Key]. If Leaf node add attribute color=grey.
  // Ranks: Rank by Level. If Leaf node Level + Vector Position since we stack them vertically.
  // Edges: Internal to Children.
  // Subgraph: Edges connecting the Leaf nodes representing the Vector.
  def buildGraph[K, V](tree: BPlusTree[K, V]): String = {
    val data = traverse(tree.root, 1, TreeData.empty[K, V])

    val vertices = data.vertices.foldLeft(new StringBuilder) { (b, v) =>
      b.append(s"""  "${v.id}" [ ${v.properties.mkString(" ")} ];\n""")
    }.toString()

    val ranks = data.vertices.groupBy(e => e.level).foldLeft(new StringBuilder) { case (b, (_, vs)) =>
      val list = vs.map(v => s""""${v.id}"""").mkString(" ")
      b.append(s"""  { rank=same; $list }\n""")
    }.toString()

    val edges = data.edges.foldLeft(new StringBuilder) { (b, e) =>
      b.append(s"""  "${e.from.id}" -> "${e.to.id}";\n""")
    }.toString()

    val subgraph = data.subgraph.foldLeft(new StringBuilder) { (b, e) =>
      b.append(s"""    "${e.from.id}" -> "${e.to.id}" [ color=grey arrowhead=none ];\n""")
    }

    val output =
      s"""
        |digraph {
        |
        |$vertices
        |
        |$ranks
        |
        |$edges
        |
        |  subgraph {
        |
        |$subgraph
        |  }
        |
        |}""".stripMargin

    output
  }

  private def traverse[K, V](
    node: Node[K, V],
    level: Int,
    data: TreeData[K, V]): TreeData[K, V] = node match
  {
    case Leaf(leaves) =>
      val levels = (0 until leaves.length).toList
      val vertices = (levels zip leaves)  map { case (l, entry) =>
        val id = s"${entry.k}_$level"
        val properties = List(Property("label", entry.k.toString), Property("color", "grey"))
        val vertexLevel = l + level // we stack leaves vertically
        Vertex(id, properties, vertexLevel)
      }
      val subgraph = makeSubgraph(vertices)
      data.copy(vertices = data.vertices ++ vertices, subgraph = data.subgraph ++ subgraph)
    case Internal(keys, children) =>
      val vertices = keys.toList.map(k => Vertex(s"${k}_$level", List(Property("label", k.toString)), level))
      val subgraph = makeSubgraph(vertices)
      val verticesKeys: List[Vertex] = keys.map(k => Vertex(makeVertexId(k, level), List.empty, level)).toList
      val verticesChildren: List[Vertex] = children.map(n => Vertex(makeVertexIdFromNode(n, level + 1), List.empty, level + 1)).toList
      // TODO: unsafe assumption verticesChildren.length - verticesKeys.length == 1
      val edges: List[Edge] = Edge(verticesKeys(0), verticesChildren(0)) ::
        ((verticesKeys zip verticesChildren.tail) map { case (from, to) => Edge(from, to) })
      val updatedData: TreeData[K, V] = data.copy(vertices = data.vertices ++ vertices, data.edges ++ edges, data.subgraph ++ subgraph)
      children.foldLeft(updatedData) { case (dataAcc, node) =>
        traverse(node, level + 1, dataAcc)
      }
  }

  private def makeSubgraph(vertices: List[Vertex]): List[Edge] = {
    if (vertices.length <= 1) List.empty[Edge]
    else (vertices zip vertices.tail) map { case (from, to) => Edge(from, to) }
  }

  private def makeVertexId[K](k: K, level: Int): String = s"${k.toString}_$level"

  private def makeVertexIdFromNode[K, V](node: Node[K, V], level: Int): String = node match {
    case Leaf(leaves) => makeVertexId(leaves(0).k, level) // TODO: unsafe
    case Internal(keys, _) => makeVertexId(keys(0), level) // TODO: unsafe
  }
}

case class TreeData[K, V](vertices: List[Vertex], edges: List[Edge], subgraph: List[Edge])

object TreeData {
  def empty[K, V](): TreeData[K, V] = TreeData(List.empty, List.empty, List.empty)
}

case class Vertex(id: String, properties: List[Property], level: Int)

case class Edge(from: Vertex, to: Vertex)

case class Property(k: String, v: String) {
  override def toString(): String = s"$k=$v"
}
