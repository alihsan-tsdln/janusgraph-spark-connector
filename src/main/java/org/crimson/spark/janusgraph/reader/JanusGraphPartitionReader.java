package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import scala.collection.JavaConverters;
import scala.jdk.CollectionConverters;

import java.io.IOException;
import java.util.*;

public class JanusGraphPartitionReader implements PartitionReader<InternalRow> {
    private InputPartition inputPartition;
    GraphTraversal<Edge, Edge> iterator;

    List<String> fieldNames;

    public JanusGraphPartitionReader(InputPartition inputPartition, Object[] fieldNames, JanusGraph graph) {
        this.inputPartition = inputPartition;
        iterator = graph.traversal().E();
    }

    @Override
    public boolean next() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        Edge e = iterator.next();
        Iterator<Vertex> vertices = e.bothVertices();
        Vertex from = vertices.next();
        Vertex to = vertices.next();

        UTF8String[] list = {
                UTF8String.fromString(from.id().toString()),
                UTF8String.fromString(CollectionConverters.IteratorHasAsScala(from.properties()).asScala().mkString(",")),
                UTF8String.fromString(e.id().toString()),
                UTF8String.fromString(CollectionConverters.IteratorHasAsScala(e.properties()).asScala().mkString(",")),
                UTF8String.fromString(to.id().toString()),
                UTF8String.fromString(CollectionConverters.IteratorHasAsScala(to.properties()).asScala().mkString(","))
        };

        return new GenericInternalRow(list);
    }

    @Override
    public void close() throws IOException {

    }
}
