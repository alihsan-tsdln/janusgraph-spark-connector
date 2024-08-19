package org.crimson.spark.janusgraph.reader.partitionReaders;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class JanusGraphPartitionReaderByLabel implements PartitionReader<InternalRow> {

    private final GraphTraversal<Vertex, Vertex> iterator;
    private final String[] fieldNames;
    private Vertex v;

    public JanusGraphPartitionReaderByLabel(String[] fieldNames, JanusGraph graph, String label) {
        this.fieldNames = fieldNames;
        iterator = graph.traversal().V().hasLabel(label);
    }

    @Override
    public boolean next() {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        v = iterator.next();
        Stream<Object> objectStream = Arrays.stream(fieldNames).map(n -> v.value(n) instanceof String ? UTF8String.fromString(v.value(n)) : v.value(n));
        return new GenericInternalRow(objectStream.toArray());
    }

    @Override
    public void close() {}
}