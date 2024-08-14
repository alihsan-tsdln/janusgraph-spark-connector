package org.crimson.spark.janusgraph.reader.partitionReaders;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

public class JanusGraphPartitionReaderByLabel implements PartitionReader<InternalRow> {

    private final GraphTraversal<Vertex, Vertex> iterator;
    private final Object[] fieldNames;

    public JanusGraphPartitionReaderByLabel(InputPartition inputPartition, Object[] fieldNames, GraphTraversalSource g, String label) {
        this.fieldNames = fieldNames;
        iterator = g.V().hasLabel(label).limit(10);
    }

    @Override
    public boolean next() {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        return new GenericInternalRow(Arrays.stream(fieldNames).map(n -> UTF8String.fromString(iterator.next().value(n.toString()).toString())).toArray());
    }

    @Override
    public void close() {

    }

    public Object[] getFieldNames() {
        return fieldNames;
    }
}