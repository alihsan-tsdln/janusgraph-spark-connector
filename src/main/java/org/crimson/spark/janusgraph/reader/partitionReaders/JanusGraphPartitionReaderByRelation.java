package org.crimson.spark.janusgraph.reader.partitionReaders;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.JanusGraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

public class JanusGraphPartitionReaderByRelation implements PartitionReader<InternalRow> {

    private final Object[] fieldNames;
    private final GraphTraversal<Edge, Edge> iterator;

    public JanusGraphPartitionReaderByRelation(InputPartition inputPartition, Object[] fieldNames, JanusGraph graph, String relation) {
        this.fieldNames = fieldNames;
        iterator = graph.traversal().E().hasLabel(relation);
    }

    @Override
    public boolean next() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        return new GenericInternalRow(Arrays.stream(fieldNames).map(n -> UTF8String.fromString(iterator.next().value(n.toString()).toString())).toArray());
    }

    @Override
    public void close() throws IOException {

    }
}
