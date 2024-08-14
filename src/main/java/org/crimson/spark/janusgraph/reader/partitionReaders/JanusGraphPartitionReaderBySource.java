package org.crimson.spark.janusgraph.reader.partitionReaders;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;

public class JanusGraphPartitionReaderBySource implements PartitionReader<InternalRow> {

    private final Object[] fieldNames;
    private final GraphTraversal<Edge, Vertex> iterator;

    public JanusGraphPartitionReaderBySource(InputPartition inputPartition, Object[] fieldNames, GraphTraversalSource g, String relation, String source) {
        this.fieldNames = fieldNames;

        iterator = g.E().hasLabel(relation).bothV().limit(10);
    }

    @Override
    public boolean next() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
