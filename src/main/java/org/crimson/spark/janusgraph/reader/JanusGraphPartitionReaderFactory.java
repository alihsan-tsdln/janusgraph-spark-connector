package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderByLabel;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderByRelation;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderBySource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

public class JanusGraphPartitionReaderFactory implements PartitionReaderFactory {

    private final StructType schema;
    private final Transform[] partitioning;
    private final Map<String, String> properties;

    public JanusGraphPartitionReaderFactory(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        JanusGraphFactory.Builder build = JanusGraphFactory.build();
        for(Map.Entry<String, String> property : properties.entrySet()) { build.set(property.getKey(), property.getValue()); }
        JanusGraph graph = build.open();
        Object[] columnNames = Arrays.stream(schema.fields()).map(StructField::name).toArray();

        String label = properties.get("label");
        if (label != null) {
            return new JanusGraphPartitionReaderByLabel(inputPartition, columnNames, graph, label);
        }
        label = properties.get("relationship");
        if (label != null) {
            String source = properties.get("relationship.source.vertex");
            String target = properties.get("relationship.target.vertex");
            if(source != null && target != null) {
                return new JanusGraphPartitionReaderBySource(inputPartition, graph, label, source, target);
            }
            return new JanusGraphPartitionReaderByRelation(inputPartition, columnNames, graph, label);
        }

        return new JanusGraphPartitionReader(inputPartition, columnNames, graph);
    }
}
