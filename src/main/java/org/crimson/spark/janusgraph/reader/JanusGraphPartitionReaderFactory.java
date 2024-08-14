package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderByLabel;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderByRelation;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderBySource;
import org.janusgraph.core.JanusGraphFactory;

import java.util.Arrays;
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
        GraphTraversalSource g = build.open().traversal();
        Object[] columnNames = Arrays.stream(schema.fields()).map(StructField::name).toArray();

        String label = properties.get("label");
        if (label != null) {
            return new JanusGraphPartitionReaderByLabel(inputPartition, columnNames, g, label);
        }
        label = properties.get("relationship");
        if (label != null) {
            String source = properties.get("relationship.source.vertex");
            if(source != null) {
                return new JanusGraphPartitionReaderBySource(inputPartition, columnNames, g, label, source);
            }
            return new JanusGraphPartitionReaderByRelation(inputPartition, columnNames, g, label);
        }

        return null;
    }
}
