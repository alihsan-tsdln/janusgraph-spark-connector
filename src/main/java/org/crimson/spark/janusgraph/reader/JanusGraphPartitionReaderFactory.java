package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderByLabel;
import org.crimson.spark.janusgraph.reader.partitionReaders.JanusGraphPartitionReaderBySource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.Arrays;
import java.util.Map;

public class JanusGraphPartitionReaderFactory implements PartitionReaderFactory {

    private final StructType schema;
    private final Map<String, String> properties;

    public JanusGraphPartitionReaderFactory(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition inputPartition) {
        JanusGraphFactory.Builder build = JanusGraphFactory.build();
        for(Map.Entry<String, String> property : properties.entrySet()) { build.set(property.getKey(), property.getValue()); }
        JanusGraph graph = build.open();

        String label = properties.get("label");
        if (label != null) {
            return new JanusGraphPartitionReaderByLabel(schema.fieldNames(), graph, label);
        }
        label = properties.get("relationship");
        if (label != null) {
            String source = properties.get("relationship.source.vertex");
            String target = properties.get("relationship.target.vertex");
            return new JanusGraphPartitionReaderBySource(graph, schema.fieldNames(), label, source, target);
        }

        return new JanusGraphPartitionReader(inputPartition, schema.fieldNames(), graph);
    }
}
