package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class JanusGraphBatch implements Batch {

    private final StructType schema;
    private final Transform[] partitioning;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public JanusGraphBatch(StructType schema, Transform[] partitioning, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        return new InputPartition[] {new JanusGraphInputPartition()};
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new JanusGraphPartitionReaderFactory(schema, properties);
    }
}
