package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

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
        try (JanusGraphPartitionReader reader = new JanusGraphPartitionReader(inputPartition, schema, partitioning, properties)) {
            return reader;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
