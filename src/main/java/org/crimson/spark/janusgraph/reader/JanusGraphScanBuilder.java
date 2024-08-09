package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class JanusGraphScanBuilder implements ScanBuilder {

    private final StructType schema;
    private final Transform[] partitioning;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public JanusGraphScanBuilder(StructType schema, Transform[] partitioning, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public Scan build() {
        return new JanusGraphScan(schema, partitioning, properties, options);
    }
}
