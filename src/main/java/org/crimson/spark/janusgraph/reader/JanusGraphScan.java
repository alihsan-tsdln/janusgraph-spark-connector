package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class JanusGraphScan implements Scan {

    private final StructType schema;
    private final Transform[] partitioning;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;

    public JanusGraphScan(StructType schema, Transform[] partitioning, Map<String, String> properties, CaseInsensitiveStringMap options) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return Scan.super.description();
    }

    @Override
    public Batch toBatch() {
        return new JanusGraphBatch(schema, partitioning, properties, options);
    }

    @Override
    public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
        return Scan.super.toMicroBatchStream(checkpointLocation);
    }

    @Override
    public ContinuousStream toContinuousStream(String checkpointLocation) {
        return Scan.super.toContinuousStream(checkpointLocation);
    }

    @Override
    public CustomMetric[] supportedCustomMetrics() {
        return Scan.super.supportedCustomMetrics();
    }

    @Override
    public CustomTaskMetric[] reportDriverMetrics() {
        return Scan.super.reportDriverMetrics();
    }

    @Override
    public ColumnarSupportMode columnarSupportMode() {
        return Scan.super.columnarSupportMode();
    }
}
