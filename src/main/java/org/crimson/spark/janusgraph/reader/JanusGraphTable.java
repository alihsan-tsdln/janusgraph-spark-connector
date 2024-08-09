package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public record JanusGraphTable(StructType schema, Transform[] partitioning,
                              Map<String, String> properties) implements Table, SupportsRead {

    public JanusGraphTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        this.schema = schema;
        this.partitioning = partitioning;
        this.properties = properties;
        System.out.println("JanusGraphTable");
        System.out.println("SCHEMA");
        System.out.println(schema);
        System.out.println("PARTITIONING");
        System.out.println(Arrays.toString(partitioning));
        System.out.println("PROPERTIES");
        System.out.println(properties);
    }

    @Contract(pure = true)
    @Override
    public @NotNull String name() {
        return "janusgraph";
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new JanusGraphScanBuilder(schema, partitioning, properties, options);
    }
}
