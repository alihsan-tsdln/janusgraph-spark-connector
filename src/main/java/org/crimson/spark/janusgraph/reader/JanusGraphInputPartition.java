package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.read.InputPartition;

public class JanusGraphInputPartition implements InputPartition {
    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}
