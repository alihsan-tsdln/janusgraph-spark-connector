package org.crimson;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("CUSTOM DATASOURCE").getOrCreate();

        //label
        //relationship
        //query

        Dataset<Row> a = spark.read().format("org.crimson.spark.janusgraph.reader.JanusGraphReader")
                .option("storage.backend", "cql")
                .option("storage.hostname", "0.0.0.0")
                .option("storage.port", "9042")
                .option("storage.batch-loading", true)
                .option("query.batch",true)
                .option("query.force-index", false)
                .option("index.search.backend", "elasticsearch")
                .option("index.search.hostname", "0.0.0.0")
                .option("index.search.port", "9200")
                .option("index.search.elasticsearch.client-only", true)
                //.option("label","person")
                .option("relationship", "acted")
                .load();

        System.out.println("DATAFRAME");
        a.show();
    }
}