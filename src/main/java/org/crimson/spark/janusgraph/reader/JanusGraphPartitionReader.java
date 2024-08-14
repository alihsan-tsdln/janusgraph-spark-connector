package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.*;

public class JanusGraphPartitionReader implements PartitionReader<InternalRow> {
    private InputPartition inputPartition;
    private StructType schema;
    private Transform[] partitioning;
    private Map<String, String> properties;
    private final JanusGraph graph;
    private final GraphTraversalSource g;

    //GraphTraversal<?, ?> iterator;
    Iterator<Object> iterator;

    List<String> fieldNames;

    public JanusGraphPartitionReader(InputPartition inputPartition, StructType schema, Transform[] partitioning, Map<String, String> properties) {
        this.properties = properties;
        this.schema = schema;
        this.partitioning = partitioning;
        this.inputPartition = inputPartition;

        System.out.println(this.schema);

        JanusGraphFactory.Builder build = JanusGraphFactory.build();

        for(Map.Entry<String, String> property : properties.entrySet()) {
            build.set(property.getKey(), property.getValue());
        }

        graph = build.open();

        g = graph.traversal();

        if(properties.get("label") != null) {
            iterator = g.V().hasLabel(properties.get("label")).limit(10).id();
        } else if (properties.get("relationship") != null) {
            iterator = g.E().hasLabel(properties.get("relationship")).limit(10).id();

            if(properties.get("relationship.source.vertex") != null) {
                ArrayList<Object> tempList = new ArrayList<>();

                GraphTraversal<Edge, Object> sourceVertex = g.E().hasLabel(properties.get("relationship")).outV().limit(10).id();
                
                while (sourceVertex.hasNext() && iterator.hasNext()) {
                    tempList.add(sourceVertex.next());
                    tempList.add(iterator.next());
                }
                System.out.println(tempList);
                iterator = tempList.iterator();
            }

        } else {
            iterator = g.E().id();
        }
        fieldNames = Arrays.stream(schema.fields())
                .map(StructField::name)
                .toList();

        //graph.close();
    }

    @Override
    public boolean next() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        if(properties.get("label") != null) {
            List<UTF8String> list = fieldNames.stream().map(n -> UTF8String.fromString(g.V(iterator.next().toString()).next().value(n).toString())).toList();
            return new GenericInternalRow(list.toArray());
        } else if (properties.get("relationship") != null) {
            List<UTF8String> list = new ArrayList<>();
            if(properties.get("relationship.source.vertex") != null) {
                System.out.println(fieldNames);
                list.addAll(fieldNames.stream().map(n -> UTF8String.fromString(g.V(iterator.next().toString()).next().value(n).toString())).toList());
            }
            list.addAll(fieldNames.stream().map(n -> UTF8String.fromString(g.E(iterator.next().toString()).next().value(n).toString())).toList());
            return new GenericInternalRow(list.toArray());
        }

        Edge e = (Edge) iterator.next();
        Iterator<Vertex> vertices = e.bothVertices();
        Vertex from = vertices.next();
        Vertex to = vertices.next();

        UTF8String[] list = {
                UTF8String.fromString(from.id().toString()),
                UTF8String.fromString(JavaConverters.asScalaIteratorConverter(from.properties()).asScala().mkString(",")),
                UTF8String.fromString(e.id().toString()),
                UTF8String.fromString(JavaConverters.asScalaIteratorConverter(e.properties()).asScala().mkString(",")),
                UTF8String.fromString(to.id().toString()),
                UTF8String.fromString(JavaConverters.asScalaIteratorConverter(to.properties()).asScala().mkString(","))
        };

        return new GenericInternalRow(list);
    }

    @Override
    public void close() throws IOException {

    }
}
