package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class JanusGraphReader implements TableProvider {
    @Override
    public StructType inferSchema(@NotNull CaseInsensitiveStringMap options) {
        if(options.containsKey("label"))
        {
            JanusGraphFactory.Builder build = JanusGraphFactory.build();

            for(Map.Entry<String, String> property : options.entrySet()) {
                build.set(property.getKey(), property.getValue());
            }

            JanusGraph graph = build.open();

            GraphTraversalSource g = graph.traversal();

            Iterator<VertexProperty<Object>> keys = g.V().hasLabel(options.get("label")).next().properties();
            ArrayList<StructField> list = new ArrayList<>();

            while (keys.hasNext()) {
                VertexProperty<Object> a = keys.next();
                list.add(new StructField(a.key(), checkType(a.value().getClass().getName()), false, Metadata.empty()));
            }

            return new StructType(list.toArray(new StructField[0]));
        }

        else if(options.containsKey("relationship")) {
            JanusGraphFactory.Builder build = JanusGraphFactory.build();

            for(Map.Entry<String, String> property : options.entrySet()) {
                build.set(property.getKey(), property.getValue());
            }

            JanusGraph graph = build.open();

            GraphTraversalSource g = graph.traversal();

            ArrayList<StructField> list = new ArrayList<>();

            if(options.containsKey("relationship.source.vertex")) {
                Iterator<VertexProperty<Object>> keysV = g.V().hasLabel(options.get("relationship.source.vertex")).next().properties();

                while (keysV.hasNext()) {
                    VertexProperty<Object> v = keysV.next();
                    list.add(new StructField(v.key(), checkType(v.value().getClass().getName()), false, Metadata.empty()));
                }
            }

            Iterator<Property<Object>> keys = g.E().hasLabel(options.get("relationship")).next().properties();

            while (keys.hasNext()) {
                Property<Object> a = keys.next();
                list.add(new StructField(a.key(), checkType(a.value().getClass().getName()), false, Metadata.empty()));
            }

            if(options.containsKey("relationship.target.vertex")) {
                Iterator<VertexProperty<Object>> keysV = g.V().hasLabel(options.get("relationship.target.vertex")).next().properties();

                while (keysV.hasNext()) {
                    VertexProperty<Object> v = keysV.next();
                    list.add(new StructField(v.key(), checkType(v.value().getClass().getName()), false, Metadata.empty()));
                }
            }

            graph.close();

            return new StructType(list.toArray(new StructField[0]));
        }

        return new StructType()
                .add("idFrom", DataTypes.StringType, false)
                .add("propertiesFrom", DataTypes.StringType, false)
                .add("edgeId", DataTypes.StringType, false)
                .add("edgeProperties", DataTypes.StringType, false)
                .add("idTo", DataTypes.StringType, false)
                .add("propertiesTo", DataTypes.StringType, false);
    }

    @Override
    public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
        return TableProvider.super.inferPartitioning(options);
    }

    @Override
    public Table getTable(StructType structType, Transform[] partitioning, Map<String, String> properties) {
        System.out.println(structType);
        return new JanusGraphTable(structType, partitioning, properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }

    private DataType checkType(String className) {
        return switch (className) {
            case "java.lang.Integer" -> DataTypes.IntegerType;
            case "java.lang.Float" -> DataTypes.FloatType;
            case "java.lang.Double" -> DataTypes.DoubleType;
            case "java.lang.Long" -> DataTypes.LongType;
            default -> DataTypes.StringType;
        };
    }
}
