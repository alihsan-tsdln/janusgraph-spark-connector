package org.crimson.spark.janusgraph.reader;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

public class JanusGraphReader implements TableProvider {
    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
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
                list.add(new StructField(a.key(), DataTypes.StringType, false, Metadata.empty()));
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

            Iterator<Property<Object>> keys = g.E().hasLabel(options.get("relationship")).next().properties();
            ArrayList<StructField> list = new ArrayList<>();

            while (keys.hasNext()) {
                Property<Object> a = keys.next();
                list.add(new StructField(a.key(), DataTypes.StringType, false, Metadata.empty()));
            }

            //TODO ADD SOURCE AND TARGET SCHEMA INFER

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
        if(properties.get("label") != null) {
            inferSchema(new CaseInsensitiveStringMap(properties));
        }
        return new JanusGraphTable(structType, partitioning, properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}
