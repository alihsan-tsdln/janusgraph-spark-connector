package org.crimson.spark.janusgraph.reader.partitionReaders;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JanusGraphPartitionReaderBySource implements PartitionReader<InternalRow> {
    private final GraphTraversal<Edge, Edge> iterator;
    private ArrayList<UTF8String> listSource;
    private final String[] fieldNamesFromSource;
    private final String[] fieldNamesFromRelation;
    private final String[] fieldNamesFromTarget;

    public JanusGraphPartitionReaderBySource(InputPartition inputPartition, JanusGraph graph, String relation, String source, String target) {
        iterator = graph.traversal().E().hasLabel(relation);
        ArrayList<String> list = new ArrayList<>();
        Iterator<VertexProperty<Object>> keysV = graph.traversal().V().hasLabel(source).next().properties();
        while (keysV.hasNext()) { list.add(keysV.next().key()); }
        fieldNamesFromSource = list.toArray(new String[0]);
        Iterator<Property<Object>> keys = graph.traversal().E().hasLabel(relation).next().properties();
        list.clear();
        while (keys.hasNext()) { list.add(keys.next().key()); }
        fieldNamesFromRelation = list.toArray(new String[0]);
        keysV = graph.traversal().V().hasLabel(target).next().properties();
        list.clear();
        while (keysV.hasNext()) { list.add(keysV.next().key()); }
        fieldNamesFromTarget = list.toArray(new String[0]);
        list.clear();
        System.out.println(Arrays.toString(fieldNamesFromSource));
        System.out.println(Arrays.toString(fieldNamesFromRelation));
        System.out.println(Arrays.toString(fieldNamesFromTarget));
    }

    @Override
    public boolean next() throws IOException {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        listSource = new ArrayList<>(Arrays.stream(fieldNamesFromSource).map(n -> UTF8String.fromString(iterator.next().outVertex().value(n).toString())).toList());
        listSource.addAll(Arrays.stream(fieldNamesFromRelation).map(n -> UTF8String.fromString(iterator.next().value(n).toString())).toList());
        listSource.addAll(Arrays.stream(fieldNamesFromTarget).map(n -> UTF8String.fromString(iterator.next().inVertex().value(n).toString())).toList());
        return new GenericInternalRow(listSource.toArray());
    }

    @Override
    public void close() throws IOException {
        listSource.clear();
    }
}
