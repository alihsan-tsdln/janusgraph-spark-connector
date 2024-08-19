package org.crimson.spark.janusgraph.reader.partitionReaders;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

public class JanusGraphPartitionReaderBySource implements PartitionReader<InternalRow> {
    private final GraphTraversal<Edge, Edge> iterator;
    private Edge v;
    private String[] columnNamesSource = {};
    private final String[] columnNamesRelation;
    private String[] columnNamesTarget = {};


    public JanusGraphPartitionReaderBySource(JanusGraph graph, String[] columnNames, String relation, String source, String target) {
        GraphTraversalSource g = graph.traversal();
        iterator = g.E().hasLabel(relation);
        int sourceProCount = 0;
        if(source != null) {
            Iterator<VertexProperty<Object>> keys = g.V().hasLabel(source).next().properties();
            while(keys.hasNext()) {
                sourceProCount++;
                keys.next();
            }
            columnNamesSource = Arrays.copyOfRange(columnNames, 0, sourceProCount);
        }
        Iterator<Property<Object>> keysE = g.E().hasLabel(relation).next().properties();
        int relationProCount = sourceProCount;
        while (keysE.hasNext()) {
            relationProCount++;
            keysE.next();
        }
        columnNamesRelation = Arrays.copyOfRange(columnNames, sourceProCount, relationProCount);
        if(target != null) {
            columnNamesTarget = Arrays.copyOfRange(columnNames, relationProCount, columnNames.length);
        }

    }

    @Override
    public boolean next() {
        return iterator.hasNext();
    }

    @Override
    public InternalRow get() {
        v = iterator.next();
        Stream<Object> a = Arrays.stream(columnNamesSource).map(n -> v.outVertex().value(n) instanceof String ? UTF8String.fromString(v.outVertex().value(n)) : v.outVertex().value(n));
        ArrayList<Object> listSource = new ArrayList<>(a.toList());
        System.out.println(listSource);
        a = Arrays.stream(columnNamesRelation).map(n -> v.value(n) instanceof String ? UTF8String.fromString(v.value(n)) : v.value(n));
        listSource.addAll(a.toList());
        System.out.println(listSource);
        a = Arrays.stream(columnNamesTarget).map(n -> v.inVertex().value(n) instanceof String ? UTF8String.fromString(v.inVertex().value(n)) : v.inVertex().value(n));
        listSource.addAll(a.toList());
        System.out.println(listSource);
        return new GenericInternalRow(listSource.toArray());
    }

    @Override
    public void close() {
    }
}
