package nl.vu.graphalytics.loader;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.apache.flink.types.NullValue;
import org.apache.flink.graph.*;

public class InputLoader {

    private final ExecutionEnvironment env;
    private final ParameterTool params;

    // Create defaul
    public final Object[][] EDGES = {
            {1L, 2L},
            {1L, 15L},
            {2L, 3L},
            {2L, 4L},
            {2L, 5L},
            {2L, 6L},
            {2L, 7L},
            {3L, 13L},
            {4L, 2L},
            {5L, 11L},
            {5L, 12L},
            {6L, 1L},
            {6L, 7L},
            {6L, 8L},
            {7L, 1L},
            {7L, 8L},
            {8L, 1L},
            {8L, 9L},
            {8L, 10L},
            {9L, 14L},
            {9L, 1L},
            {10L, 1L},
            {10L, 13L},
            {11L, 12L},
            {11L, 1L},
            {12L, 1L},
            {13L, 14L},
            {14L, 12L},
            {15L, 1L},
    };

    public InputLoader(ExecutionEnvironment env, ParameterTool params) {
        this.env = env;
        this.params = params;
    }

    public DataSet<Tuple3<Long, Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Tuple3<Long, Long, Double>> edges = new ArrayList< Tuple3<Long, Long, Double> >();
        for (Object[] e : EDGES) {
            edges.add(new Tuple3<Long, Long, Double>((Long) e[0], (Long) e[1], 1.0));
        }
        return env.fromCollection(edges);
    }

    public DataSet<Tuple2<Long, Long>> getDefaultVerticesDataSet(ExecutionEnvironment env) {
        List<Tuple2<Long, Long>> vertices = new ArrayList< Tuple2<Long, Long> >();

        vertices.add( new Tuple2<Long, Long> (0L, 0L));
        return  env.fromCollection(vertices);
    }

    public DataSet<Tuple2<Long, Long>> getVerticesDataSet(ExecutionEnvironment env, ParameterTool params) {
        if (params.has("vertices")) {
            return env.readCsvFile(params.get("vertices"))
                    .fieldDelimiter(" ")
                    .lineDelimiter("\n")
                    .types(Long.class)
                    .map(new MapFunction<Tuple1<Long>, Tuple2<Long,Long> >() {
                        @Override
                        public Tuple2<Long, Long> map(Tuple1<Long> v) {
                            return new Tuple2<Long, Long>(v.f0, v.f0);
                        }
                    });
        } else {
            System.out.println("--vertices missing ");
            System.out.println("Use --vertices to specify file input.");
            return getDefaultVerticesDataSet(env);
        }
    }

    public DataSet<Tuple3<Long, Long, Double>> getEdgesDataSet(ExecutionEnvironment env, ParameterTool params) {
        if (params.has("edges")) {
            return env.readTextFile(params.get("edges")).map(new MapFunction<String, Tuple3<Long,Long, Double> >() {
                @Override
                public Tuple3<Long,Long, Double> map(String line) {
                    String[] columns = line.split(" ");
                    Double weight = 1.0;

                    if (columns.length == 3) {
                        weight =  Double.parseDouble(columns[2]);
                        //return new Tuple3(Long.parseLong(columns[0]), Long.parseLong(columns[1]), 1.0);
                    } else {
                        //return new Tuple3(Long.parseLong(columns[0]), Long.parseLong(columns[1]), Double.parseDouble(columns[2]));
                    }

                    return  new Tuple3(Long.parseLong(columns[0]), Long.parseLong(columns[1]), weight);
                }
            });

//            return env.readCsvFile(params.get("edges"))
//                    .fieldDelimiter(" ")
//                    .lineDelimiter("\n")
//                    .ignoreInvalidLines()
//                    .types(Long.class, Long.class, Double.class);
        } else {
            System.out.println("--edges missing");
            System.out.println("Use --edges to specify file input.");
            return getDefaultEdgeDataSet(env);
        }
    }
}
