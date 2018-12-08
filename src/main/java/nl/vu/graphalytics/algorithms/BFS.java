package nl.vu.graphalytics.algorithms;

//import nl.tudelft.graphalytics.domain.algorithms.AlgorithmParameters;
//import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.types.NullValue;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

public class BFS implements GraphAlgorithm<Long, Long, Double, DataSet<Tuple2<Long, Long>>> {

    private final Long srcVertexId;
    private final Integer maxIterations;

    /**
     * @param srcVertexId The ID of the source vertex.
     * @param maxIterations The maximum number of iterations to run.
     */
    public BFS(Long srcVertexId, Integer maxIterations) {
        this.srcVertexId = srcVertexId;
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Tuple2<Long, Long>> run(Graph<Long, Long, Double> input) {
        Graph<Long, Long, Double> result = input.mapVertices(new InitVerticesMapper(srcVertexId))
                .runScatterGatherIteration(new MinDistanceMessenger(), new VertexDistanceUpdater(),
                        maxIterations);

        return result.getVertices().map(new VertexToTuple2Map<Long, Long>());
    }

    @SuppressWarnings("serial")
    public static final class InitVerticesMapper implements MapFunction<Vertex<Long, Long>, Long> {

        private Long srcVertexId;

        public InitVerticesMapper(Long srcId) {
            this.srcVertexId = srcId;
        }

        public Long map(Vertex<Long, Long> value) {
            if (value.f0.equals(srcVertexId)) {
                return 0l;
            } else {
                return Long.MAX_VALUE;
            }
        }
    }

    /**
     * Function that updates the value of a vertex by picking the minimum
     * distance from all incoming messages.
     *
     */
//    @SuppressWarnings("serial")
//    public static final class VertexDistanceUpdater extends GatherFunction<Long, Long, Long> {
//
//        @Override
//        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
//
//            long minDistance = Long.MAX_VALUE;
//
//            for (long msg : inMessages) {
//                if (msg < minDistance) {
//                    minDistance = msg;
//                }
//            }
//
//            if (vertex.getValue() > minDistance) {
//                setNewVertexValue(minDistance);
//            }
//        }
//    }

    // gather: vertex update
    public static final class VertexDistanceUpdater extends GatherFunction<Long, Long, Long> {
        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            long minDistance = Long.MAX_VALUE;

            for (long msg : inMessages) {
                if (msg < minDistance) {
                    minDistance = msg;
                }
            }

            if (vertex.getValue() > minDistance) {
                setNewVertexValue(minDistance);
            }
        }
    }

    /**
     * Distributes the minimum distance associated with a given vertex among all
     * the target vertices summed up with the edge's value.
     *
     */
    @SuppressWarnings("serial")
    public static final class MinDistanceMessenger extends ScatterFunction<Long, Long, Long, Double> {

        @Override
        public void sendMessages(Vertex<Long, Long> vertex) {
            if (vertex.getValue() < Long.MAX_VALUE) {
                sendMessageToAllNeighbors(vertex.getValue() + 1);
            }
        }
    }

}
