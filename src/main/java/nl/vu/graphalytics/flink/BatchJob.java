/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.vu.graphalytics.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.NullValue;
import org.apache.flink.graph.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.graph.library.*;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.graph.library.linkanalysis.PageRank.Result;
/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	// Create defaul
	public static final Object[][] EDGES = {
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

	public static void main(String[] args) throws Exception {
		// Parse command line parameter
		ParameterTool parameter = ParameterTool.fromArgs(args);

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Get vertices and edges from --vertices and --edges input
		DataSet<Tuple2<Long, Long>> vertexTuples = getVerticesDataSet(env, parameter);
		DataSet<Tuple3<Long, Long, Double>> edgeTuples = getEdgesDataSet(env, parameter);
		Graph<Long, Long, Double> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);

		// Label Propagation
//		DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(new LabelPropagation<Long, Long, Double>(2));
//		verticesWithCommunity.print();

		// PageRank
		DataSet<Result<Long>> verticesWithRank = graph.run(new PageRank<Long, Long, Double>(0.85, 2));
		verticesWithRank.print();
	}

	public static DataSet<Tuple3<Long, Long, Double>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

		List<Tuple3<Long, Long, Double>> edges = new ArrayList< Tuple3<Long, Long, Double> >();
		for (Object[] e : EDGES) {
			edges.add(new Tuple3<Long, Long, Double>((Long) e[0], (Long) e[1], (Double) e[2]));
		}
		return env.fromCollection(edges);
	}

	public static DataSet<Tuple2<Long, Long>> getDefaultVerticesDataSet(ExecutionEnvironment env) {
//		Tuple2<Long, Long> defaultVertex = ;
		List<Tuple2<Long, Long>> vertices = new ArrayList< Tuple2<Long, Long> >();

		vertices.add( new Tuple2<Long, Long> (0L, 0L));
		return  env.fromCollection(vertices);
	}

	private static DataSet<Tuple2<Long, Long>> getVerticesDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("vertices")) {
			return env.readCsvFile(params.get("vertices"))
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class)
					.map(new MapFunction<Tuple1<Long>, Tuple2<Long,Long> >() {
						@Override
						public Tuple2<Long, Long> map(Tuple1<Long> v) {
							return new Tuple2<Long, Long>(v.f0, 0L);
						}
					});
		} else {
			System.out.println("--vertices missing ");
			System.out.println("Use --vertices to specify file input.");
			return getDefaultVerticesDataSet(env);
		}
	}

	private static DataSet<Tuple3<Long, Long, Double>> getEdgesDataSet(ExecutionEnvironment env, ParameterTool params) {
		if (params.has("edges")) {
			return env.readCsvFile(params.get("edges"))
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class);
		} else {
			System.out.println("--edges missing");
			System.out.println("Use --edges to specify file input.");
			return getDefaultEdgeDataSet(env);
		}
	}
}
