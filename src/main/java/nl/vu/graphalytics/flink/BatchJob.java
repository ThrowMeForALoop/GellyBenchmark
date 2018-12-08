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

import java.util.ResourceBundle;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.library.*;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.apache.flink.graph.library.linkanalysis.PageRank.Result;
import org.apache.flink.graph.library.SingleSourceShortestPaths;

import org.apache.flink.graph.*;

import nl.vu.graphalytics.loader.InputLoader;
import nl.vu.graphalytics.algorithms.BFS;
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

	private static final ResourceBundle rb = ResourceBundle.getBundle("config");

	public static void main(String[] args) throws Exception {
		// Parse command line parameter
		ParameterTool parameter = ParameterTool.fromArgs(args);
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Get vertices and edges from --vertices and --edges input
		Graph<Long, Long, Double> graph = loadGraph(env, parameter);

		runAlgorithm(graph, env, parameter);
	}

	private static Graph<Long, Long, Double> loadGraph(ExecutionEnvironment env, ParameterTool parameter) {
		final InputLoader inputLoader = new InputLoader(env, parameter);
		DataSet<Tuple2<Long, Long>> vertexTuples;
		DataSet<Tuple3<Long, Long, Double>> edgeTuples;

		try {
			// Get vertices and edges from --vertices and --edges input
			vertexTuples = inputLoader.getVerticesDataSet(env, parameter);
			System.out.printf("Vertices: %d \n ", vertexTuples.count());

			 edgeTuples = inputLoader.getEdgesDataSet(env, parameter);
			System.out.printf("Edges: %d", edgeTuples.count());
		} catch (Exception e) {
			e.printStackTrace();
			 vertexTuples = inputLoader.getDefaultVerticesDataSet(env);
			 edgeTuples = inputLoader.getDefaultEdgeDataSet(env);
		}

		return Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);
	}

	private static void runAlgorithm(Graph<Long, Long, Double> graph, ExecutionEnvironment env, ParameterTool parameter) {
		try {
			System.out.println(graph.getVertices().count());
		} catch (Exception e) {
			System.out.println("Load graph exception");
			System.out.println(e.getMessage());
		}


		if (! parameter.has("algorithm")) {
			System.out.println("--algorithm missing");
			System.out.println("Use --algorithm pr / cdlp / lcc / bfs / wcc]");
			return;
		}

		if(!parameter.has("dataset")) {
			System.out.println("--dataset datasetname missing");
			System.out.println("Use --dataset datasetname");
			return;
		}

		String dataSetName = parameter.get("dataset");
		String propertyPrefix = "graph." + dataSetName + ".";

		String algorithmAcronym = parameter.get("algorithm");
		Integer numIterations = 1;
		Double dampingFactor = 0.85;
		Long sourceVertexId = 0L;

		try {
			switch (algorithmAcronym) {
				case "bfs":
					sourceVertexId = Long.parseLong(rb.getString(propertyPrefix + "bfs.source-vertex"));
					DataSet<Tuple2<Long, Long>> bfsResult = graph.run(new BFS(sourceVertexId, numIterations));
					bfsResult.count();

					break;
				case "cdlp":
					numIterations = Integer.parseInt(rb.getString(propertyPrefix +"cdlp.max-iterations"));
					DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(new LabelPropagation<Long, Long, Double>(2));
					verticesWithCommunity.count();
					break;
				case "lcc":
					break;
				case "pr":
//					System.out.println(propertyPrefix + "pr.damping-factor");
//					System.out.println(propertyPrefix + "pr.num-iterations");
					dampingFactor =  Double.parseDouble(rb.getString(propertyPrefix + "pr.damping-factor"));
					numIterations = Integer.parseInt(rb.getString(propertyPrefix + "pr.num-iterations"));
					System.out.printf("Start page ranking - Damping: %.2f - Iterator: %d \n", dampingFactor, numIterations);
					DataSet<Result<Long>> verticesWithRank = graph.run(new PageRank<Long, Long, Double>(dampingFactor, numIterations));
//					System.out.println("2");
					verticesWithRank.count();
//					System.out.println("3");
					break;
				case "sssp":
					sourceVertexId = Long.parseLong(rb.getString(propertyPrefix + "sssp.source-vertex"));

					DataSet<Vertex<Long,Double>> singleSourceShortestPaths =
							graph.run(new SingleSourceShortestPaths<Long, Long>(sourceVertexId, numIterations));
					singleSourceShortestPaths.count();
					break;
				case "wcc":
					DataSet<Vertex<Long, Long>> components = graph.run(new ConnectedComponents<Long, Long, Double>(numIterations));
					components.count();
					break;
				default:
					throw new Exception("Algorithm " + algorithmAcronym + " is not supported!");
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}

	}
}
