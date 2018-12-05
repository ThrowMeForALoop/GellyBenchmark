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
import org.apache.flink.graph.*;

import nl.vu.graphalytics.loader.InputLoader;

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

	//private static final ResourceBundle rb = ResourceBundle.getBundle("config");

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


		// Get vertices and edges from --vertices and --edges input
		DataSet<Tuple2<Long, Long>> vertexTuples = inputLoader.getVerticesDataSet(env, parameter);
		DataSet<Tuple3<Long, Long, Double>> edgeTuples = inputLoader.getEdgesDataSet(env, parameter);
		return Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);
	}

	private static void runAlgorithm(Graph<Long, Long, Double> graph, ExecutionEnvironment env, ParameterTool parameter) {
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
		final ResourceBundle rb = ResourceBundle.getBundle("config");
		Integer numIterations = 1;
		Double dampingFactor = 0.85;

		try {
			switch (algorithmAcronym) {
				case "bfs":
					break;
				case "cdlp":
					numIterations = Integer.parseInt(rb.getString(propertyPrefix +"cdlp.max-iterations"));
					DataSet<Vertex<Long, Long>> verticesWithCommunity = graph.run(new LabelPropagation<Long, Long, Double>(2));
					verticesWithCommunity.count();
					break;
				case "lcc":
					break;
				case "pr":
					dampingFactor =  Double.parseDouble(rb.getString(propertyPrefix + "pr.damping-factor"));
					numIterations = Integer.parseInt(rb.getString(propertyPrefix + "graph.pr.num-iterations"));
					DataSet<Result<Long>> verticesWithRank = graph.run(new PageRank<Long, Long, Double>(dampingFactor, numIterations));
					verticesWithRank.count();
					break;
				case "sssp":
					//Integer sourceVertexId = Integer.parseInt(rb.getString(propertyPrefix "sssp.source-vertex"));

					break;
				case "wcc":
					break;
				default: throw new Exception("Algorithm " + algorithmAcronym + " is not supported!");
			}
		} catch (Exception exception) {
			System.out.println(exception.getMessage());
		}

	}
}
