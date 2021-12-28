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

package flinkdemo;

import flinkdemo.entity.Query;
import flinkdemo.operator.CacheAndLandmark;
import flinkdemo.operator.MiniResolver;
import flinkdemo.operator.QueryCluster;
import flinkdemo.operator.WindowRouter;
import flinkdemo.source.MySyntheticSource;
import flinkdemo.util.DataLoad;
import flinkdemo.util.TopologyGraph;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//Reading configuration Files
		String propertiesFile = "src/main/resources/myJob.properties";
		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesFile);
		//register global job parameters
		// env.getConfig().setGlobalJobParameters(parameterTool);

		//get the public topology network
		DataLoad.getGraph(parameterTool.get("dataLoad.coDataFileName"), parameterTool.get("dataLoad.grDataFileName"),
				parameterTool.getDouble("cluster.eccentricity"));

		// create SideOutPut for LengthRouter and Injection through the constructor
		final OutputTag<Query> miniQuery = new OutputTag<>("miniQuery"){};

		//miniQuery about 10%; cacheAble query about 15%
		SingleOutputStreamOperator<Query> mainDataStream = env.addSource(new MySyntheticSource(TopologyGraph.getNumOfVertices()))
				.keyBy(Query::getPartition)
				.process(new WindowRouter(miniQuery, parameterTool.getInt("router.bufferSize")));

		// get miniQuery output(query length < 0.3 average query length)
		DataStream<Query> miniDataStream = mainDataStream.getSideOutput(miniQuery);

		// query cluster by search space estimation and set query.clusterID
		DataStream<Query> preparedDataStream = mainDataStream.keyBy(Query::getPartition)
						.process(new QueryCluster(parameterTool.getInt("cluster.convergence"),
								parameterTool.getDouble("cluster.eccentricity")));

		DataStream<String> landmarkResultDataStream = preparedDataStream.keyBy(Query::getClusterID)
						.process(new CacheAndLandmark(parameterTool.getInt("localCache.winners.MaxSize"),
								parameterTool.getInt("localCache.candidates.MaxSize"),
								parameterTool.getInt("localCache.convergence"),
								parameterTool.getDouble("localCache.negligible"),
								parameterTool.getInt("localCache.abandon")));
//
		DataStream<String> miniResultDataStream = miniDataStream.map(new MiniResolver());

//		landmarkResultDataStream.print();
//		miniResultDataStream.print();

		// execute program
		env.execute("test");
	}
}
