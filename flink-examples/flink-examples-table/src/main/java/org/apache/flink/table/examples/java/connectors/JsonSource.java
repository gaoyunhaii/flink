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

package org.apache.flink.table.examples.java.connectors;

import com.google.common.io.Files;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.filesystem.DeserializationSchemaAdapter;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

public class JsonSource {
	private static final int NUM_RECORDS = 1000;

	public static void main(String[] args) throws IOException {
		File dir = generateTestData();

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		bsEnv.setParallelism(1);
		bsEnv.setRestartStrategy(RestartStrategies.noRestart());
		EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

		String sql = String.format(
				"CREATE TABLE source ( " +
						"	id INT, " +
						"	content STRING" +
						") PARTITIONED by (id) WITH (" +
						"	'connector' = 'filesystem'," +
						"	'path' = '%s'," +
						"	'format' = 'json'" +
						")", dir);
		bsTableEnv.executeSql(sql);

		TableResult result = bsTableEnv.executeSql("select * from source");
		Set<String> elements = new HashSet<>();
		result.collect().forEachRemaining(r -> elements.add((String) r.getField(1)));
		System.out.println("Count: " + elements.size());
		System.out.println("Elements: " + elements);
	}

	private static File generateTestData() throws IOException {
		File tempDir = Files.createTempDir();

		File root = new File(tempDir, "id=0");
		root.mkdir();

		File dataFile = new File(root, "testdata");
		try (PrintWriter writer = new PrintWriter(dataFile)) {
			for (int i = 0; i < NUM_RECORDS; ++i) {
				writer.println(String.format("{\"content\":\"%s\"}", i));
			}
		}

		return tempDir;
	}
}
