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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.formats.hadoop.bulk.committer.OutputCommitterBasedFileCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class DefaultHadoopFileCommitterFactoryHadoopV2Test {

	@Test
	public void testCreateFileOutputCommitter() throws IOException {
		int maxParallelism = 20;
		int subtaskIndex = 1;
		int partCount = 22500;
		int expectedAttemptCount = Integer.MAX_VALUE / maxParallelism + partCount;

		String targetFilePath = "hdfs://localhost:9000/tmp/target/part-1-22500.txt";

		DefaultHadoopFileCommitterFactory factory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer = factory.create(
			new Configuration(),
			new Path(targetFilePath),
			maxParallelism,
			subtaskIndex,
			partCount);

		assertEquals(OutputCommitterBasedFileCommitter.class, committer.getClass());
		assertEquals(FileOutputCommitter.class, ((OutputCommitterBasedFileCommitter) committer).getOutputCommitter().getClass());
		assertEquals(new Path(targetFilePath), committer.getTargetFilePath());
		assertEquals(new Path(String.format(
			"hdfs://localhost:9000/tmp/target/_temporary/%d/_temporary/attempt_part-1-22500.txt_0000_r_000000_0/part-1-22500.txt",
			expectedAttemptCount)), committer.getInProgressFilePath());
	}
}
