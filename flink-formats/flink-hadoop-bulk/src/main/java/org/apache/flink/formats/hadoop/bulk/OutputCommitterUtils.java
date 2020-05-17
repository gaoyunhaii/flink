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

import org.apache.flink.api.java.typeutils.TypeExtractionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;

import java.lang.reflect.Method;

/**
 *
 */
public class OutputCommitterUtils {
	private static final String HADOOP_3_PATH_OUTPUT_COMMITTER_NAME =
		"org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter";

	private static final String PATH_OUTPUT_COMMITTER_WORKER_PATH_METHOD = "getWorkPath";

	private static final String HADOOP_2_FILE_OUTPUT_COMMITTER_NAME =
		"org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter";

	private static final String FILE_OUTPUT_COMMITTER_WORKER_PATH_METHOD = "getWorkPath";

	public static Path getTaskAttemptPath(OutputCommitter outputCommitter) {
		// If it is the path-based output committer
		if (TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), HADOOP_3_PATH_OUTPUT_COMMITTER_NAME)) {
			return invokeMethod(outputCommitter, PATH_OUTPUT_COMMITTER_WORKER_PATH_METHOD);
		} else if (TypeExtractionUtils.hasSuperclass(outputCommitter.getClass(), HADOOP_2_FILE_OUTPUT_COMMITTER_NAME)) {
			return invokeMethod(outputCommitter, FILE_OUTPUT_COMMITTER_WORKER_PATH_METHOD);
		}

		throw new UnsupportedOperationException(String.format(
			"Unsupported output committer of type %s",
			outputCommitter.getClass().toString()));
	}

	@SuppressWarnings("unchecked")
	private static <T> T invokeMethod(Object object, String methodName) {
		try {
			Method method = object.getClass().getMethod(methodName);
			return (T) method.invoke(object);
		} catch (Exception e) {
			throw new RuntimeException("", e);
		}
	}
}
