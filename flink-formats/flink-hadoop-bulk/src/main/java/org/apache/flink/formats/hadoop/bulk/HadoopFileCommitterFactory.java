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

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.Serializable;

/**
 * The factory to create the {@link HadoopFileCommitter}.
 */
@Internal
public interface HadoopFileCommitterFactory extends Serializable {

	/**
	 * Creates the corresponding Hadoop file committer according to the Hadoop
	 * configuration and the target path.
	 *
	 * @param configuration The hadoop configuration.
	 * @param targetFilePath The target path to commit.
	 * @return The corresponding Hadoop file committer.
	 */
	HadoopFileCommitter create(
		Configuration configuration,
		Path targetFilePath,
		int maxParallelism,
		int subtaskIndex,
		long partCount) throws IOException;

}
