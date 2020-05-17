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

package org.apache.flink.formats.hadoop.bulk.committer;

import org.apache.flink.formats.hadoop.bulk.AbstractHadoopFileCommitterTest;
import org.apache.flink.testutils.s3.S3TestCredentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 *
 */
public class OutputCommitterBasedFileCommitterStagingTest extends AbstractHadoopFileCommitterTest {

	@Override
	protected Configuration getConfiguration() {
		S3TestCredentials.assumeCredentialsAvailable();

		Configuration configuration = new Configuration();
		configuration.set("fs.s3a.access.key", S3TestCredentials.getS3AccessKey());
		configuration.set("fs.s3a.secret.key", S3TestCredentials.getS3SecretKey());
		configuration.set("fs.default.name", S3TestCredentials.getTestBucketUri());
		configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
		configuration.set("fs.s3a.committer.name", "staging");
		configuration.set("fs.s3a.committer.staging.unique-filenames", "false");
		return configuration;
	}

	@Override
	protected Path getBasePath() throws IOException {
		S3TestCredentials.assumeCredentialsAvailable();

		Path path = new Path(S3TestCredentials.getTestBucketUriWithScheme("s3a") + "staging-" + UUID.randomUUID());
		System.out.println(path);
		return path;
	}
}
