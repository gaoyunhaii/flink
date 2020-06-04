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

package org.apache.flink.formats.hadoop.bulk.committer.cluster;

import org.apache.flink.testutils.s3.S3TestCredentials;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * Utility class for testing with S3 FileSystem.
 */
public class S3Cluster {
	private final Path s3RootPath;

	public S3Cluster() {
		String schema = loadAvailableS3Schema();
		s3RootPath = new Path(S3TestCredentials.getTestBucketUriWithScheme(schema));
	}

	public Path newFolder() {
		return new Path(s3RootPath + "/" + UUID.randomUUID().toString());
	}

	public Configuration getConfiguration() {
		Configuration configuration = new Configuration();

		configuration.set("fs.s3a.access.key", S3TestCredentials.getS3AccessKey());
		configuration.set("fs.s3a.secret.key", S3TestCredentials.getS3SecretKey());

		configuration.set("fs.s3.awsAccessKeyId", S3TestCredentials.getS3AccessKey());
		configuration.set("fs.s3.awsSecretAccessKey", S3TestCredentials.getS3SecretKey());

		configuration.set("fs.s3n.awsAccessKeyId", S3TestCredentials.getS3AccessKey());
		configuration.set("fs.s3n.awsSecretAccessKey", S3TestCredentials.getS3SecretKey());

		return configuration;
	}

	private String loadAvailableS3Schema() {
		try {
			Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem");
			return "s3a";
		} catch (ClassNotFoundException e) {
			// Ignore the exception.
		}

		try {
			Class.forName("org.apache.hadoop.fs.s3native.NativeS3FileSystem");
			return "s3n";
		} catch (ClassNotFoundException e) {
			// Ignore the exception.
		}

		fail("No S3 filesystem upload test executed. Please activate the " +
			"'include_hadoop_aws' build profile or set '-Dinclude_hadoop_aws' during build " +
			"(Hadoop >= 2.6 moved S3 filesystems out of hadoop-common).");

		// Ease the compiler
		return null;
	}

}
