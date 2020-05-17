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

import org.apache.flink.test.util.AbstractTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public abstract class AbstractHadoopFileCommitterTest extends AbstractTestBase {
	private static final List<String> CONTENTS = new ArrayList<>(Arrays.asList(
		"first line",
		"second line",
		"third line"));

	private static final int MAX_PARALLELISM = 20;

	protected abstract Configuration getConfiguration();

	protected abstract Path getBasePath() throws IOException;

	@Test
	public void testBasicCommit() throws IOException {
		Configuration configuration = getConfiguration();
		Path basePath = getBasePath();

		FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
		Path targetFilePath = new Path(basePath, "part-0-0.txt");
		DefaultHadoopFileCommitterFactory hadoopFileCommitterFactory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);
		writeFile(committer.getInProgressFilePath(), configuration);
		committer.preCommit();
		committer.commit();

		deleteTemporary(fileSystem, basePath);

		FileStatus[] files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 1, files.length);
		assertTrue("", files[0].isFile());
		assertEquals("part-0-0.txt", files[0].getPath().getName());
		List<String> written = readFile(fileSystem, files[0].getPath());
		assertEquals(CONTENTS, written);
	}

	@Test
	public void testReWriteAfterFailover() throws IOException {
		Configuration configuration = getConfiguration();
		Path basePath = getBasePath();

		FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
		Path targetFilePath = new Path(basePath, "part-0-0.txt");
		DefaultHadoopFileCommitterFactory hadoopFileCommitterFactory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);
		writeFile(committer.getInProgressFilePath(), configuration);

		// Now restart the progress
		committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);
		writeFile(committer.getInProgressFilePath(), configuration);
		committer.preCommit();
		committer.commit();

		// Remove _temporary if required
		deleteTemporary(fileSystem, basePath);

		FileStatus[] files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 1, files.length);
		assertTrue("", files[0].isFile());
		assertEquals("part-0-0.txt", files[0].getPath().getName());
		List<String> written = readFile(fileSystem, files[0].getPath());
		assertEquals(CONTENTS, written);
	}

	@Test
	public void testCommitAfterFailOver() throws IOException {
		Configuration configuration = getConfiguration();
		Path basePath = getBasePath();

		FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
		Path targetFilePath = new Path(basePath, "part-0-0.txt");
		DefaultHadoopFileCommitterFactory hadoopFileCommitterFactory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);
		writeFile(committer.getInProgressFilePath(), configuration);
		committer.preCommit();

		// Now re-creating the committer after failover
		committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);
		committer.commit();

		deleteTemporary(fileSystem, basePath);

		FileStatus[] files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 1, files.length);
		assertTrue("", files[0].isFile());
		assertEquals("part-0-0.txt", files[0].getPath().getName());
		List<String> written = readFile(fileSystem, files[0].getPath());
		assertEquals(CONTENTS, written);
	}

	@Test
	public void testReCommitAfterFailOver() throws IOException {
		Configuration configuration = getConfiguration();
		Path basePath = getBasePath();

		FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
		Path targetFilePath = new Path(basePath, "part-0-0.txt");
		DefaultHadoopFileCommitterFactory hadoopFileCommitterFactory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);
		writeFile(committer.getInProgressFilePath(), configuration);
		committer.preCommit();
		committer.commit();

		deleteTemporary(fileSystem, basePath);

		FileStatus[] files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 1, files.length);
		assertTrue("", files[0].isFile());
		assertEquals("part-0-0.txt", files[0].getPath().getName());
		List<String> written = readFile(fileSystem, files[0].getPath());
		assertEquals(CONTENTS, written);

		// Now re-creating the committer after failover
		committer = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath,
			MAX_PARALLELISM,
			0,
			0);

		try {
			committer.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}

		deleteTemporary(fileSystem, basePath);

		files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 1, files.length);
		assertTrue("", files[0].isFile());
		assertEquals("part-0-0.txt", files[0].getPath().getName());
		written = readFile(fileSystem, files[0].getPath());
		assertEquals(CONTENTS, written);
	}

	@Test
	public void testMultipleFilesCommit() throws IOException {
		Configuration configuration = getConfiguration();
		Path basePath = getBasePath();

		FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
		Path targetFilePath1 = new Path(basePath, "part-0-0.txt");
		Path targetFilePath2 = new Path(basePath, "part-1-1.txt");

		DefaultHadoopFileCommitterFactory hadoopFileCommitterFactory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer1 = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath1,
			MAX_PARALLELISM,
			0,
			0);
		HadoopFileCommitter committer2 = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath2,
			MAX_PARALLELISM,
			1,
			1);

		writeFile(committer1.getInProgressFilePath(), configuration);
		writeFile(committer2.getInProgressFilePath(), configuration);

		committer1.preCommit();
		committer1.commit();

		assertTrue("", fileSystem.exists(targetFilePath1));
		assertTrue("", fileSystem.isFile(targetFilePath1));
		assertFalse("", fileSystem.exists(targetFilePath2));
		List<String> written = readFile(fileSystem, targetFilePath1);
		assertEquals(CONTENTS, written);

		committer2.preCommit();
		committer2.commit();

		deleteTemporary(fileSystem, basePath);

		FileStatus[] files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 2, files.length);

		List<String> names = new ArrayList<>();
		for (FileStatus status : files) {
			names.add(status.getPath().getName());
		}
		Collections.sort(names);
		assertEquals(Arrays.asList("part-0-0.txt", "part-1-1.txt"), names);

		for (FileStatus status : files) {
			assertTrue("", status.isFile());
			written = readFile(fileSystem, status.getPath());
			assertEquals(CONTENTS, written);
		}
	}

	@Test
	public void testMultipleFilesCommit2() throws IOException {
		Configuration configuration = getConfiguration();
		Path basePath = getBasePath();

		FileSystem fileSystem = FileSystem.get(basePath.toUri(), configuration);
		Path targetFilePath1 = new Path(basePath, "part-0-0.txt");
		Path targetFilePath2 = new Path(basePath, "part-1-1.txt");

		DefaultHadoopFileCommitterFactory hadoopFileCommitterFactory = new DefaultHadoopFileCommitterFactory();
		HadoopFileCommitter committer1 = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath1,
			MAX_PARALLELISM,
			0,
			0);
		HadoopFileCommitter committer2 = hadoopFileCommitterFactory.create(
			configuration,
			targetFilePath2,
			MAX_PARALLELISM,
			1,
			1);

		writeFile(committer1.getInProgressFilePath(), configuration);
		writeFile(committer2.getInProgressFilePath(), configuration);

		committer1.preCommit();
		committer2.preCommit();

		committer1.commit();
		assertTrue("", fileSystem.exists(targetFilePath1));
		assertTrue("", fileSystem.isFile(targetFilePath1));
		assertFalse("", fileSystem.exists(targetFilePath2));
		List<String> written = readFile(fileSystem, targetFilePath1);
		assertEquals(CONTENTS, written);

		committer2.commit();

		deleteTemporary(fileSystem, basePath);

		FileStatus[] files = fileSystem.listStatus(basePath);
		assertEquals(Arrays.toString(files), 2, files.length);

		List<String> names = new ArrayList<>();
		for (FileStatus status : files) {
			names.add(status.getPath().getName());
		}
		Collections.sort(names);
		assertEquals(Arrays.asList("part-0-0.txt", "part-1-1.txt"), names);

		for (FileStatus status : files) {
			assertTrue("", status.isFile());
			written = readFile(fileSystem, status.getPath());
			assertEquals(CONTENTS, written);
		}
	}

	private void writeFile(Path path, Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);
		try (FSDataOutputStream fsDataOutputStream = fileSystem.create(path, true);
			 PrintWriter printWriter = new PrintWriter(fsDataOutputStream)) {

			for (String line : CONTENTS) {
				printWriter.println(line);
			}
		}
	}

	private List<String> readFile(FileSystem fileSystem, Path partFile) throws IOException {
		try (FSDataInputStream dataInputStream = fileSystem.open(partFile)) {
			List<String> lines = new ArrayList<>();
			BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
			String line = null;
			while ((line = reader.readLine()) != null) {
				lines.add(line);
			}

			return lines;
		}
	}

	private void deleteTemporary(FileSystem fileSystem, Path basePath) throws IOException {
		FileStatus[] files = fileSystem.listStatus(basePath);
		for (FileStatus file : files) {
			if (file.getPath().getName().startsWith("_")) {
				fileSystem.delete(file.getPath(), true);
			}
		}
	}
}
