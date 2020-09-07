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

package org.apache.flink.test.sink.batch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.test.util.TestUtils;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;

public class DistFileSystemTest extends TestUtils {

	@Test
	public void testSerializeAndDeserialize() throws IOException {
		String s = "you are right";
		KryoSerializer<String> kryoSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());

		Path path = new Path("file:///tmp/serializable_test/file");
		FileSystem fileSystem = FileSystem.get(path.toUri());

		try (FSDataOutputStream outputStream = fileSystem.create(path, FileSystem.WriteMode.OVERWRITE)) {
			for (int i = 0; i < 10; ++i) {
				DataOutputSerializer buffer = new DataOutputSerializer(1024);
				kryoSerializer.serialize(s, buffer);
				outputStream.write(buffer.getSharedBuffer(), 0, buffer.length());
			}
		}

		try (FSDataInputStream inputStream = fileSystem.open(path)) {
			DataInputView input = new DataInputViewStreamWrapper(inputStream);

			while (true) {
				String ret = kryoSerializer.deserialize(input);
				System.out.println(ret);
			}
		} catch (EOFException e) {
			System.out.println("At the end of stream");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
