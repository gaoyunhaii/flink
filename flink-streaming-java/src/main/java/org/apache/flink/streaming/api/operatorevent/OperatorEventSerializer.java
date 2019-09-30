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

package org.apache.flink.streaming.api.operatorevent;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

/**
 *
 */
public class OperatorEventSerializer extends TypeSerializer<AbstractOperatorEvent> {

	private static final long serialVersionUID = 1L;

	public static final OperatorEventSerializer INSTANCE = new OperatorEventSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<AbstractOperatorEvent> duplicate() {
		return new OperatorEventSerializer();
	}

	@Override
	public AbstractOperatorEvent createInstance() {
		return null;
	}

	@Override
	public AbstractOperatorEvent copy(AbstractOperatorEvent from) {
		try {
			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
			serialize(from, dataOutputSerializer);

			DataInputDeserializer deserializerInput = new DataInputDeserializer(dataOutputSerializer.wrapAsByteBuffer());
			return deserialize(deserializerInput);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}

		return null;
	}

	@Override
	public AbstractOperatorEvent copy(AbstractOperatorEvent from, AbstractOperatorEvent reuse) {
		try {
			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);
			serialize(from, dataOutputSerializer);

			DataInputDeserializer deserializerInput = new DataInputDeserializer(dataOutputSerializer.wrapAsByteBuffer());
			return deserialize(reuse, deserializerInput);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}

		return null;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(AbstractOperatorEvent record, DataOutputView target) throws IOException {
		target.writeUTF(record.getClass().getName());
		record.write(target);
	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractOperatorEvent deserialize(DataInputView source) throws IOException {
		try {
			Class<? extends AbstractOperatorEvent> clazz = (Class<? extends AbstractOperatorEvent>) Thread.currentThread().getContextClassLoader().loadClass(source.readUTF());
			AbstractOperatorEvent event = clazz.newInstance();
			event.read(source);
			return event;
		} catch (Exception e) {
			throw new IOException("Failed to deserialize the operator event");
		}
	}

	@Override
	public AbstractOperatorEvent deserialize(AbstractOperatorEvent reuse, DataInputView source) throws IOException {
		try {
			source.readUTF();
			reuse.read(source);
			return reuse;
		} catch (Exception e) {
			ExceptionUtils.rethrow(e);
		}

		return null;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		AbstractOperatorEvent event = deserialize(source);
		serialize(event, target);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof OperatorEventSerializer;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public TypeSerializerSnapshot<AbstractOperatorEvent> snapshotConfiguration() {
		return new OperatorEventSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class OperatorEventSerializerSnapshot extends SimpleTypeSerializerSnapshot<AbstractOperatorEvent> {

		public OperatorEventSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
