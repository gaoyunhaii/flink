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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.task.IntegerTaskEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.encodeAndDecode;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes.
 */
public class NettyMessageServerSideSerializationTest {

	private final EmbeddedChannel channel = new EmbeddedChannel(
			new NettyMessage.NettyMessageEncoder(), // outbound messages
			new NettyMessage.NettyMessageDecoder()); // inbound messages

	private final Random random = new Random();

	@Test
	public void testEncodeDecode() {
		{
			NettyMessage.PartitionRequest expected = new NettyMessage.PartitionRequest(new ResultPartitionID(), random.nextInt(), new InputChannelID(), random.nextInt());
			NettyMessage.PartitionRequest actual = encodeAndDecode(expected, channel);

			assertEquals(expected.partitionId, actual.partitionId);
			assertEquals(expected.queueIndex, actual.queueIndex);
			assertEquals(expected.receiverId, actual.receiverId);
			assertEquals(expected.credit, actual.credit);
		}

		{
			NettyMessage.TaskEventRequest expected = new NettyMessage.TaskEventRequest(new IntegerTaskEvent(random.nextInt()), new ResultPartitionID(), new InputChannelID());
			NettyMessage.TaskEventRequest actual = encodeAndDecode(expected, channel);

			assertEquals(expected.event, actual.event);
			assertEquals(expected.partitionId, actual.partitionId);
			assertEquals(expected.receiverId, actual.receiverId);
		}

		{
			NettyMessage.CancelPartitionRequest expected = new NettyMessage.CancelPartitionRequest(new InputChannelID());
			NettyMessage.CancelPartitionRequest actual = encodeAndDecode(expected, channel);

			assertEquals(expected.receiverId, actual.receiverId);
		}

		{
			NettyMessage.CloseRequest expected = new NettyMessage.CloseRequest();
			NettyMessage.CloseRequest actual = encodeAndDecode(expected, channel);

			assertEquals(expected.getClass(), actual.getClass());
		}

		{
			NettyMessage.AddCredit expected = new NettyMessage.AddCredit(random.nextInt(Integer.MAX_VALUE) + 1, new InputChannelID());
			NettyMessage.AddCredit actual = encodeAndDecode(expected, channel);

			assertEquals(expected.credit, actual.credit);
			assertEquals(expected.receiverId, actual.receiverId);
		}
	}
}
