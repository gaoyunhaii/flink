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

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.util.AbstractID;

import java.io.Serializable;

/**
 * The interface for the committable sink. It acts like a factory class that helps construct the {@link Writer}
 * and {@link CommitFunction}.
 * //TODO:: CommittableSink extend Function just for extracting split TypeInformation. Maybe we could build it from scratch in the future.
 * @param <T>        		The type of the sink's input.
 * @param <CommittableT>   	The type of handle that is ready to be committed to the external system.
 */
@PublicEvolving
public interface USink<T, CommittableT> extends Function, Serializable {

	Writer<T, CommittableT> createWriter(InitialContext context) throws Exception;

	CommitFunction<CommittableT> createCommitFunction();

	//TODO:: create the CommittableT serializer.

	/**
	 * Java Doc.
	 */
	interface InitialContext {

		boolean isRestored();

		int getSubtaskIndex();

		int getParallelism();

		//TODO :: key or non-key does the sink developer knows? TBV
		<S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception;

		<S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception;

		// TODO we could use the session id to clean up the un-managed CommittableT.
		//  The session id looks like (JobId-JobAttemptId-TaskId-TaskAttemptId)
		//  Both the bounded and un-bounded scenario would needs this information.
		AbstractID getSessionId();
	}
}
