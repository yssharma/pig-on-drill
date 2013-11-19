/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.drill.exec.physical.impl.broadcastsender;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.BroadcastSender;
import org.apache.drill.exec.physical.impl.RootExec;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.GeneralRPCProtos;
import org.apache.drill.exec.record.FragmentWritableBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.BaseRpcOutcomeListener;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.bit.BitTunnel;
import org.apache.drill.exec.work.foreman.ErrorHelper;

import java.util.List;

import static org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class BroadcastSenderRootExec implements RootExec {
  private final FragmentContext context;
  private final BroadcastSender config;
  private final BitTunnel[] tunnels;
  private final ExecProtos.FragmentHandle handle;
  private volatile boolean ok;
  private final RecordBatch incoming;
  private final StatusHandler statusHandler;

  public BroadcastSenderRootExec(FragmentContext context,
                                 RecordBatch incoming,
                                 BroadcastSender config) {
    this.ok = true;
    this.context = context;
    this.incoming = incoming;
    this.statusHandler = new StatusHandler();
    this.config = config;
    this.handle = context.getHandle();
    List<DrillbitEndpoint> destinations = config.getDestinations();
    this.tunnels = new BitTunnel[destinations.size()];
    for(int i = 0; i < destinations.size(); ++i) {
      tunnels[i] = context.getCommunicator().getTunnel(destinations.get(i));
    }
  }

  @Override
  public boolean next() {
    if(!ok) {
      return false;
    }

    RecordBatch.IterOutcome out = incoming.next();
    logger.debug("Outcome of sender next {}", out);
    switch(out){
      case STOP:
      case NONE:
        for (int i = 0; i < tunnels.length; ++i) {
          FragmentWritableBatch b2 = FragmentWritableBatch.getEmptyLast(handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), config.getOppositeMajorFragmentId(), i);
          tunnels[i].sendRecordBatch(statusHandler, context, b2);
        }
        return false;

      case OK_NEW_SCHEMA:
      case OK:
        for (int i = 0; i < tunnels.length; ++i) {
          FragmentWritableBatch batch = new FragmentWritableBatch(false, handle.getQueryId(), handle.getMajorFragmentId(), handle.getMinorFragmentId(), config.getOppositeMajorFragmentId(), i, incoming.getWritableBatch());
          tunnels[i].sendRecordBatch(statusHandler, context, batch);
        }
        return true;

      case NOT_YET:
      default:
        throw new IllegalStateException();
    }
  }

  @Override
  public void stop() {
      ok = false;
      incoming.kill();
  }

  private class StatusHandler extends BaseRpcOutcomeListener<GeneralRPCProtos.Ack> {
    RpcException ex;

    @Override
    public void failed(RpcException ex) {
      logger.error("Failure while sending data to user.", ex);
      ErrorHelper.logAndConvertError(context.getIdentity(), "Failure while sending fragment to client.", ex, logger);
      ok = false;
      this.ex = ex;
    }

  }
}
