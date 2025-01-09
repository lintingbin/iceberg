/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;

public class TableRefreshSource extends RichSourceFunction<Schema> implements CheckpointedFunction {
  private final TableLoader tableLoader;
  private Table table = null;
  private Schema lastSchema = null;
  private boolean needRefresh = false;
  private boolean isRunning = true;

  public TableRefreshSource(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  @Override
  public void initializeState(FunctionInitializationContext context) {
    tableLoader.open();
    table = tableLoader.loadTable();
    lastSchema = table.schema();
  }

  @Override
  public void run(SourceContext<Schema> ctx) throws Exception {
    ctx.collect(lastSchema);
    while (isRunning) {
      if (needRefresh) {
        table.refresh();
        Schema curSchema = table.schema();
        if (!curSchema.sameSchema(lastSchema)) {
          ctx.collect(curSchema);
          lastSchema = curSchema;
        }
        needRefresh = false;
      }
      TimeUnit.SECONDS.sleep(1);
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) {
    needRefresh = true;
  }

  @Override
  public void cancel() {
    isRunning = false;
    if (tableLoader != null) {
      try {
        tableLoader.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
