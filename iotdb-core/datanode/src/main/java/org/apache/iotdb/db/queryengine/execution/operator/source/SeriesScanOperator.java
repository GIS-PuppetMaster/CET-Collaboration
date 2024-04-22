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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.execution.ReadFromCloudFlag;
import org.apache.iotdb.db.queryengine.plan.execution.ReceiveTsBlock;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.*;
import org.apache.iotdb.tsfile.utils.Binary;
import zyh.service.SendData;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.Optional;

public class SeriesScanOperator extends AbstractDataSourceOperator {

  private final TsBlockBuilder builder;
  private boolean finished = false;
  private  boolean flag_boolean=false;
  private  boolean flag_binary=false;
  private  static TsBlock rev_tsblock=null;
  private static Boolean Isrev = false;
  private static Boolean finish_rev =false;
  private  static boolean finish_boolean=false;
  private  static boolean finish_binary=false;


  public SeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext());
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
    boolean[] values1 = new boolean[] {true,false, false, true};
    Binary[] values2 = new Binary[]{new Binary("test1", StandardCharsets.UTF_8),new Binary("test2", StandardCharsets.UTF_8),new Binary("test3", StandardCharsets.UTF_8),new Binary("test4", StandardCharsets.UTF_8)};
    TimeColumn timeColumn = new TimeColumn(4, new long[] {1, 2, 3, 4});
    Column column1 = new BooleanColumn(4, Optional.empty(), values1);
    Column column2 = new BinaryColumn(4, Optional.empty(), values2);
//    this.rev_tsblock=null;
//    this.rev_tsblock=new TsBlock(4,timeColumn, column1,column2);


  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    if(ReadFromCloudFlag.getInstance().readFromCloudFlag){
      if(rev_tsblock==null&Isrev==false){
        Isrev=true;
        ReceiveTsBlock receive=new ReceiveTsBlock();
        TsBlock reveiveTsBlock = receive.receive();
        rev_tsblock=reveiveTsBlock;
        finish_rev=true;
      }
      while(finish_rev!=true){
        Thread.sleep(10);
      }
      if(seriesScanUtil.dataType== TSDataType.BOOLEAN){
        appendToBuilder(rev_tsblock);
        flag_boolean=true;
        flag_boolean=true;
      }else{
        appendToBuilder1(rev_tsblock);
        flag_binary=true;
        finish_binary=true;
      }

      if(flag_boolean==true&flag_binary==true){
        rev_tsblock=null;
        Isrev=false;
        finish_rev=false;
        finish_binary=false;
        flag_boolean=false;
      }
    }

    resultTsBlock = builder.build();
    builder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }

    if(ReadFromCloudFlag.getInstance().readFromCloudFlag){
      if(seriesScanUtil.dataType!= TSDataType.BOOLEAN&seriesScanUtil.dataType!= TSDataType.TEXT){
        System.out.println(seriesScanUtil.dataType );
        System.out.println(false);
        finished=true;
        return false;
      }
      if(flag_boolean==true&seriesScanUtil.dataType== TSDataType.BOOLEAN){
        System.out.println(seriesScanUtil.dataType );
        System.out.println(false);
        finished=true;
        return false;
      }
      if(flag_binary==true&seriesScanUtil.dataType== TSDataType.TEXT){
        System.out.println(seriesScanUtil.dataType );
        System.out.println(false);
        finished=true;
        return false;
      }
//      if(ReadFromCloudFlag.getInstance().readFromCloudFlag){

//        if(flag==1){
//          ReadFromCloudFlag readFromCloudFlag=ReadFromCloudFlag.getInstance();
//          readFromCloudFlag.setFlag(false);
//        }

//        if(seriesScanUtil.dataType== TSDataType.BOOLEAN){
//          appendToBuilder(rev_tsblock);
//          flag_boolean=true;
//        }else{
//          appendToBuilder1(rev_tsblock);
//          flag_binary=true;
//        }
//      }
//      finished = builder.isEmpty();
//      return !finished;
      System.out.println(seriesScanUtil.dataType );
      System.out.println(true);
      return true;
    }else {

    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */

        if (!readPageData() && !readChunkData() && !readFileData()) {
          break;
        }
      } while (System.nanoTime() - start < maxRuntime && !builder.isFull());

      finished = builder.isEmpty();
//      System.out.println(!finished);
//      Thread.sleep(100);

      System.out.println(seriesScanUtil.dataType );
      System.out.println(!finished);
      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }

    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemory() - calculateMaxReturnSize();
  }

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();

      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
        return true;
      }
    }
    return false;
  }

  private void appendToBuilder(TsBlock tsBlock) {
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);
    Column column = tsBlock.getColumn(0);

    if (column.mayHaveNull()) {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
        builder.declarePosition();
      }
    } else {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.write(column, i);
        builder.declarePosition();
      }
    }
  }
  private void appendToBuilder1(TsBlock tsBlock) {
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);
    Column column = tsBlock.getColumn(1);

    if (column.mayHaveNull()) {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
        builder.declarePosition();
      }
    } else {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.write(column, i);
        builder.declarePosition();
      }
    }
  }
  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }
}
