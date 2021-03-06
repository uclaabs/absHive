/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class RewriteProcCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, Integer> tidIndex =
      new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private final HashMap<Operator<? extends OperatorDesc>, Integer> countIndex =
  new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private final HashMap<Operator<? extends OperatorDesc>, Integer> lineageIndex =
  new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>> condIndex =
  new HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>>();
  private final HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>> gbyIdIndex =
  new HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>>();

  private final HashMap<GroupByOperator, HashMap<Integer, UdafPair>> evaluators =
      new HashMap<GroupByOperator, HashMap<Integer, UdafPair>>();

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>> transform =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>>();

  private final TraceProcCtx tctx;
  private ConditionAnnotation condAnno;
  private final boolean simpleQuery;

  public RewriteProcCtx(TraceProcCtx ctx) {
    tctx = ctx;
    condAnno = tctx.getCondition(tctx.getSinkOp());
    if (condAnno == null) {
      condAnno = new ConditionAnnotation();
    }
    simpleQuery = condAnno.isSimpleQuery();
  }

  public AggregateInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    return tctx.getLineage(op, internalName);
  }

  public void putLineage(Operator<? extends OperatorDesc> op, String internalName,
      AggregateInfo linfo) {
    tctx.putLineage(op, internalName, linfo);
  }

  public boolean withTid(Operator<? extends OperatorDesc> op) {
    return tctx.isAnnotatedWithSrv(op) && !simpleQuery;
  }

  public Integer getTidColumnIndex(Operator<? extends OperatorDesc> op) {
    return tidIndex.get(op);
  }

  public void putTidColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    tidIndex.put(op, index);
  }

  public Integer getCountColumnIndex(Operator<? extends OperatorDesc> op) {
    return countIndex.get(op);
  }

  public void putCountColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    countIndex.put(op, index);
  }

  public Integer getLineageColumnIndex(Operator<? extends OperatorDesc> op) {
    return lineageIndex.get(op);
  }

  public void putLineageColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    lineageIndex.put(op, index);
  }

  public List<Integer> getCondColumnIndexes(Operator<? extends OperatorDesc> op) {
    return condIndex.get(op);
  }

  public void addCondColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    ArrayList<Integer> indexes = condIndex.get(op);
    if (indexes == null) {
      indexes = new ArrayList<Integer>();
      condIndex.put(op, indexes);
    }
    indexes.add(index);
  }

  public Map<GroupByOperator, Integer> getGbyIdColumnIndexes(Operator<? extends OperatorDesc> op) {
    return gbyIdIndex.get(op);
  }

  public Integer getGbyIdColumnIndex(Operator<? extends OperatorDesc> op, GroupByOperator gby) {
    HashMap<GroupByOperator, Integer> map = gbyIdIndex.get(op);
    if (map == null) {
      return null;
    }
    return map.get(gby);
  }

  public void addGbyIdColumnIndex(Operator<? extends OperatorDesc> op, GroupByOperator gby,
      int index) {
    HashMap<GroupByOperator, Integer> map = gbyIdIndex.get(op);
    if (map == null) {
      map = new HashMap<GroupByOperator, Integer>();
      gbyIdIndex.put(op, map);
    }
    map.put(gby, index);
  }

  public HashSet<Integer> getSpecialColumnIndexes(Operator<? extends OperatorDesc> op) {
    HashSet<Integer> ret = new HashSet<Integer>();

    Integer tidIndex = getTidColumnIndex(op);
    if (tidIndex != null) {
      ret.add(tidIndex);
    }

    Integer countIndex = getCountColumnIndex(op);
    if (countIndex != null) {
      ret.add(countIndex);
    }

    Integer lineageIndex = getLineageColumnIndex(op);
    if (lineageIndex != null) {
      ret.add(lineageIndex);
    }

    List<Integer> condIndexes = getCondColumnIndexes(op);
    if (condIndexes != null) {
      ret.addAll(condIndexes);
    }

    Map<GroupByOperator, Integer> gbyIdIndexMap = getGbyIdColumnIndexes(op);
    if (gbyIdIndexMap != null) {
      ret.addAll(gbyIdIndexMap.values());
    }

    return ret;
  }

  public boolean isContinuous(GroupByOperator gby) {
    return condAnno.isContinuous(gby);
  }

  public void putGroupByInput(GroupByOperator gby, SelectOperator input) {
    condAnno.putGroupByInput(gby, input);
  }

  public void putGroupByOutput(GroupByOperator gby, SelectOperator output) {
    condAnno.putGroupByOutput(gby, output);
  }

  public ArrayList<ExprNodeDesc> getTransform(Operator<? extends OperatorDesc> filter) {
    return transform.get(filter);
  }

  public boolean lastUsedBy(GroupByOperator gby, Operator<? extends OperatorDesc> op) {
    return tctx.lastUsedBy(gby, op);
  }

  public void usedAt(GroupByOperator gby, Operator<? extends OperatorDesc> op) {
    tctx.usedAt(gby, op);
  }

  public void addTransform(Operator<? extends OperatorDesc> filter, ExprNodeDesc func) {
    ArrayList<ExprNodeDesc> funcs = transform.get(filter);
    if (funcs == null) {
      funcs = new ArrayList<ExprNodeDesc>();
      transform.put(filter, funcs);
    }
    funcs.add(func);
  }

  public int getAggregateId(AggregateInfo ai) {
    return condAnno.getAggregateId(ai);
  }

  public List<Boolean> getCondFlags(GroupByOperator gby) {
    return condAnno.getCondFlags(gby);
  }

  public void setupMCSim(SelectOperator select, int[] aggrColIdxs) {
    condAnno.setupMCSim(select, aggrColIdxs);
  }

  public LineageCtx getLineageCtx() {
    return tctx.getLineageCtx();
  }

  public boolean isUncertain(Operator<? extends OperatorDesc> op) {
    return tctx.isUncertain(op);
  }

  public boolean conditionNeedsSimulation() {
    return condAnno.conditionNeedsSimulation();
  }

  public ParseContext getParseContext() {
    return tctx.getParseContext();
  }

  public OpParseContext getOpParseContext(Operator<? extends OperatorDesc> op) {
    return tctx.getOpParseContext(op);
  }

  public void putEvaluator(GroupByOperator gby, int index, GenericUDAFEvaluator udafEvaluator, String udafName) {
    HashMap<Integer, UdafPair> map = evaluators.get(gby);
    if (map == null) {
      map = new HashMap<Integer, UdafPair>();
      evaluators.put(gby, map);
    }
    map.put(index, new UdafPair(udafEvaluator, udafName));
  }

  public UdafPair getEvaluator(GroupByOperator gby, int index) {
    HashMap<Integer, UdafPair> map = evaluators.get(gby);
    if (map == null) {
      return null;
    }
    return map.get(index);
  }

}

class UdafPair {

  public final GenericUDAFEvaluator udafEvaluator;
  public String udafName;

  public UdafPair(GenericUDAFEvaluator udafEvaluator, String udafName) {
    this.udafEvaluator = udafEvaluator;
    this.udafName = udafName;
  }

}
