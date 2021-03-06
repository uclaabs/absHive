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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class TraceProcCtx implements NodeProcessorCtx {

  private Operator<? extends OperatorDesc> sinkOp = null;

  private final HashMap<Operator<? extends OperatorDesc>, HashMap<String, AggregateInfo>> lineages =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<String, AggregateInfo>>();

  private final HashMap<Operator<? extends OperatorDesc>, ConditionAnnotation> conditions =
      new HashMap<Operator<? extends OperatorDesc>, ConditionAnnotation>();

  private final HashMap<GroupByOperator, Operator<? extends OperatorDesc>> lastUsedBy =
      new HashMap<GroupByOperator, Operator<? extends OperatorDesc>>();

  private final HashSet<GroupByOperator> continuous = new HashSet<GroupByOperator>();

  private final LineageCtx lctx;

  public TraceProcCtx(LineageCtx ctx) {
    lctx = ctx;
  }

  public AggregateInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    HashMap<String, AggregateInfo> lineage = lineages.get(op);
    if (lineage == null) {
      return null;
    }
    return lineage.get(internalName);
  }

  public void putLineage(Operator<? extends OperatorDesc> op, String internalName,
      AggregateInfo linfo) {
    HashMap<String, AggregateInfo> lineage = lineages.get(op);
    if (lineage == null) {
      lineage = new HashMap<String, AggregateInfo>();
      lineages.put(op, lineage);
    }
    lineage.put(internalName, linfo);
  }

  public ConditionAnnotation getCondition(Operator<? extends OperatorDesc> op) {
    return conditions.get(op);
  }

  public void addCondition(Operator<? extends OperatorDesc> op, ConditionAnnotation cond) {
    ConditionAnnotation anno = getOrCreateCondAnno(op);
    anno.combine(cond);
  }

  public void addCondition(Operator<? extends OperatorDesc> op, ComparisonTransform pred) {
    ConditionAnnotation anno = getOrCreateCondAnno(op);
    anno.conditionOn(pred);
    for (AggregateInfo aggr : pred.getAggregatesInvolved()) {
      usedAt(aggr.getGroupByOperator(), op);
    }
  }

  public void addReturn(Operator<? extends OperatorDesc> op, AggregateInfo ai) {
    ConditionAnnotation anno = getOrCreateCondAnno(op);
    anno.returnVal(ai);
//    usedAt(ai.getGroupByOperator(), op);
  }

  public void setContinuous(GroupByOperator gby) {
    continuous.add(gby);
  }

  public void groupByAt(GroupByOperator gby) {
    ConditionAnnotation anno = getOrCreateCondAnno(gby);
    anno.groupByAt(gby, continuous.contains(gby));
  }

  private ConditionAnnotation getOrCreateCondAnno(Operator<? extends OperatorDesc> op) {
    ConditionAnnotation anno = conditions.get(op);
    if (anno == null) {
      anno = new ConditionAnnotation();
      conditions.put(op, anno);

      sinkOp = op;
    }
    return anno;
  }

  public boolean lastUsedBy(GroupByOperator gby, Operator<? extends OperatorDesc> op) {
    return op.equals(lastUsedBy.get(gby));
  }

  public void usedAt(GroupByOperator gby, Operator<? extends OperatorDesc> op) {
    lastUsedBy.put(gby, op);
  }

  public Operator<? extends OperatorDesc> getSinkOp() {
    return sinkOp;
  }

  public LineageCtx getLineageCtx() {
    return lctx;
  }

  public ParseContext getParseContext() {
    return lctx.getParseContext();
  }

  public Map<String, ExprInfo> getOpColumnMapping(Operator<? extends OperatorDesc> op) {
    return lctx.getLineages(op);
  }

  public OpParseContext getOpParseContext(Operator<? extends OperatorDesc> op) {
    return lctx.getParseContext().getOpParseCtx().get(op);
  }

  public boolean isUncertain(Operator<? extends OperatorDesc> op) {
    return lctx.isUncertain(op);
  }

  public boolean isAnnotatedWithSrv(Operator<? extends OperatorDesc> op) {
    return lctx.isAnnotatedWithSrv(op);
  }

}
