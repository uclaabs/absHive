package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class TraceProcCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, HashMap<String, AggregateInfo>> lineages =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<String, AggregateInfo>>();

  private final HashMap<Operator<? extends OperatorDesc>, HashSet<AggregateInfo>> conditions =
      new HashMap<Operator<? extends OperatorDesc>, HashSet<AggregateInfo>>();
  private final HashSet<AggregateInfo> allAggrs = new HashSet<AggregateInfo>();

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

  public void addLineage(Operator<? extends OperatorDesc> op, String internalName, AggregateInfo linfo) {
    HashMap<String, AggregateInfo> lineage = lineages.get(op);
    if (lineage == null) {
      lineage = new HashMap<String, AggregateInfo>();
      lineages.put(op, lineage);
    }
    lineage.put(internalName, linfo);
  }

  public Set<AggregateInfo> getConditions(Operator<? extends OperatorDesc> op) {
    return conditions.get(op);
  }

  public Set<AggregateInfo> getAllAggregatesAsConditions() {
    return allAggrs;
  }

  public void addCondition(Operator<? extends OperatorDesc> op,
      AggregateInfo aggr) throws SemanticException {
    HashSet<AggregateInfo> conds = conditions.get(op);
    if (conds == null) {
      conds = new HashSet<AggregateInfo>();
      conditions.put(op, conds);
    }
    conds.add(aggr);
    allAggrs.add(aggr);
  }

  public void addConditions(Operator<? extends OperatorDesc> op,
      Set<AggregateInfo> aggrs) throws SemanticException {
    HashSet<AggregateInfo> conds = conditions.get(op);
    if (conds == null) {
      conds = new HashSet<AggregateInfo>();
      conditions.put(op, conds);
    }
    conds.addAll(aggrs);
    allAggrs.addAll(aggrs);
  }

  public LineageCtx getLineageCtx() {
    return lctx;
  }

  public ParseContext getParseContext() {
    return lctx.getParseContext();
  }

  public Map<String, ExprInfo> getOpColumnMapping(Operator<? extends OperatorDesc> op) {
    return lctx.get(op);
  }

  public OpParseContext getOpParseContext(Operator<? extends OperatorDesc> op) {
    return lctx.getParseContext().getOpParseCtx().get(op);
  }

  public boolean isSampled(Operator<? extends OperatorDesc> op) {
    return lctx.isSampled(op);
  }

}
