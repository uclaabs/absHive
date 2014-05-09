package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class TraceProcCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, HashMap<String, AggregateInfo>> lineages =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<String, AggregateInfo>>();

  private final HashMap<Operator<? extends OperatorDesc>, ConditionAnnotation> conditions =
      new HashMap<Operator<? extends OperatorDesc>, ConditionAnnotation>();

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

  public void putLineage(Operator<? extends OperatorDesc> op, String internalName, AggregateInfo linfo) {
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

  public void addCondition(Operator<? extends OperatorDesc> op, AggregateInfo aggr) {
    ConditionAnnotation anno = getOrCreateCondAnno(op);
    anno.conditionOn(aggr);
  }

  public void groupByAt(GroupByOperator gby) {
    ConditionAnnotation anno = getOrCreateCondAnno(gby);
    anno.groupByAt(gby);
  }

  private ConditionAnnotation getOrCreateCondAnno(Operator<? extends OperatorDesc> op) {
    ConditionAnnotation anno = conditions.get(op);
    if (anno == null) {
      anno = new ConditionAnnotation();
      conditions.put(op, anno);
    }
    return anno;
  }

  public void check() {
    for (ConditionAnnotation cond : conditions.values()) {
      cond.check();
    }
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

  public boolean isSampled(Operator<? extends OperatorDesc> op) {
    return lctx.isSampled(op);
  }

}
