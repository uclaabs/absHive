package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class RewriteProcCtx implements NodeProcessorCtx {

  private final HashMap<GroupByOperator, TreeSet<AggregateInfo>> condensed =
      new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
  private final HashMap<Operator<? extends OperatorDesc>, Integer> innerCovIndex =
      new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private final HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>> interCovIndex =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>>();
  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>> condIndex =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>>();

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>> transform =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>>();

  private final HashSet<Operator<? extends OperatorDesc>> lineageReaders =
      new HashSet<Operator<? extends OperatorDesc>>();

  private final TraceProcCtx tctx;

  public RewriteProcCtx(TraceProcCtx ctx) {
    tctx = ctx;

    condenseConditions();
  }

  // 1. Group conditions by GroupByOperator;
  // 2. Sort them by indexes.
  private void condenseConditions() {
    Set<AggregateInfo> allAggrs = tctx.getAllAggregatesAsConditions();

    for (AggregateInfo aggr : allAggrs) {
      GroupByOperator gby = aggr.getGroupByOperator();
      TreeSet<AggregateInfo> aggrs = condensed.get(gby);
      if (aggrs == null) {
        aggrs = new TreeSet<AggregateInfo>();
        condensed.put(gby, aggrs);
      }
      aggrs.add(aggr);
    }
  }

  public AggregateInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    return tctx.getLineage(op, internalName);
  }

  public Set<AggregateInfo> getConditions(Operator<? extends OperatorDesc> op) {
    return tctx.getConditions(op);
  }

  public AggregateInfo[] getInnerGroupAggrs(GroupByOperator gby) {
    TreeSet<AggregateInfo> ret = condensed.get(gby);
    return ret.toArray(new AggregateInfo[ret.size()]);
  }

  public List<AggregateInfo[]> getInterGroupAggrs(GroupByOperator gby) {
    HashSet<GroupByOperator> gbys = new HashSet<GroupByOperator>();
    for (AggregateInfo cond : tctx.getConditions(gby)) {
      gbys.add(cond.getGroupByOperator());
    }

    ArrayList<AggregateInfo[]> ret = new ArrayList<AggregateInfo[]>();
    for (GroupByOperator gbyOp : gbys) {
      TreeSet<AggregateInfo> aggrs = condensed.get(gbyOp);
      ret.add(aggrs.toArray(new AggregateInfo[aggrs.size()]));
    }
    return ret;
  }

  public Integer getInnerCovIndex(Operator<? extends OperatorDesc> op) {
    return innerCovIndex.get(op);
  }

  public void putInnerCovIndex(Operator<? extends OperatorDesc> op, Integer index) {
    innerCovIndex.put(op, index);
  }

  public Integer getInterCovIndex(Operator<? extends OperatorDesc> op, GroupByOperator other) {
    HashMap<GroupByOperator, Integer> map = interCovIndex.get(op);
    if (map == null) {
      return null;
    }
    return map.get(other);
  }

  public void putInterCovIndex(Operator<? extends OperatorDesc> op, GroupByOperator other, Integer index) {
    HashMap<GroupByOperator, Integer> map = interCovIndex.get(op);
    if (map == null) {
      map = new HashMap<GroupByOperator, Integer>();
      interCovIndex.put(op, map);
    }
    map.put(other, index);
  }

  public Set<AggregateInfo> getAllLineagesToWrite() {
    HashSet<AggregateInfo> ret = new HashSet<AggregateInfo>();
    for (Operator<? extends OperatorDesc> reader : lineageReaders) {
      ret.addAll(tctx.getConditions(reader));
    }
    return ret;
  }

  public List<Integer> getCondColumnIndexes(Operator<? extends OperatorDesc> op) {
    return condIndex.get(op);
  }

  public void putCondColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    ArrayList<Integer> indexes = condIndex.get(op);
    if (indexes == null) {
      indexes = new ArrayList<Integer>();
      condIndex.put(op, indexes);
    }
    indexes.add(index);
  }

  public void addToLineageReaders(Operator<? extends OperatorDesc> op) {
    lineageReaders.add(op);
  }

  public HashSet<Operator<? extends OperatorDesc>> getLineageReaders() {
    return lineageReaders;
  }

  public ArrayList<ExprNodeDesc> getTransform(Operator<? extends OperatorDesc> filter) {
    return transform.get(filter);
  }

  public void addTransform(Operator<? extends OperatorDesc> filter, ExprNodeDesc func) {
    ArrayList<ExprNodeDesc> funcs = transform.get(filter);
    if (funcs == null) {
      funcs = new ArrayList<ExprNodeDesc>();
      transform.put(filter, funcs);
    }
    funcs.add(func);
  }

  public LineageCtx getLineageCtx() {
    return tctx.getLineageCtx();
  }

  public boolean isSampled(Operator<? extends OperatorDesc> op) {
    return tctx.isSampled(op);
  }

  public ParseContext getParseContext() {
    return tctx.getParseContext();
  }

  public OpParseContext getOpParseContext(Operator<? extends OperatorDesc> op) {
    return tctx.getOpParseContext(op);
  }

}
