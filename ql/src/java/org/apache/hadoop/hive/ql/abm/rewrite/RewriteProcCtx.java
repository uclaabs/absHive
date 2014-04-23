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

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>> condColumnIndexes =
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

  // If another (gby, index) exists, remove (gby, -1), as COUNT>0 is implied by the aggregate;
  private void condenseConditions() {
    Set<AggregateInfo> allAggrs = tctx.getAllAggregatesAsConditions();

    HashMap<GroupByOperator, TreeSet<AggregateInfo>> condensed =
        new HashMap<GroupByOperator, TreeSet<AggregateInfo>>();
    for (AggregateInfo aggr : allAggrs) {
      GroupByOperator gby = aggr.getGroupByOperator();
      TreeSet<AggregateInfo> aggrs = condensed.get(gby);
      if (aggrs == null) {
        aggrs = new TreeSet<AggregateInfo>();
        condensed.put(gby, aggrs);
      }
      aggrs.add(aggr);
    }
    for (TreeSet<AggregateInfo> aggrs : condensed.values()) {
      if (aggrs.size() == 1) {
        continue;
      }
      AggregateInfo toRemove = null;
      for (AggregateInfo aggr : aggrs) {
        if (aggr.getIndex() == -1) {
          toRemove = aggr;
          break;
        }
      }
      aggrs.remove(toRemove);
    }
  }

  public AggregateInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    return tctx.getLineage(op, internalName);
  }

  public Set<AggregateInfo> getConditions(Operator<? extends OperatorDesc> op) {
    return tctx.getConditions(op);
  }

  public Set<AggregateInfo> getAllLineagesToWrite() {
    HashSet<AggregateInfo> ret = new HashSet<AggregateInfo>();
    for (Operator<? extends OperatorDesc> reader : lineageReaders) {
      ret.addAll(tctx.getConditions(reader));
    }
    return ret;
  }

  public List<Integer> getCondColumnIndexes(Operator<? extends OperatorDesc> op) {
    return condColumnIndexes.get(op);
  }

  public void putCondColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    ArrayList<Integer> indexes = condColumnIndexes.get(op);
    if (indexes == null) {
      indexes = new ArrayList<Integer>();
      condColumnIndexes.put(op, indexes);
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

  public ParseContext getParseContext() {
    return tctx.getParseContext();
  }

  public OpParseContext getOpParseContext(Operator<? extends OperatorDesc> op) {
    return tctx.getOpParseContext(op);
  }

}
