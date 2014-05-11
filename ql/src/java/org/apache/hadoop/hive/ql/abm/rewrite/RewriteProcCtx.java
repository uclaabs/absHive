package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class RewriteProcCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>> condIndex =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>>();
  private final HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>> gbyIdIndex =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>>();

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>> transform =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>>();

  private final HashMap<GroupByOperator, GroupByLineage> lineages =
      new HashMap<GroupByOperator, GroupByLineage>();
  private final HashMap<GroupByOperator, GroupByResult> results =
      new HashMap<GroupByOperator, GroupByResult>();

  private final TraceProcCtx tctx;

  public RewriteProcCtx(TraceProcCtx ctx) {
    tctx = ctx;
  }

  public AggregateInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    return tctx.getLineage(op, internalName);
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

  public void addGbyIdColumnIndex(Operator<? extends OperatorDesc> op, GroupByOperator gby, int index) {
    HashMap<GroupByOperator, Integer> map = gbyIdIndex.get(op);
    if (map == null) {
      map = new HashMap<GroupByOperator, Integer>();
      gbyIdIndex.put(op, map);
    }
    map.put(gby, index);
  }

  public HashSet<Integer> getSpecialColumnIndexes(Operator<? extends OperatorDesc> op) {
    HashSet<Integer> ret = new HashSet<Integer>();

    List<Integer> condIndexes = getCondColumnIndexes(op);
    if (condIndexes != null) {
      ret.addAll(condIndexes);
    }

    Map<GroupByOperator, Integer> idIndexMap = getGbyIdColumnIndexes(op);
    if (idIndexMap != null) {
      ret.addAll(idIndexMap.values());
    }


    return ret;
  }

  public void putGroupByLineage(GroupByOperator gby, GroupByLineage lineage) {
    lineages.put(gby, lineage);
  }

  public GroupByLineage getGroupByLineage(GroupByOperator gby) {
    return lineages.get(gby);
  }

  public void putGroupByResult(GroupByOperator gby, GroupByResult lineage) {
    results.put(gby, lineage);
  }

  public GroupByResult getGroupByResult(GroupByOperator gby) {
    return results.get(gby);
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
