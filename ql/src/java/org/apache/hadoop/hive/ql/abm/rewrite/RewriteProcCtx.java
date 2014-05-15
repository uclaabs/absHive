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

public class RewriteProcCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, Integer> tidIndex =
      new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>> condIndex =
  new HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>>();
  private final HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>> gbyIdIndex =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<GroupByOperator, Integer>>();
  private final HashMap<Operator<? extends OperatorDesc>, Integer> lineageIndex =
  new HashMap<Operator<? extends OperatorDesc>, Integer>();

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>> transform =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>>();

  private final HashMap<GroupByOperator, SelectOperator> inputs =
      new HashMap<GroupByOperator, SelectOperator>();
  private final HashMap<GroupByOperator, SelectOperator> outputs =
      new HashMap<GroupByOperator, SelectOperator>();

  private final TraceProcCtx tctx;

  public RewriteProcCtx(TraceProcCtx ctx) {
    tctx = ctx;
  }

  public AggregateInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    return tctx.getLineage(op, internalName);
  }

  public boolean withTid(Operator<? extends OperatorDesc> op) {
    return isAnnotatedWithSrv(op);
  }

  public Integer getTidColumnIndex(Operator<? extends OperatorDesc> op) {
    return tidIndex.get(op);
  }

  public void putTidColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    tidIndex.put(op, index);
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

  public Integer getLineageColumnIndex(Operator<? extends OperatorDesc> op) {
    return lineageIndex.get(op);
  }

  public void putLineageColumnIndex(Operator<? extends OperatorDesc> op, int index) {
    lineageIndex.put(op, index);
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

    Integer lineageIndex = getLineageColumnIndex(op);
    if (lineageIndex != null) {
      ret.add(lineageIndex);
    }

    Map<GroupByOperator, Integer> gbyIdIndexMap = getGbyIdColumnIndexes(op);
    if (gbyIdIndexMap != null) {
      ret.addAll(gbyIdIndexMap.values());
    }

    List<Integer> condIndexes = getCondColumnIndexes(op);
    if (condIndexes != null) {
      ret.addAll(condIndexes);
    }

    return ret;
  }

  public void putGroupByInput(GroupByOperator gby, SelectOperator input) {
    input.getConf().cache(tctx.getCondition(tctx.getSinkOp()).getInputSize(gby));
    inputs.put(gby, input);
  }

  public SelectOperator getGroupByInput(GroupByOperator gby) {
    return inputs.get(gby);
  }

  public void putGroupByOutput(GroupByOperator gby, SelectOperator output) {
    output.getConf().cache(tctx.getCondition(tctx.getSinkOp()).getOutputSize(gby));
    outputs.put(gby, output);
  }

  public SelectOperator getGroupByOutput(GroupByOperator gby) {
    return outputs.get(gby);
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

  public boolean isUncertain(Operator<? extends OperatorDesc> op) {
    return tctx.isUncertain(op);
  }

  public boolean isAnnotatedWithSrv(Operator<? extends OperatorDesc> op) {
    return tctx.isAnnotatedWithSrv(op);
  }

  public ParseContext getParseContext() {
    return tctx.getParseContext();
  }

  public OpParseContext getOpParseContext(Operator<? extends OperatorDesc> op) {
    return tctx.getOpParseContext(op);
  }

}
