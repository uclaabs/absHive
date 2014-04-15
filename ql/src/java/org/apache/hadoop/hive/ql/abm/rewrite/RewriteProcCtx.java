package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class RewriteProcCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, HashMap<String, LineageInfo>> lineageMap =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<String, LineageInfo>>();

  private final HashMap<Operator<? extends OperatorDesc>, HashSet<LineageInfo>> conditionLineage =
      new HashMap<Operator<? extends OperatorDesc>, HashSet<LineageInfo>>();
  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>> condColumnIndexes =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<Integer>>();

  private final HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>> transform =
      new HashMap<Operator<? extends OperatorDesc>, ArrayList<ExprNodeDesc>>();

  private final HashSet<Operator<? extends OperatorDesc>> lineageReaders =
      new HashSet<Operator<? extends OperatorDesc>>();

  private final LineageCtx lctx;

  public RewriteProcCtx(LineageCtx ctx) {
    lctx = ctx;
  }

  public LineageInfo getLineage(Operator<? extends OperatorDesc> op, String internalName) {
    HashMap<String, LineageInfo> lineage = lineageMap.get(op);
    if (lineage == null) {
      return null;
    }
    return lineage.get(internalName);
  }

  public void addLineage(Operator<? extends OperatorDesc> op, String internalName, LineageInfo linfo) {
    HashMap<String, LineageInfo> lineage = lineageMap.get(op);
    if (lineage == null) {
      lineage = new HashMap<String, LineageInfo>();
      lineageMap.put(op, lineage);
    }
    lineage.put(internalName, linfo);
  }

  public HashSet<LineageInfo> getConditions(Operator<? extends OperatorDesc> op) {
    return conditionLineage.get(op);
  }

  public void addConditionLineage(Operator<? extends OperatorDesc> op,
      LineageInfo condition) throws SemanticException {
    HashSet<LineageInfo> conds = conditionLineage.get(op);
    if (conds == null) {
      conds = new HashSet<LineageInfo>();
      conditionLineage.put(op, conds);
    }
    conds.add(condition);
  }

  public void addConditionLineages(Operator<? extends OperatorDesc> op,
      Set<LineageInfo> condition) throws SemanticException {
    HashSet<LineageInfo> conds = conditionLineage.get(op);
    if (conds == null) {
      conds = new HashSet<LineageInfo>();
      conditionLineage.put(op, conds);
    }
    conds.addAll(condition);
  }

  public Set<LineageInfo> getAllLineagesToWrite() {
    HashSet<LineageInfo> ret = new HashSet<LineageInfo>();
    for (Operator<? extends OperatorDesc> reader : lineageReaders) {
      ret.addAll(conditionLineage.get(reader));
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
    return lctx;
  }

  public ParseContext getParseContext() {
    return lctx.getParseContext();
  }

}
