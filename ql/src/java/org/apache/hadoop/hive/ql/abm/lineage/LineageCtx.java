package org.apache.hadoop.hive.ql.abm.lineage;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class LineageCtx implements NodeProcessorCtx {

  private final HashMap<Operator<? extends OperatorDesc>, HashMap<String, ExprInfo>> lineage =
      new HashMap<Operator<? extends OperatorDesc>, HashMap<String, ExprInfo>>();
  private final HashSet<Operator<? extends OperatorDesc>> sampled =
      new HashSet<Operator<? extends OperatorDesc>>();
  private final ParseContext ctx;

  public LineageCtx(ParseContext pctx) {
    ctx = pctx;
  }

  public ParseContext getParseContext() {
    return ctx;
  }

  public void put(Operator<? extends OperatorDesc> op, String internalName, ExprInfo ctx) {
    HashMap<String, ExprInfo> map = lineage.get(op);
    if (map == null) {
      map = new HashMap<String, ExprInfo>();
      lineage.put(op, map);
    }
    map.put(internalName, ctx);
  }

  public ExprInfo get(Operator<? extends OperatorDesc> op, String internalName) {
    HashMap<String, ExprInfo> map = lineage.get(op);
    if (map != null) {
      return map.get(internalName);
    }
    return null;
  }

  public HashMap<String, ExprInfo> get(Operator<? extends OperatorDesc> op) {
    return lineage.get(op);
  }

  public void addSampled(Operator<? extends OperatorDesc> op) {
    sampled.add(op);
  }

  public boolean isSampled(Operator<? extends OperatorDesc> op) {
    return sampled.contains(op);
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();

    for (Entry<Operator<? extends OperatorDesc>, HashMap<String, ExprInfo>> entry : lineage.entrySet()) {
        ret.append(entry.getKey().toString());
        ret.append(":\n");
        ret.append(entry.getValue());
        ret.append("\n\n");
    }

    return ret.toString();
  }

}
