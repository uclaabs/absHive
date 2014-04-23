package org.apache.hadoop.hive.ql.abm.rewrite;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class AggregateInfo implements Comparable<AggregateInfo> {

  private final GroupByOperator gby;
  private final int index;
  // Whether the set of tuples contributing to this aggregate is deterministic
  private final boolean deterministic;
  private TypeInfo type = null;

  public AggregateInfo(GroupByOperator gbyOp, int ind, boolean det) {
    gby = gbyOp;
    index = ind;
    deterministic = det;
  }

  public GroupByOperator getGroupByOperator() {
    return gby;
  }

  public int getIndex() {
    return index;
  }

  public boolean isDeterministic() {
    return deterministic;
  }

  public TypeInfo getTypeInfo() throws SemanticException {
    if (type == null) { // resolve the type
      AggregationDesc desc = gby.getConf().getAggregators().get(index);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          desc.getGenericUDAFEvaluator(), desc.getMode(), desc.getParameters());
      type = udaf.returnType;
    }
    return type;
  }

  @Override
  public int hashCode() {
    return gby.hashCode() * 31 + index;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AggregateInfo)) {
      return false;
    }

    AggregateInfo info = (AggregateInfo) obj;
    return gby.equals(info.gby) && index == info.index;
  }

  @Override
  public int compareTo(AggregateInfo arg0) {
    int ret = Integer.parseInt(gby.getIdentifier()) - Integer.parseInt(arg0.gby.getIdentifier());
    if (ret == 0) {
      return index - arg0.index;
    }
    return ret;
  }

  @Override
  public String toString() {
    return gby.toString() + " : " + index;
  }

}
