package org.apache.hadoop.hive.ql.abm.lineage;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ExprProcCtx implements NodeProcessorCtx {

  private final Operator<? extends OperatorDesc> parent;
  private final LineageCtx ctx;
  private final ExprInfo info;

  public ExprProcCtx(Operator<? extends OperatorDesc> parentOp, LineageCtx lctx) {
    parent = parentOp;
    ctx = lctx;
    info = new ExprInfo(parent);
  }

  public Operator<? extends OperatorDesc> getParentOp() {
    return parent;
  }

  public LineageCtx getLineageCtx() {
    return ctx;
  }

  public ExprInfo getExprInfo() {
    return info;
  }

}
