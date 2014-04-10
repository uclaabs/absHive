package org.apache.hadoop.hive.ql.abm.lineage;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ExprInfo {

  private final Operator<? extends OperatorDesc> operator;
  // We treat all contributing columns indifferently
  // wherever the columns appear in the expression, e.g.,
  // in CASE x WHEN y THEN z, x, y, z are indifferent.
  private final HashSet<String> columns = new HashSet<String>();
  private boolean deterministic = true;
  private boolean bijection = true;
  private boolean hasAggrOutput = false;

  public ExprInfo(Operator<? extends OperatorDesc> op) {
    operator = op;
  }

  public ExprInfo(Operator<? extends OperatorDesc> op, String column) {
    this(op);
    addColumn(column);
  }

  public void setDeterministic(boolean det) {
    deterministic = det;
    if (!deterministic) {
      bijection = false;
    }
  }

  public boolean isDeterministic() {
    return deterministic;
  }

  public void setBijection(boolean bij) {
    bijection = bij;
  }

  public boolean isBijection() {
    return bijection;
  }

  public void setHasAggrOutput(boolean aggr) {
    hasAggrOutput = aggr;
  }

  public boolean hasAggrOutput() {
    return hasAggrOutput;
  }

 //In general cases an expression where multiple parameters have columns
 // is not a bijection function, even if all the parameters are the same,
 // e.g., y = f(x, x).
 // For instance, in the case y = x - x, we cannot solve x from y.
  public void addColumn(String column) {
    if (!columns.isEmpty()) {
      bijection = false;
    }
    columns.add(column);
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return operator;
  }

  public Set<String> getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();

    buf.append("{");

    buf.append("Op: ");
    buf.append(operator.toString());

    buf.append(" Columns: ");
    buf.append(columns.toString());

    buf.append(" bij=" + bijection);
    buf.append(" det=" + deterministic);
    buf.append(" aggr=" + hasAggrOutput);

    buf.append("}");

    return buf.toString();
  }

}
