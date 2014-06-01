package org.apache.hadoop.hive.ql.abm.algebra;

public class ComparisonTransform extends BinaryTransform {

  private final boolean greaterThanOrEqualTo;

  public ComparisonTransform(Transform left, Transform right, boolean gteq) {
    super(left, right);
    greaterThanOrEqualTo = gteq;
  }

  public boolean isAscending() {
    if (lhs.getAggregatesInvolved().isEmpty()) {
      return greaterThanOrEqualTo;
    }
    assert !rhs.getAggregatesInvolved().isEmpty();
    return !greaterThanOrEqualTo;
  }

}
