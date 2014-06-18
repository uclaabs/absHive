package org.apache.hadoop.hive.ql.abm.algebra;

import org.apache.hadoop.hive.ql.abm.simulation.PredicateType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;

public class ComparisonTransform extends BinaryTransform {

  public static enum Comparator {
    LESS_THAN,
    LESS_THAN_EQUAL_TO,
    GREATER_THAN,
    GREATER_THAN_EQUAL_TO;

    public static Comparator get(GenericUDF udf) {
      if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
        return GREATER_THAN_EQUAL_TO;
      } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
        return LESS_THAN_EQUAL_TO;
      } else if (udf instanceof GenericUDFOPGreaterThan) {
        return GREATER_THAN;
      } else {
        assert (udf instanceof GenericUDFOPLessThan);
        return LESS_THAN;
      }
    }
  }

  private final Comparator comparator;

  public ComparisonTransform(Transform left, Transform right, Comparator comp) {
    super(left, right);
    comparator = comp;
  }

  public boolean isAscending() {
    if (!lhs.getAggregatesInvolved().isEmpty()) {
      return Comparator.GREATER_THAN == comparator
          || Comparator.GREATER_THAN_EQUAL_TO == comparator;
    }
    assert !rhs.getAggregatesInvolved().isEmpty();
    return Comparator.LESS_THAN == comparator || Comparator.LESS_THAN_EQUAL_TO == comparator;
  }

  public PredicateType getPredicateType() {
    if (getAggregatesInvolved().size() == 1) {
      switch (comparator) {
      case LESS_THAN:
        return PredicateType.SINGLE_LESS_THAN;
      case LESS_THAN_EQUAL_TO:
        return PredicateType.SINGLE_LESS_THAN_OR_EQUAL_TO;
      case GREATER_THAN:
        return PredicateType.SINGLE_GREATER_THAN;
      default: // case GREATER_THAN_EQUAL_TO:
        return PredicateType.SINGLE_GREATER_THAN_OR_EQUAL_TO;
      }
    } else {
      switch (comparator) {
      case LESS_THAN:
        return PredicateType.DOUBLE_LESS_THAN;
      case LESS_THAN_EQUAL_TO:
        return PredicateType.DOUBLE_LESS_THAN_OR_EQUAL_TO;
      case GREATER_THAN:
        return PredicateType.DOUBLE_GREATER_THAN;
      default: // case GREATER_THAN_EQUAL_TO:
        return PredicateType.DOUBLE_GREATER_THAN_OR_EQUAL_TO;
      }
    }
  }

}
