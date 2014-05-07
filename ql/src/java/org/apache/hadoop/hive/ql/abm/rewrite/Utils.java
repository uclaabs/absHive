package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class Utils {

  public static List<ExprNodeColumnDesc> generateColumnDescs(
      Operator<? extends OperatorDesc> op, Integer index) {
    if (index == null) {
      return new ArrayList<ExprNodeColumnDesc>();
    }
    return generateColumnDescs(op, Arrays.asList(index));
  }

  public static List<ExprNodeColumnDesc> generateColumnDescs(
      Operator<? extends OperatorDesc> op, List<Integer> indexes) {
    ArrayList<ExprNodeColumnDesc> ret = new ArrayList<ExprNodeColumnDesc>();
    if (indexes != null) {
      System.out.println(op.toString() + " " + indexes);
      for (int index : indexes) {
        ColumnInfo colInfo = op.getSchema().getSignature().get(index);
        ret.add(new ExprNodeColumnDesc(colInfo.getType(), colInfo
            .getInternalName(), colInfo.getTabAlias(), colInfo
            .getIsVirtualCol()));
      }
    }
    return ret;
  }

  public static String getColumnInternalName(int pos) {
    return "__col" + pos;
  }

  public static class Pair<L, R> implements Comparable<Pair<L, R>> {
    /** Left object */
    public final L left;
    /** Right object */
    public final R right;

    public Pair(L lhs, R rhs) {
      left = lhs;
      right = rhs;
    }

    // -----------------------------------------------------------------------
    /**
     * <p>
     * Compares the pair based on the left element followed by the right element. The types must be
     * {@code Comparable}.
     * </p>
     *
     * @param other
     *          the other pair, not null
     * @return negative if this is less, zero if equal, positive if greater
     */
    public int compareTo(Pair<L, R> other) {
      return new CompareToBuilder().append(left, other.left)
          .append(right, other.right).toComparison();
    }

    /**
     * <p>
     * Compares this pair to another based on the two elements.
     * </p>
     *
     * @param obj
     *          the object to compare to, null returns false
     * @return true if the elements of the pair are equal
     */
    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof Pair<?, ?>) {
        Pair<?, ?> other = (Pair<?, ?>) obj;
        return left.equals(other.left) && right.equals(other.right);
      }
      return false;
    }

    /**
     * <p>
     * Returns a suitable hash code. The hash code follows the definition in {@code Map.Entry}.
     * </p>
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
      // see Map.Entry API specification
      return (left == null ? 0 : left.hashCode()) ^
          (right == null ? 0 : right.hashCode());
    }

    /**
     * <p>
     * Returns a String representation of this pair using the format {@code ($left,$right)}.
     * </p>
     *
     * @return a string describing this object, not null
     */
    @Override
    public String toString() {
      return new StringBuilder().append('(').append(left).append(',').append(right)
          .append(')').toString();
    }

  }

}
