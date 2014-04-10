package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class Utils {

  public static ArrayList<ExprNodeColumnDesc> generateColumnDescs(
      Operator<? extends OperatorDesc> op, Integer index) {
    if (index == null) {
      return new ArrayList<ExprNodeColumnDesc>();
    }
    return generateColumnDescs(op, Arrays.asList(index));
  }

  public static ArrayList<ExprNodeColumnDesc> generateColumnDescs(
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

}
