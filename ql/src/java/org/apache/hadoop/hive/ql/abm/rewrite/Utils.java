/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
