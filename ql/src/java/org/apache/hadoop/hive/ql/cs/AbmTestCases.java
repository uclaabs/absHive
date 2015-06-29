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

package org.apache.hadoop.hive.ql.cs;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class AbmTestCases {

  FileSinkOperator rootOp;
  static String lineageUDAFName = "lin_sum";

  public AbmTestCases(Operator<? extends OperatorDesc> sinkOp) {
    try {
      if (sinkOp instanceof FileSinkOperator) {
        rootOp = (FileSinkOperator) rootOp;
      }
    }
    catch (Exception e) {
      error("sinkOp is not FileSinkOperator");
    }
  }

  public void test(Operator<? extends OperatorDesc> op, int level) {
    //group by combination
    if (op instanceof GroupByOperator) {
      List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
      if (parents.size() == 1 && parents.get(0) instanceof ReduceSinkOperator) {
        List<Operator<? extends OperatorDesc>> grandParents = parents.get(0).getParentOperators();
        if (grandParents.size() == 1 && grandParents.get(0) instanceof GroupByOperator) {
          GroupByOperator gby1 = (GroupByOperator) grandParents.get(0);
          GroupByOperator gby2 = (GroupByOperator) op;

          testGby1PreSelect(gby1);
          testGby2PostSelect(gby2);

          testGby1ContainsLin(gby1);
          testGby2ContainsLin(gby2);

          //skip two levels
          test(gby1, level + 2);
        }
      }
    }

    if (op instanceof FilterOperator) {
      //testFilterPostSelect((FilterOperator)op);
    }
    if (op instanceof JoinOperator) {
      //testJoinPostSelect((JoinOperator)op);
    }

    testTidExistsFromSampledTable(op);

    List<Operator<? extends OperatorDesc>> lst = op.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        test(l, level + 1);
      }
    }

  }

  private void checkFirstColumnsMatchKeys(GroupByOperator op, SelectOperator selectOp) {
    Set<String> gbyKeys = op.getColumnExprMap().keySet();
    int keySize = gbyKeys.size();

    //firsts are the keys
    List<ColumnInfo> infos = selectOp.getSchema().getSignature();
    List<ExprNodeDesc> colList = selectOp.getConf().getColList();
    testBool(infos.size() == colList.size(), "infos and colList size mismatch", op);

    /*
    for (int i = 0; i< keySize; i++) {
      // TODO: not test duplicate, keys are not matched as they are renamed, use op.getColumnExprMap().ValueSet
      testBool(gbyKeys.contains(infos.get(i).getInternalName()), "first column " + i + " not contained in gbyKeys");
    }*/
  }

  private void testGby1PreSelect(GroupByOperator op) {
    List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
    if (parents.size() != 1) {
      error("Gby1 not exact 1 pre op");
    }
    else {
      testBool(parents.get(0) instanceof SelectOperator, "select not pre gby1", op);
    }

    SelectOperator selectOp = (SelectOperator) parents.get(0);
    checkFirstColumnsMatchKeys(op, selectOp);

    // TODO: skip aggrs
    //i = op.getConf().getKeys().size() + op.getConf().getAggregators().size(); not correct

    //assume last column is the tid or generated column
    List<ExprNodeDesc> colList = selectOp.getConf().getColList();

    String lastColumnName = colList.get(colList.size()-1).getExprString();
    testBool(lastColumnName.startsWith("__col") || lastColumnName.startsWith("tid"), "gby1 last col not tid", op);
  }

  private void testGby2PostSelect(GroupByOperator op) {
    List<Operator<? extends OperatorDesc>> childs = op.getChildOperators();
    if (childs.size() != 1) {
      error("Gby2 not exact 1 post op");
    }
    else {
      testBool(childs.get(0) instanceof SelectOperator, "select not post gby2", op);
    }

    SelectOperator selectOp = (SelectOperator) childs.get(0);
    checkFirstColumnsMatchKeys(op, selectOp);

  }

  private void testGby1ContainsLin(GroupByOperator op) {
    ArrayList<AggregationDesc> aggrs = op.getConf().getAggregators();
    String s = aggrs.get(aggrs.size()-1).getExprString();
    testBool(s.startsWith(lineageUDAFName + "(__col") || s.startsWith(lineageUDAFName + "(tid"), "gby1 last aggr not lin", op);
  }

  private void testGby2ContainsLin(GroupByOperator op) {
    ArrayList<AggregationDesc> aggrs = op.getConf().getAggregators();
    String s = aggrs.get(aggrs.size()-1).getExprString();
    testBool(s.startsWith(lineageUDAFName + "(VALUE"), "gby2 last aggr not lin", op);
  }

  private void testFilterPostSelect(FilterOperator op) {
    List<Operator<? extends OperatorDesc>> childs = op.getChildOperators();
    if (childs.size() != 1) {
      error("Filter not exact 1 post op");
    }
    else {
      testBool(childs.get(0) instanceof SelectOperator, "select not post filter", op);
    }
  }

  private void testJoinPostSelect(JoinOperator op) {
    List<Operator<? extends OperatorDesc>> childs = op.getChildOperators();
    if (childs.size() != 1) {
      error("Join not exact 1 post op");
    }
    else {
      testBool(childs.get(0) instanceof SelectOperator, "select not post join", op);
    }
  }

  private void testTidExistsFromSampledTable(Operator<? extends OperatorDesc> op) {
    // TODO
  }

  private void testBool(boolean b, String s, Operator<? extends OperatorDesc> op) {
    if (!b) {
      error(s + " failed. " + op.toString());
    }
  }

  private void error(String s) {
    throw new RuntimeException("[ABM TEST ERROR] " + s);
  }

}