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

package org.apache.hadoop.hive.ql.abm.funcdep;

import java.util.HashSet;

import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class FuncDepCtx implements NodeProcessorCtx {

  // op.copy.col
  private static final String nameFormat = "%s.%d.%s";
  //private final HashSet<Operator<? extends OperatorDesc>> visitedOp = new HashSet<Operator<? extends OperatorDesc>>();

  public static class FuncDep {

    public HashSet<String> determinant;
    public HashSet<String> dependent;

    public FuncDep(HashSet<String> det, HashSet<String> dep) {
      determinant = det;
      dependent = dep;
    }

    @Override
    public int hashCode() {
      return determinant.hashCode() * 31 + dependent.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FuncDep)) {
        return false;
      }
      FuncDep other = (FuncDep) o;
      return determinant.equals(other.determinant) && dependent.equals(other.dependent);
    }

    @Override
    public String toString() {
      return determinant.toString() + " --> " + dependent.toString() + "\n";
    }

  }

  public static String mkColName(Operator<? extends OperatorDesc> op,
      String col, int copy) {
    return String.format(nameFormat, op.toString(), copy, col);
  }

  private static HashSet<String> mkColGrp(ExprInfo exprInfo, int copy) {
    HashSet<String> ret = new HashSet<String>();

    Operator<? extends OperatorDesc> op = exprInfo.getOperator();
    for (String column : exprInfo.getColumns()) {
      ret.add(mkColName(op, column, copy));
    }

    for (String column : exprInfo.getColumns()) {
      ret.add(mkColName(op, column, copy));
    }

    return ret;
  }

  private final HashSet<FuncDep> funcDeps = new HashSet<FuncDep>();
  private final LineageCtx lineage;

  public FuncDepCtx(LineageCtx lctx) {
    lineage = lctx;
  }

  public void addFD(FuncDep fd) {
    funcDeps.add(fd);
  }

  // Column FD is generated through this relationship:
  // pInfo is generating cCol in cOp's output
  public void addColumnFD(Operator<?> cOp, String cCol, int cCopy,
      ExprInfo pInfo, int pCopy) {
    addEqualityFD(new ExprInfo(cOp, cCol), cCopy, pInfo, pCopy);
  }

  // FilterFD is generated through this relaionship:
  // lhs is generating x, rhs is generating y
  // where x=y
  public void addEqualityFD(ExprInfo lhs, int lCopy, ExprInfo rhs, int rCopy) {
    if (!lhs.isDeterministic() || lhs.hasAggrOutput()
        || !rhs.isDeterministic() || rhs.hasAggrOutput()) {
      return;
    }

    if (!lhs.isBijection()) {
      if (rhs.isBijection()) {
        funcDeps.add(new FuncDep(
            FuncDepCtx.mkColGrp(lhs, lCopy),
            FuncDepCtx.mkColGrp(rhs, rCopy)));
      }
    } else if (!rhs.isBijection()) {
      funcDeps.add(new FuncDep(
          FuncDepCtx.mkColGrp(rhs, lCopy),
          FuncDepCtx.mkColGrp(lhs, rCopy)));
    } else {
      funcDeps.add(new FuncDep(
          FuncDepCtx.mkColGrp(lhs, lCopy),
          FuncDepCtx.mkColGrp(rhs, rCopy)));
      funcDeps.add(new FuncDep(
          FuncDepCtx.mkColGrp(rhs, lCopy),
          FuncDepCtx.mkColGrp(lhs, rCopy)));
    }
  }

  public LineageCtx getLineage() {
    return lineage;
  }

  public ParseContext getParseContext() {
    return lineage.getParseContext();
  }

  public boolean infer(HashSet<String> det, HashSet<String> dep) {
    HashSet<String> inferred = new HashSet<String>(det);
    boolean added;

    do {
      added = false;

      for (FuncDep fd : funcDeps) {
        if (inferred.containsAll(fd.determinant)) {
          added = added || inferred.addAll(fd.dependent);
        }
      }
    } while(added);

    return inferred.containsAll(dep);
  }

  @Override
  public String toString() {
    return funcDeps.toString();
  }

  /*
  public boolean isVisited(Operator<? extends OperatorDesc> op) {
    return visitedOp.contains(op);
  }

  public void setVisited(Operator<? extends OperatorDesc> op) {
    visitedOp.add(op);
  }*/
}
