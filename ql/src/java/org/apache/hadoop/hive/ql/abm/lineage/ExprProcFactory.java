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

package org.apache.hadoop.hive.ql.abm.lineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.lib.PostOrderExprWalker;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;

/**
 *
 * ExprProcFactory parse non-filter expressions (i.e., appearing as output columns).
 *
 */
public class ExprProcFactory {

  /**
   *
   * Processor for column expressions.
   *
   */
  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprProcCtx ctx = (ExprProcCtx) procCtx;
      ExprNodeColumnDesc col = (ExprNodeColumnDesc) nd;
      ExprInfo info = ctx.getExprInfo();

      String internalName = col.getColumn();
      info.addColumn(internalName);

      Operator<? extends OperatorDesc> op = ctx.getParentOp();
      ExprInfo dep = ctx.getLineageCtx().getLineage(op, internalName);
      if (dep != null) {
        if (info.hasAggrOutput()) {
          AbmUtilities.report(ErrorMsg.FUNC_OF_AGGR_NOT_ALLOWED_FOR_ABM);
        }
        info.setHasAggrOutput(dep.hasAggrOutput());
      } else {
        // dep = null iff op = TS
        assert op instanceof TableScanOperator;
      }

      return null;
    }

  }

  /**
   *
   * Processor for field expressions.
   *
   */
  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      AbmUtilities.report(ErrorMsg.FIELDS_NOT_ALLOWED_FOR_ABM);
      return null;
    }

  }

  /**
   *
   * Processor for generic function expressions.
   *
   */
  public static class GenericFuncExprProcessor implements NodeProcessor {

    private static final HashSet<String> basicFuncs = new HashSet<String>(
        Arrays.asList("+", "-", "*", "/"));

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprProcCtx ctx = (ExprProcCtx) procCtx;
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) nd;
      ExprInfo info = ctx.getExprInfo();

      // We do not support function of aggregates in any form
      // As it's hard to maintain the lineage
      // But it may be doable in Shark if we store the function with the lineage
      if (info.hasAggrOutput()) {
        AbmUtilities.report(ErrorMsg.FUNC_OF_AGGR_NOT_ALLOWED_FOR_ABM);
      }

      GenericUDF udf = func.getGenericUDF();

      if (udf instanceof GenericUDFBridge) {
        // +, -, *, /, %, pow, log, ...
        GenericUDFBridge bridge = (GenericUDFBridge) udf;

        if (!basicFuncs.contains(bridge.getUdfName().toLowerCase())) {
          // %, &, |, round, floor, ceil, ceiling, pow, power, sin, cos, acos, tan, ...
          // y = pow(x, 2) is not bijection, but z = pow(x, 3) is
          // But we simply ignore this complexity and treat all of them as non-bijection.
          info.setBijection(false);
        }

        if (bridge.getUdfName().equalsIgnoreCase("rand")) {
          info.setDeterministic(false);
        }

      } else {
        // including comparison, logical ops, between, ...
        info.setBijection(false);
      }

      return null;
    }

  }

  /**
   *
   * Default Processor for constant and null expressions.
   * Do nothing.
   *
   */
  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }

  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }

  public static NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static ExprInfo extractExprInfo(Operator<? extends OperatorDesc> parent,
      LineageCtx lctx, Collection<ExprNodeDesc> exprs) throws SemanticException {
    // Create a walker which walks the tree in a DFS manner while maintaining
    // the expression stack.
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(
        new RuleRegExp("R1", ExprNodeColumnDesc.class.getName() + "%"),
        getColumnProcessor());
    exprRules.put(
        new RuleRegExp("R2", ExprNodeFieldDesc.class.getName() + "%"),
        getFieldProcessor());
    exprRules.put(new RuleRegExp("R3", ExprNodeGenericFuncDesc.class.getName()
        + "%"), getGenericFuncProcessor());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    ExprProcCtx ctx = new ExprProcCtx(parent, lctx);
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, ctx);
    GraphWalker walker = new PostOrderExprWalker(disp);

    ArrayList<Node> startNodes = new ArrayList<Node>();
    startNodes.addAll(exprs);
    walker.startWalking(startNodes, null);

    return ctx.getExprInfo();
  }

  public static ExprInfo extractExprInfo(Operator<? extends OperatorDesc> parent,
      LineageCtx lctx, ExprNodeDesc exprs) throws SemanticException {
    return extractExprInfo(parent, lctx, Arrays.asList(exprs));
  }

  // Only if it's and|>|>=|<|<=, we need to go into its child expressions.
  // Otherwise, we need to make itself does not contain any aggregate.
  // Because,
  // (1) Or/Not... involving aggregates are too complicated to compute;
  // (2) Equality test on aggregates are not eligible
  public static void checkFilter(Operator<? extends OperatorDesc> parent,
      LineageCtx lctx, ExprNodeDesc filter) throws SemanticException {

    if (filter instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) filter;
      GenericUDF udf = func.getGenericUDF();

      if (udf instanceof GenericUDFOPAnd) {
        for (ExprNodeDesc child : func.getChildExprs()) {
          checkFilter(parent, lctx, child);
        }
        return;
      }

      if ((udf instanceof GenericUDFOPEqualOrGreaterThan)
          || (udf instanceof GenericUDFOPEqualOrLessThan)
          || (udf instanceof GenericUDFOPGreaterThan)
          || (udf instanceof GenericUDFOPLessThan)) {
        for (ExprNodeDesc child : func.getChildExprs()) {
          checkFilter(parent, lctx, child);
        }
        return;
      }

      if ((udf instanceof GenericUDFOPEqual)
          || (udf instanceof GenericUDFOPNotEqual)) {
        for (ExprNodeDesc child : func.getChildExprs()) {
          ExprInfo info = extractExprInfo(parent, lctx, child);
          if (info.hasAggrOutput()) {
            AbmUtilities.report(ErrorMsg.EQUAL_OF_AGGR_NOT_ABM_ELIGIBLE);
          }
        }
        return;
      }

      ExprInfo info = extractExprInfo(parent, lctx, filter);
      if (info.hasAggrOutput()) {
        AbmUtilities.report(ErrorMsg.FUNC_OF_AGGR_NOT_ALLOWED_FOR_ABM);
      }
    }
  }

}
