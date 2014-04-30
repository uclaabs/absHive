package org.apache.hadoop.hive.ql.abm.funcdep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.funcdep.FuncDepCtx.FuncDep;
import org.apache.hadoop.hive.ql.abm.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.ExprProcFactory;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;

/**
 *
 * FuncDepProcFactory
 * 1. extracts functional dependency from the query plan.
 * 2. checks all the functional dependency eligibility.
 *
 */
public class FuncDepProcFactory {

  private static int getBranch(Operator<? extends OperatorDesc> parent,
      Operator<? extends OperatorDesc> op) {
    return parent.getChildOperators().indexOf(op);
  }

  private static int getNumCopies(Operator<? extends OperatorDesc> op) {
    if (op.getChildOperators() == null) {
      return 1;
    }
    return Math.max(op.getChildOperators().size(), 1);
  }

  /**
   *
   * TableScanChecker for table scan operator.
   * Add base functional dependency from schema.
   *
   */
  public static class TableScanChecker implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FuncDepCtx ctx = (FuncDepCtx) procCtx;
      TableScanOperator op = (TableScanOperator) nd;

      Set<String> keys = extractPrimaryKeys(op, ctx.getParseContext());

      int numCopies = getNumCopies(op);
      for (int copy = 0; copy < numCopies; ++copy) {
        HashSet<String> pk = new HashSet<String>();
        for (String col : keys) {
          pk.add(FuncDepCtx.mkColName(op, col, copy));
        }

        HashSet<String> cols = new HashSet<String>();
        for (ColumnInfo ci : op.getSchema().getSignature()) {
          cols.add(FuncDepCtx.mkColName(op, ci.getInternalName(), copy));
        }

        ctx.addFD(new FuncDep(pk, cols));
      }

      return null;
    }

    public static Set<String> extractPrimaryKeys(TableScanOperator ts, ParseContext pctx) {
      HashSet<String> keys = new HashSet<String>();

      Table tab = pctx.getTopToTable().get(ts);
      String tabName = tab.getTableName();

      Map<String, Set<String>> schema = AbmUtilities.getSchemaPrimaryKeyMap();
      Set<String> primaryKey = schema.get(tabName);

      if (primaryKey == null) {
        for (ColumnInfo ci : ts.getSchema().getSignature()) {
          keys.add(ci.getInternalName());
        }
        return keys;
      }

      for (ColumnInfo ci : ts.getSchema().getSignature()) {
        if (primaryKey.contains(ci.getInternalName())) {
          keys.add(ci.getInternalName());
        }
      }

      // If not all primary keys are included, we have to use all of the columns.
      if (!keys.containsAll(primaryKey)) {
        for (ColumnInfo ci : ts.getSchema().getSignature()) {
          keys.add(ci.getInternalName());
        }
      }

      return keys;
    }

  }

  /**
   *
   * FilterChecker for filter operator.
   * Maintain extra functional dependency from filter predicates.
   *
   */
  public static class FilterChecker extends DefaultChecker {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      FuncDepCtx ctx = (FuncDepCtx) procCtx;
      FilterOperator op = (FilterOperator) nd;
      ExprNodeDesc filter = op.getConf().getPredicate();
      int numCopies = getNumCopies(op);
      parseFilter(ctx, filter, op, numCopies);

      return null;
    }

    private static void parseFilter(FuncDepCtx ctx, ExprNodeDesc expr,
        Operator<? extends OperatorDesc> op, int numCopies) throws SemanticException {
      if (expr instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
        GenericUDF udf = func.getGenericUDF();

        // We only parse x=y and z=w and ... where x, y, z, w are bijection funcs
        if (udf instanceof GenericUDFOPAnd) {
          for (ExprNodeDesc param : func.getChildExprs()) {
            parseFilter(ctx, param, op, numCopies);
          }
        } else if (udf instanceof GenericUDFOPEqual) {
          List<ExprNodeDesc> params = func.getChildExprs();
          assert params.size() == 2;

          ExprInfo lhs = ExprProcFactory.extractExprInfo(op, ctx.getLineage(), params.get(0));
          ExprInfo rhs = ExprProcFactory.extractExprInfo(op, ctx.getLineage(), params.get(1));

          for (int i = 0; i < numCopies; ++i) {
            ctx.addEqualityFD(lhs, i, rhs, i);
          }

        }
      }
    }

  }

  /**
   *
   * JoinChecker for join operator.
   * Maintain extra functional dependency from join keys for the outer side.
   * Note we do not add extra functional dependency for filter predicates,
   * as all the filter predicates for the outer side have already been pushed down.
   *
   */
  public static class JoinChecker extends DefaultChecker {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      FuncDepCtx ctx = (FuncDepCtx) procCtx;
      JoinOperator op = (JoinOperator) nd;
      JoinDesc desc = op.getConf();

      // Pick out those tables participating in inner joins.
      JoinCondDesc[] jconds = desc.getConds();
      HashSet<Integer> inners = new HashSet<Integer>();
      for (JoinCondDesc jcond : jconds) {
        if (jcond.getType() == JoinDesc.INNER_JOIN
            || jcond.getType() == JoinDesc.LEFT_SEMI_JOIN) {
          inners.add(jcond.getLeft());
          inners.add(jcond.getRight());
        }
      }

      // The input operator to the join is always a reduce sink operator
      ArrayList<ArrayList<ExprInfo>> allKeys = new ArrayList<ArrayList<ExprInfo>>();
      ArrayList<Integer> aCopies = new ArrayList<Integer>();
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        ReduceSinkOperator rs = (ReduceSinkOperator) parent;
        ReduceSinkDesc rsDesc = rs.getConf();
        int tag = rsDesc.getTag();

        if (!inners.contains(tag)) {
          continue;
        }

        Operator<? extends OperatorDesc> ancestor = rs.getParentOperators().get(0);
        aCopies.add(getBranch(ancestor, rs));

        ArrayList<ExprInfo> keys = new ArrayList<ExprInfo>();
        for (ExprNodeDesc joinKey : rsDesc.getKeyCols()) {
          ExprInfo key = ExprProcFactory.extractExprInfo(
              ancestor, ctx.getLineage(), joinKey);
          keys.add(key);
        }
        allKeys.add(keys);
      }

      extractFuncDeps(ctx, allKeys, aCopies);

      return null;
    }

    private static void extractFuncDeps(FuncDepCtx ctx,
        ArrayList<ArrayList<ExprInfo>> allKeys, ArrayList<Integer> aCopies) {
      for (int i = 0; i < allKeys.size(); ++i) {
        for (int j = i + 1; j < allKeys.size(); ++j) {
          ArrayList<ExprInfo> lhs = allKeys.get(i);
          ArrayList<ExprInfo> rhs = allKeys.get(j);
          assert lhs.size() == rhs.size();
          for (int k = 0; k < lhs.size(); ++k) {
            ctx.addEqualityFD(lhs.get(k), aCopies.get(i), rhs.get(k), aCopies.get(j));
          }
        }
      }
    }

  }

  /**
   *
   * GroupByChecker for group by operator.
   * Check functional dependency.
   *
   */
  public static class GroupByChecker extends DefaultChecker {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      FuncDepCtx ctx = (FuncDepCtx) procCtx;
      GroupByOperator op = (GroupByOperator) nd;

      // Func Dep: keys --> aggrs
      int numCopies = getNumCopies(op);
      for (int i = 0; i < numCopies; ++i) {
        ctx.addFD(new FuncDep(extractKeys(op, i), extractAggrs(op, i)));
      }

      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        // There are two types of group by plan:
        // 1. GRY
        // 2. GRY --> RS --> GRY
        // We only need to check in the first aggregate.
        boolean toCheckAggr = !(parent instanceof ReduceSinkOperator);
        if (toCheckAggr) {
          funcDepCheck(ctx, op, parent, parent, getBranch(parent, op));
        }
      }

      return null;
    }

    private HashSet<String> extractKeys(GroupByOperator gb, int copy) {
      HashSet<String> keys = new HashSet<String>();
      int numKeys = gb.getConf().getKeys().size();
      ArrayList<ColumnInfo> columnInfos = gb.getSchema().getSignature();
      for (int i = 0; i < numKeys; ++i) {
        keys.add(FuncDepCtx.mkColName(gb, columnInfos.get(i).getInternalName(), copy));
      }
      return keys;
    }

    private HashSet<String> extractAggrs(GroupByOperator gb, int copy) {
      HashSet<String> aggrs = new HashSet<String>();
      int numKeys = gb.getConf().getKeys().size();
      ArrayList<ColumnInfo> columnInfos = gb.getSchema().getSignature();
      for (int i = numKeys; i < columnInfos.size(); ++i) {
        aggrs.add(FuncDepCtx.mkColName(gb, columnInfos.get(i).getInternalName(), copy));
      }
      return aggrs;
    }

    // Backtrack the stack to GroupByOperator or TableScanOperator
    // (1) If we find a GroupByOperator x, we only need to check
    // the key of this GroupByOperator can infer the key of x,
    // as the other func deps have been taken care of by x or another trace;
    // (2) If we find a TableScanOperator which reads the sampled table, we only need to check
    // the key of this GroupByOperator + the primary key of the sampled table
    // can infer the head of the input of this GroupByOperator
    private void funcDepCheck(FuncDepCtx ctx,
        GroupByOperator target, Operator<? extends OperatorDesc> parent,
        Operator<? extends OperatorDesc> anc, int branchIndex) throws SemanticException {
      if (anc instanceof TableScanOperator) {
        HashSet<String> keys1 = extractKeys(target, 0);
        TableScanOperator ts = (TableScanOperator) anc;
        Table tab = ctx.getParseContext().getTopToTable().get(ts);

        assert tab != null;
        if (AbmUtilities.getSampledTable().equals(tab.getTableName())) {
          for (String pk : TableScanChecker.extractPrimaryKeys(ts, ctx.getParseContext())) {
            keys1.add(FuncDepCtx.mkColName(ts, pk, branchIndex));
          }

          HashSet<String> keys2 = new HashSet<String>();
          int index = getBranch(parent, target);
          for (ColumnInfo ci : parent.getSchema().getSignature()) {
            keys2.add(FuncDepCtx.mkColName(parent, ci.getInternalName(), index));
          }

          if (!ctx.infer(keys1, keys2)) {
            AbmUtilities.report(ErrorMsg.QUERY_NOT_ABM_ELIGIBLE);
          }
        }
      }

      if (anc instanceof GroupByOperator) {
        HashSet<String> keys1 = extractKeys(target, 0);
        GroupByOperator gb = (GroupByOperator) anc;
        HashSet<String> keys2 = extractKeys(gb, branchIndex);

        // We only warn here
        // because despite being NOT_PTIME_ELIGIBLE, some TPCH queries can still be computed.
        if (!ctx.infer(keys1, keys2)) {
          AbmUtilities.warn(ErrorMsg.QUERY_NOT_ABM_PTIME_ELIGIBLE);
        }

        return;
      }

      List<Operator<? extends OperatorDesc>> ancParents = anc.getParentOperators();
      if (ancParents != null) {
        for (int i = 0; i < ancParents.size(); ++i) {
          funcDepCheck(ctx, target, parent, ancParents.get(i), i);
        }
      }
    }

  }

  /**
   *
   * DefaultChecker: maintain functional dependency.
   *
   */
  public static class DefaultChecker implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FuncDepCtx ctx = (FuncDepCtx) procCtx;
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      HashMap<String, ExprInfo> lineage = ctx.getLineage().getLineages(op);

      if (lineage == null) {
        return null;
      }

      int numCopy = getNumCopies(op);
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        int pcopy = getBranch(parent, op);
        for (Entry<String, ExprInfo> entry : lineage.entrySet()) {
          String col = entry.getKey();
          ExprInfo exprInfo = entry.getValue();
          for (int ccopy = 0; ccopy < numCopy; ++ccopy) {
            ctx.addColumnFD(op, col, ccopy, exprInfo, pcopy);
          }
        }
      }

      return null;
    }

  }

  public static NodeProcessor getTableScanProc() {
    return new TableScanChecker();
  }

  public static NodeProcessor getFilterProc() {
    return new FilterChecker();
  }

  public static NodeProcessor getJoinProc() {
    return new JoinChecker();
  }

  public static NodeProcessor getGroupByProc() {
    return new GroupByChecker();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultChecker();
  }

  public static void checkFuncDep(LineageCtx lctx) throws SemanticException {
    FuncDepCtx ctx = new FuncDepCtx(lctx);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
        getTableScanProc());
    opRules.put(new RuleRegExp("R2", FilterOperator.getOperatorName() + "%"),
        getFilterProc());
    opRules.put(new RuleRegExp("R3", CommonJoinOperator.getOperatorName() + "%"),
        getJoinProc());
    opRules.put(new RuleRegExp("R4", GroupByOperator.getOperatorName() + "%"),
        getGroupByProc());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, ctx);
    GraphWalker walker = new PreOrderWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(ctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);
  }

}
