package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.abm.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;

/**
 *
 * TraceProcFactory maintains the following information:
 * (1) Identified each aggregate by <GroupByOperator, index in conf.aggregators>.
 * (2) Map columns generated from aggregates (currently only consider the case w/ no transformation)
 * to the corresponding aggregates.
 * (3) Track the aggregates in conditions.
 *
 */
public class TraceProcFactory {

  private static void cleanRowResolver(TraceProcCtx ctx, Operator<? extends OperatorDesc> op) {
    OpParseContext opParseCtx = ctx.getOpParseContext(op);
    RowResolver oldRR = opParseCtx.getRowResolver();
    RowResolver newRR = new RowResolver();

    ArrayList<ColumnInfo> allCols = op.getSchema().getSignature();
    for (ColumnInfo col : allCols) {
      String[] name = oldRR.reverseLookup(col.getInternalName());
      newRR.put((name[0] == null) ? "" : name[0], name[1], col);
    }

    op.setSchema(newRR.getRowSchema());
    opParseCtx.setRowResolver(newRR);
  }

  /**
   *
   * BaseProcessor maintains info (3) by propagating the conditions.
   *
   */
  public static class BaseProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      TraceProcCtx ctx = (TraceProcCtx) procCtx;

      cleanRowResolver(ctx, op);

      List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
      if (parents != null) {
        for (Operator<? extends OperatorDesc> parent : parents) {
          Set<AggregateInfo> condLineage = ctx.getConditions(parent);
          if (condLineage != null) {
            ctx.addConditions(op, condLineage);
          }
        }
      }

      return null;
    }

  }

  /**
   *
   * DefaultProcessor maintains info (1) and (2).
   *
   */
  public static class DefaultProcessor extends BaseProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      TraceProcCtx ctx = (TraceProcCtx) procCtx;

      // info (2)
      Map<String, ExprInfo> mapping = ctx.getOpColumnMapping(op);
      if (mapping != null) {
        for (Entry<String, ExprInfo> entry : mapping.entrySet()) {
          ExprInfo dep = entry.getValue();
          if (dep.hasAggrOutput()) {
            Set<String> cols = dep.getColumns();
            assert cols.size() == 1;
            for (String col : cols) {
              Operator<? extends OperatorDesc> parent = dep.getOperator();
              ctx.addLineage(op, entry.getKey(), ctx.getLineage(parent, col));
            }
          }
        }
      }

      return null;
    }

  }

  /**
   *
   * FilterProcessor maintains info (3) by adding extra correlated columns from the predicate.
   *
   */
  public static class FilterProcessor extends DefaultProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      FilterOperator fil = (FilterOperator) nd;
      TraceProcCtx ctx = (TraceProcCtx) procCtx;
      FilterDesc desc = fil.getConf();
      traverse(desc.getPredicate(), fil, ctx);

      return null;
    }

    private void traverse(ExprNodeDesc expr, FilterOperator fil,
        TraceProcCtx ctx) throws SemanticException {
      if (expr instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
        GenericUDF udf = func.getGenericUDF();

        if ((udf instanceof GenericUDFOPAnd)
            || (udf instanceof GenericUDFOPEqualOrGreaterThan)
            || (udf instanceof GenericUDFOPEqualOrLessThan)
            || (udf instanceof GenericUDFOPGreaterThan)
            || (udf instanceof GenericUDFOPLessThan)) {
          for (ExprNodeDesc childExpr : func.getChildExprs()) {
            traverse(childExpr, fil, ctx);
          }
          return;
        }

        return;
      }

      if (expr instanceof ExprNodeColumnDesc) {
        Operator<? extends OperatorDesc> parent = fil.getParentOperators().get(0);
        ExprNodeColumnDesc col = (ExprNodeColumnDesc) expr;
        AggregateInfo aggr = ctx.getLineage(parent, col.getColumn());
        if (aggr != null) {
          ctx.addCondition(fil, aggr);
        }
        return;
      }

      return;
    }
  }

  /**
   *
   * GroupByProcessor maintains info (1).
   * Since GroupBy is the source of info (2), so it does not need to maintain info (2).
   * It maintains info (3) by putting itself as an extra condition.
   *
   */
  public static class GroupByProcessor extends BaseProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      GroupByOperator gby = (GroupByOperator) nd;
      TraceProcCtx ctx = (TraceProcCtx) procCtx;

      if (!ctx.isSampled(gby)) {
        return null;
      }

      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);
      boolean firstGby = !(parent instanceof ReduceSinkOperator);

      if (firstGby) {
        return null;
      }

      GroupByDesc desc = gby.getConf();
      ArrayList<ColumnInfo> allCols = gby.getSchema().getSignature();
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();

      // If this GroupByOperator has no condition,
      // then the set of tuples contributing to the aggregates is deterministic.
      boolean deterministic = (ctx.getConditions(gby) == null);

      // info (1)
      int numKeys = desc.getKeys().size();
      for (int i = numKeys; i < allCols.size(); ++i) {
        int idx = i - numKeys;
        ctx.addLineage(gby, allCols.get(i).getInternalName(),
            new AggregateInfo(gby, idx, aggrs.get(idx).getGenericUDAFName(), deterministic));
      }

      // info (3)
      ctx.addCondition(gby, new AggregateInfo(gby, -1, "count", deterministic));

      return null;
    }

  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultProcessor();
  }

  public static NodeProcessor getFilterProc() {
    return new FilterProcessor();
  }

  public static NodeProcessor getGroupByProc() {
    return new GroupByProcessor();
  }

  public static TraceProcCtx trace(LineageCtx lctx) throws SemanticException {
    TraceProcCtx ctx = new TraceProcCtx(lctx);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"),
        getFilterProc());
    opRules.put(new RuleRegExp("R4", GroupByOperator.getOperatorName() + "%"),
        getGroupByProc());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, ctx);
    GraphWalker walker = new PreOrderWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(lctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);

    return ctx;
  }

}