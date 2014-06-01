package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.abm.algebra.ComparisonTransform;
import org.apache.hadoop.hive.ql.abm.algebra.ConstantTransform;
import org.apache.hadoop.hive.ql.abm.algebra.IdentityTransform;
import org.apache.hadoop.hive.ql.abm.algebra.Transform;
import org.apache.hadoop.hive.ql.abm.lib.PostOrderPlanWalker;
import org.apache.hadoop.hive.ql.abm.lineage.ExprInfo;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
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
          ConditionAnnotation cond = ctx.getCondition(parent);
          if (cond != null) {
            ctx.addCondition(op, cond);
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
              ctx.putLineage(op, entry.getKey(), ctx.getLineage(parent, col));
            }
          }
        }
      }

      return null;
    }

  }

  /**
   *
   * FileSinkProcessor sets all the GBYs which generate the output aggregates as used by FileSink.
   *
   */
  public static class FileSinkProcessor extends DefaultProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      FileSinkOperator fs = (FileSinkOperator) nd;
      TraceProcCtx ctx = (TraceProcCtx) procCtx;
      for (ColumnInfo ci : fs.getSchema().getSignature()) {
        AggregateInfo ai = ctx.getLineage(fs, ci.getInternalName());
        if (ai != null) {
          ctx.addReturn(fs, ai);
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

    private Transform traverse(ExprNodeDesc expr, FilterOperator fil,
        TraceProcCtx ctx) throws SemanticException {
      if (expr instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
        GenericUDF udf = func.getGenericUDF();

        if (udf instanceof GenericUDFOPAnd) {
          for (ExprNodeDesc childExpr : func.getChildExprs()) {
            traverse(childExpr, fil, ctx);
          }
          return null;
        }

        if ((udf instanceof GenericUDFOPEqualOrGreaterThan)
            || (udf instanceof GenericUDFOPEqualOrLessThan)
            || (udf instanceof GenericUDFOPGreaterThan)
            || (udf instanceof GenericUDFOPLessThan)) {
          ArrayList<Transform> params = new ArrayList<Transform>();
          for (ExprNodeDesc childExpr : func.getChildExprs()) {
            params.add(traverse(childExpr, fil, ctx));
          }

          boolean uncertain = false;
          for (Transform param : params) {
            uncertain = uncertain || (param instanceof IdentityTransform);
          }

          if (uncertain) {
            assert params.size() == 2;
            ctx.addCondition(fil, new ComparisonTransform(params.get(0), params.get(1),
                (udf instanceof GenericUDFOPEqualOrGreaterThan)
                    || (udf instanceof GenericUDFOPGreaterThan)));
          }

          return null;
        }

        return null;
      }

      if (expr instanceof ExprNodeColumnDesc) {
        Operator<? extends OperatorDesc> parent = fil.getParentOperators().get(0);
        ExprNodeColumnDesc col = (ExprNodeColumnDesc) expr;
        AggregateInfo aggr = ctx.getLineage(parent, col.getColumn());
        if (aggr != null) {
          return new IdentityTransform(aggr);
        }
        return new ConstantTransform(null);
      }

      if (expr instanceof ExprNodeConstantDesc) {
        ExprNodeConstantDesc cons = (ExprNodeConstantDesc) expr;
        return new ConstantTransform(cons.getValue());
      }

      return null;
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

      if (!ctx.isUncertain(gby)) {
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

      // info (1)
      int numKeys = desc.getKeys().size();
      for (int i = numKeys; i < allCols.size(); ++i) {
        int idx = i - numKeys;
        ctx.putLineage(gby, allCols.get(i).getInternalName(),
            new AggregateInfo(gby, idx, aggrs.get(idx).getGenericUDAFName()));
      }

      // info (3)
      ctx.groupByAt(gby);
      ctx.addCondition(gby, new ComparisonTransform(
          new IdentityTransform(new AggregateInfo(gby, -1, "count")),
          new ConstantTransform(0), true));

      return null;
    }

  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultProcessor();
  }

  public static NodeProcessor getFileSinkProc() {
    return new FileSinkProcessor();
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
    opRules.put(new RuleRegExp("R2", GroupByOperator.getOperatorName() + "%"),
        getGroupByProc());
    opRules.put(new RuleRegExp("R3", FileSinkOperator.getOperatorName() + "%"),
        getFileSinkProc());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, ctx);
    GraphWalker walker = new PostOrderPlanWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(lctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);

    return ctx;
  }

}
