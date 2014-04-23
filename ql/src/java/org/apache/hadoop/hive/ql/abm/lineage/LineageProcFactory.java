package org.apache.hadoop.hive.ql.abm.lineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
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
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

/**
 *
 * LineageProcFactory
 *
 * 1. extracts lineage information from the optimized plan.
 * Lineage information includes:
 * (1) this column of this operator is GENEATED BY which column(s) in the output of the parent
 * operator(s) (we don't distinguish parameters in CASE...WHEN/IF);
 * (2) how the column is generated, i.e., is the generating function
 * (a) bijection (b) has aggregate (c) deterministic (e.g., not rand()) ?
 *
 * 2. checks all the lineage errors/exceptions, including:
 * (1) aggregates of aggregates are not eligible;
 * (2) equality test on aggregates (including group by(project)/join on aggregates) are not
 * eligible;
 * (3) min/max aggregates on sampled table are not eligible.
 * (4) only basic arithmetic on aggregates (aggregate +|-|*|/ constant) are supported for now;
 * (5) we don't support any aggregates in any udf.
 *
 * Note that only aggregate computed on sampled table or join results using sampled table
 * are considered as aggregates.
 *
 */
public class LineageProcFactory {

  /**
   *
   * BaseLineage: maintains whether an operator's input/output are from the sampled table.
   *
   */
  public static class BaseLineage implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LineageCtx ctx = (LineageCtx) procCtx;

      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        if (ctx.isSampled(parent)) {
          ctx.addSampled(op);
          break;
        }
      }

      return null;
    }

  }

  /**
   *
   * Processor for table scan operator.
   * Only maintains whether an operator's input/output are from the sampled table,
   * do not maintain lineage (as there is no parent).
   *
   */
  public static class TableScanLineage implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LineageCtx ctx = (LineageCtx) procCtx;
      ParseContext pctx = ctx.getParseContext();
      TableScanOperator ts = (TableScanOperator) nd;

      Table tab = pctx.getTopToTable().get(ts);
      if (AbmUtilities.getSampledTable().equals(tab.getTableName())) {
        ctx.addSampled(ts);
      }

      return null;
    }

  }

  /**
   *
   * Processor for reduce sink operator.
   *
   */
  public static class ReduceSinkLineage extends BaseLineage {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LineageCtx ctx = (LineageCtx) procCtx;
      ReduceSinkOperator rs = (ReduceSinkOperator) nd;
      Operator<? extends OperatorDesc> parent = rs.getParentOperators().get(0);

      ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
      int cnt = 0;

      // The keys may or may not be included:
      // (1) if RS is used in join, the keys are not included
      // (2) if RS is used in group by, the keys are included
      if (colInfos.size() > rs.getConf().getValueCols().size()) {
        for (ExprNodeDesc expr : rs.getConf().getKeyCols()) {
          String internalName = colInfos.get(cnt).getInternalName();
          ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
          ctx.put(rs, internalName, info);
          cnt++;
        }
      }

      for (ExprNodeDesc expr : rs.getConf().getValueCols()) {
        String internalName = colInfos.get(cnt).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
        ctx.put(rs, internalName, info);
        cnt++;
      }

      return super.process(nd, stack, procCtx, nodeOutputs);
    }

  }

  /**
   *
   * Processor for select operator.
   *
   */
  public static class SelectLineage extends DefaultLineage {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator sel = (SelectOperator) nd;
      SelectDesc desc = sel.getConf();

      // If this is a selStarNoCompute then this select operator
      // is treated like a default operator, so just call the super classes
      // process method.
      if (desc.isSelStarNoCompute()) {
        return super.process(nd, stack, procCtx, nodeOutputs);
      }

      // Otherwise we treat this as a normal select operator and look at
      // the expressions.
      LineageCtx ctx = (LineageCtx) procCtx;
      Operator<? extends OperatorDesc> parent = sel.getParentOperators().get(0);

      ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
      int cnt = 0;
      for (ExprNodeDesc expr : desc.getColList()) {
        String internalName = colInfos.get(cnt).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
        ctx.put(sel, internalName, info);
        cnt++;
      }

      return super.baseProcess(nd, stack, procCtx, nodeOutputs);
    }

  }

  public static class FilterLineage extends DefaultLineage {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator fil = (FilterOperator) nd;
      FilterDesc desc = fil.getConf();

      // We do not support sampling.
      if (desc.getIsSamplingPred()) {
        AbmUtilities.report(ErrorMsg.SAMPLE_NOT_ALLOWED_FOR_ABM);
      }

      LineageCtx ctx = (LineageCtx) procCtx;
      Operator<? extends OperatorDesc> parent = fil.getParentOperators().get(0);
      ExprProcFactory.checkFilter(parent, ctx, desc.getPredicate());

      return super.process(nd, stack, procCtx, nodeOutputs);
    }

  }

  /**
   *
   * Processor for group by operator.
   *
   */
  public static class GroupByLineage extends BaseLineage {

    private static final HashSet<String> extrema = new HashSet<String>(Arrays.asList("min", "max"));
    private static final HashSet<String> basic = new HashSet<String>(Arrays.asList("sum", "count",
        "avg"));

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      // For group by operators, columnExprMap only contains the key columns.
      // We should include both key and aggregates columns here.
      // Use "schema" to get the full info -- [keys, aggregates]

      GroupByOperator gby = (GroupByOperator) nd;
      LineageCtx ctx = (LineageCtx) procCtx;
      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);

      ArrayList<ColumnInfo> colInfos = gby.getSchema().getSignature();
      int cnt = 0;

      // In keys
      for (ExprNodeDesc expr : gby.getConf().getKeys()) {
        String internalName = colInfos.get(cnt).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
        if (info.hasAggrOutput()) {
          AbmUtilities.report(ErrorMsg.EQUAL_OF_AGGR_NOT_ABM_ELIGIBLE);
        }
        ctx.put(gby, internalName, info);
        cnt++;
      }

      // There are two types of group by plan:
      // 1. GRY
      // 2. GRY --> RS --> GRY
      // We only need to check in the first aggregate.
      boolean toCheckAggr = !(parent instanceof ReduceSinkOperator);
      boolean sampled = ctx.isSampled(gby);

      // In aggregates
      for (AggregationDesc agg : gby.getConf().getAggregators()) {
        String internalName = colInfos.get(cnt).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, agg.getParameters());
        if (sampled && toCheckAggr) {
          if (extrema.contains(agg.getGenericUDAFName().toLowerCase())) {
            AbmUtilities.report(ErrorMsg.MIN_MAX_NOT_ABM_ELIGIBLE);
          }
          if (agg.getDistinct()) {
            AbmUtilities.report(ErrorMsg.DISTINCT_AGGR_NOT_ABM_PTIME_ELIGIBLE);
          }
          if (!basic.contains(agg.getGenericUDAFName().toLowerCase())) {
            AbmUtilities.report(ErrorMsg.COMPLEX_AGGR_NOT_ALLOWED_FOR_ABM);
          }
          if (info.hasAggrOutput()) {
            AbmUtilities.report(ErrorMsg.AGGR_OF_AGGR_NOT_ABM_ELIGIBLE);
          }
          info.setHasAggrOutput(true);
        }
        ctx.put(gby, internalName, info);
        cnt++;
      }

      return null;
    }

  }

  /**
   *
   * Processor for join operator.
   *
   */
  public static class JoinLineage extends BaseLineage {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // Join filters are used when outer joins exist and predicates cannot be pushed down

      // LineageCtx
      LineageCtx ctx = (LineageCtx) procCtx;
      JoinOperator join = (JoinOperator) nd;
      JoinDesc desc = join.getConf();

      for (Operator<? extends OperatorDesc> parent : join.getParentOperators()) {
        // The input operator to the join is always a reduce sink operator
        ReduceSinkOperator rs = (ReduceSinkOperator) parent;
        ReduceSinkDesc rsDesc = rs.getConf();
        Operator<? extends OperatorDesc> grandParent = rs.getParentOperators().get(0);
        byte tag = (byte) rsDesc.getTag();

        // Join key cannot be aggregates
        for (ExprNodeDesc key : rsDesc.getKeyCols()) {
          ExprInfo info = ExprProcFactory.extractExprInfo(grandParent, ctx, key);
          if (info.hasAggrOutput()) {
            AbmUtilities.report(ErrorMsg.EQUAL_OF_AGGR_NOT_ABM_ELIGIBLE);
          }
        }

        // Iterate over the outputs of the join operator and merge the
        // dependencies of the columns that corresponding to the tag.
        int cnt = 0;
        List<ExprNodeDesc> exprs = desc.getExprs().get(tag);
        for (ColumnInfo ci : join.getSchema().getSignature()) {
          String internalName = ci.getInternalName();
          if (desc.getReversedExprs().get(internalName) != tag) {
            continue;
          }
          // Otherwise look up the expression corresponding to this ci
          ExprNodeDesc expr = exprs.get(cnt);

          ExprInfo info = ExprProcFactory.extractExprInfo(rs, ctx, expr);
          ctx.put(join, internalName, info);
          cnt++;
        }
      }

      return super.process(nd, stack, procCtx, nodeOutputs);
    }

  }

  /**
   *
   * Default processor for FileSink, Forward, Filter.
   * This basically passes the input as such to the output.
   *
   */
  public static class DefaultLineage extends BaseLineage {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      LineageCtx ctx = (LineageCtx) procCtx;
      Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);

      // Get the row schema of the input operator.
      for (ColumnInfo ci : op.getSchema().getSignature()) {
        String internalName = ci.getInternalName();
        ExprInfo info = new ExprInfo(parent, internalName);

        ExprInfo dep = ctx.get(parent, internalName);
        if (dep != null) {
          info.setHasAggrOutput(dep.hasAggrOutput());
        }

        ctx.put(op, internalName, info);
      }

      return super.process(nd, stack, procCtx, nodeOutputs);
    }

    public Object baseProcess(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return super.process(nd, stack, procCtx, nodeOutputs);
    }

  }

  /**
   *
   * Processor for ListSink.
   * Do nothing.
   *
   */
  public static class ListSinkLineage extends BaseLineage {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return super.process(nd, stack, procCtx, nodeOutputs);
    }

  }

  /**
   *
   * Exceptional processor.
   * Throw NOT_ALLOWED exception for:
   * Extract, LateralViewForward, LateralVewJoin, PTF, Script, Union, Limit,
   * Collect, ...
   *
   */
  public static class ExceptionalLineage implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      AbmUtilities.report(ErrorMsg.OPERATOR_NOT_ALLOWED_FOR_ABM, op.getName());
      return null;
    }

  }

  public static NodeProcessor getTableScanProc() {
    return new TableScanLineage();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultLineage();
  }

  public static NodeProcessor getListSinkProc() {
    return new ListSinkLineage();
  }

  public static NodeProcessor getReduceSinkProc() {
    return new ReduceSinkLineage();
  }

  public static NodeProcessor getSelectProc() {
    return new SelectLineage();
  }

  public static NodeProcessor getFilterProc() {
    return new FilterLineage();
  }

  public static NodeProcessor getGroupByProc() {
    return new GroupByLineage();
  }

  public static NodeProcessor getJoinProc() {
    return new JoinLineage();
  }

  public static NodeProcessor getExceptionalProc() {
    return new ExceptionalLineage();
  }

  public static LineageCtx extractLineage(ParseContext pctx) throws SemanticException {
    LineageCtx ctx = new LineageCtx(pctx);

    // MapJoin, SMBMapJoin, DummyStore are added in physical optimizer.
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
        getTableScanProc());
    opRules.put(new RuleRegExp("R2", FileSinkOperator.getOperatorName() + "%|"
        + ForwardOperator.getOperatorName() + "%"),
        getDefaultProc());
    opRules.put(new RuleRegExp("R3", FilterOperator.getOperatorName() + "%"),
        getFilterProc());
    opRules.put(new RuleRegExp("R4", ReduceSinkOperator.getOperatorName() + "%"),
        getReduceSinkProc());
    opRules.put(new RuleRegExp("R5", SelectOperator.getOperatorName() + "%"),
        getSelectProc());
    opRules.put(new RuleRegExp("R6", GroupByOperator.getOperatorName() + "%"),
        getGroupByProc());
    opRules.put(new RuleRegExp("R7", CommonJoinOperator.getOperatorName() + "%"),
        getJoinProc());
    opRules.put(new RuleRegExp("R8", ListSinkOperator.getOperatorName() + "%"),
        getListSinkProc());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getExceptionalProc(), opRules, ctx);
    GraphWalker walker = new PreOrderWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(pctx.getTopOps().values());
    walker.startWalking(topNodes, null);

    return ctx;
  }

}
