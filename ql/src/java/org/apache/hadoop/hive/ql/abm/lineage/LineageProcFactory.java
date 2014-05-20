package org.apache.hadoop.hive.ql.abm.lineage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.lib.PostOrderPlanWalker;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
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
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
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
   * BaseLineage maintains the annotation/condition sources of an operator.
   *
   */
  public static abstract class BaseLineage implements NodeProcessor {

    protected Operator<? extends OperatorDesc> op = null;
    protected ArrayList<ColumnInfo> signature = null;
    protected LineageCtx ctx = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      initialize(nd, procCtx);
      // We must propagate annotation first, as in GBY we need to use isUncertain
      propagateAnnoAndCond();
      propagateLineage();
      return null;
    }

    @SuppressWarnings("unchecked")
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      op = (Operator<? extends OperatorDesc>) nd;
      ctx = (LineageCtx) procCtx;
      signature = op.getSchema().getSignature();
    }

    protected abstract void propagateLineage() throws SemanticException;

    protected void propagateAnnoAndCond() throws SemanticException {
      int numAnnoSrc = 0;
      int numCondSrc = 0;
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        Set<Operator<? extends OperatorDesc>> anno = ctx.getAnnoSrcs(parent);
        Set<Operator<? extends OperatorDesc>> cond = ctx.getCondSrcs(parent);
        if (anno != null) {
          ++numAnnoSrc;
          ctx.addAnnoSrcs(op, anno);
        }
        if (cond != null) {
          ++numCondSrc;
          ctx.addCondSrcs(op, cond);
        }
      }

      if (numAnnoSrc > 1) {
        AbmUtilities.report(ErrorMsg.SELF_JOIN_NOT_ALLOWED_FOR_ABM);
      } else if (numAnnoSrc == 1) {
        ctx.addAnnoSrc(op, op);
      } else if (numCondSrc > 0) {
        ctx.addCondSrc(op, op);
      }
    }

  }

  /**
   *
   * Processor for table scan operator.
   * Only maintains
   * 1. whether an operator's input/output are from the sampled table,
   * 2. the type of the annotation.
   * Do not maintain lineage (as there is no parent).
   *
   */
  public static class TableScanLineage extends BaseLineage {
    private TableScanOperator ts = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      ts = (TableScanOperator) nd;
    }

    @Override
    protected void propagateLineage() throws SemanticException { }

    @Override
    protected void propagateAnnoAndCond() throws SemanticException {
      ParseContext pctx = ctx.getParseContext();
      Table tab = pctx.getTopToTable().get(ts);
      if (AbmUtilities.getSampledTable().equals(tab.getTableName())) {
        ctx.addAnnoSrc(ts, ts);
      }
    }

  }

  /**
   *
   * Processor for reduce sink operator.
   *
   */
  public static class ReduceSinkLineage extends DefaultLineage {

    private ReduceSinkOperator rs = null;
    private ReduceSinkDesc desc = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      rs = (ReduceSinkOperator) nd;
      desc = rs.getConf();
    }

    @Override
    protected void propagateLineage() throws SemanticException {
      int index = 0;

      // The keys may or may not be included:
      // (1) if RS is used in join, the keys are not included
      // (2) if RS is used in group by, the keys are included
      if (signature.size() > desc.getValueCols().size()) {
        for (ExprNodeDesc expr : desc.getKeyCols()) {
          String internalName = signature.get(index).getInternalName();
          ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
          ctx.putLineage(rs, internalName, info);
          index++;
        }
      }

      for (ExprNodeDesc expr : desc.getValueCols()) {
        String internalName = signature.get(index).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
        ctx.putLineage(rs, internalName, info);
        index++;
      }
    }

  }

  /**
   *
   * Processor for select operator.
   *
   */
  public static class SelectLineage extends DefaultLineage {

    private SelectOperator sel = null;
    private SelectDesc desc = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      sel = (SelectOperator) nd;
      desc = sel.getConf();
    }

    @Override
    protected void propagateLineage() throws SemanticException {
      // If this is a selStarNoCompute then this select operator
      // is treated like a default operator, so just call the super classes
      // process method.
      if (desc.isSelStarNoCompute()) {
        super.propagateLineage();
        return;
      }

      // Otherwise we treat this as a normal select operator and look at
      // the expressions.
      int index = 0;
      for (ExprNodeDesc expr : desc.getColList()) {
        String internalName = signature.get(index).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
        ctx.putLineage(sel, internalName, info);
        index++;
      }
    }

  }

  public static class FilterLineage extends DefaultLineage {

    protected FilterOperator fil = null;
    protected FilterDesc desc = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      // We do not support sampling.
      if (desc.getIsSamplingPred()) {
        AbmUtilities.report(ErrorMsg.SAMPLE_NOT_ALLOWED_FOR_ABM);
      }

      ExprProcFactory.checkFilter(parent, ctx, desc.getPredicate());

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      fil = (FilterOperator) nd;
      desc = fil.getConf();
    }

  }

  /**
   *
   * Processor for group by operator.
   *
   */
  public static class GroupByLineage extends DefaultLineage {

    private static final HashSet<String> extrema =
        new HashSet<String>(Arrays.asList("min", "max"));
    private static final HashSet<String> basic =
        new HashSet<String>(Arrays.asList("sum", "count", "avg"));

    private GroupByOperator gby = null;
    private GroupByDesc desc = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      gby = (GroupByOperator) nd;
      desc = gby.getConf();
    }

    @Override
    protected void propagateLineage() throws SemanticException {
      // For group by operators, columnExprMap only contains the key columns.
      // We should include both key and aggregates columns here.
      // Use "schema" to get the full info -- [keys, aggregates]
      int index = 0;

      // In keys
      for (ExprNodeDesc expr : desc.getKeys()) {
        String internalName = signature.get(index).getInternalName();
        ExprInfo info = ExprProcFactory.extractExprInfo(parent, ctx, expr);
        if (info.hasAggrOutput()) {
          AbmUtilities.report(ErrorMsg.EQUAL_OF_AGGR_NOT_ABM_ELIGIBLE);
        }
        ctx.putLineage(gby, internalName, info);
        index++;
      }

      // There are two types of group by plan:
      // 1. GRY
      // 2. GRY --> RS --> GRY
      // We only need to check in the first aggregate.
      boolean toCheckAggr = !(parent instanceof ReduceSinkOperator);
      boolean sampled = ctx.isUncertain(gby);

      // In aggregates
      for (AggregationDesc agg : desc.getAggregators()) {
        String internalName = signature.get(index).getInternalName();
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
          } else if (agg.getGenericUDAFName().equalsIgnoreCase("count")
              && !agg.getParameters().isEmpty()) {
            AbmUtilities.report(ErrorMsg.COUNT_PARAM_NOT_ALLOWED_FOR_ABM);
          }
          if (info.hasAggrOutput()) {
            AbmUtilities.report(ErrorMsg.AGGR_OF_AGGR_NOT_ABM_ELIGIBLE);
          }
          info.setHasAggrOutput(true);
        }
        ctx.putLineage(gby, internalName, info);
        index++;
      }
    }

    @Override
    protected void propagateAnnoAndCond() throws SemanticException {
      int numAnnoSrc = 0;
      int numCondSrc = 0;

      Set<Operator<? extends OperatorDesc>> anno = ctx.getAnnoSrcs(parent);
      Set<Operator<? extends OperatorDesc>> cond = ctx.getCondSrcs(parent);
      if (anno != null) {
        ++numAnnoSrc;
        ctx.addCondSrcs(op, anno);
      }
      if (cond != null) {
        ++numCondSrc;
        ctx.addCondSrcs(op, cond);
      }

      if (numAnnoSrc > 0 || numCondSrc > 0) {
        ctx.addCondSrc(op, op);
      }
    }

  }

  /**
   *
   * Processor for join operator.
   *
   */
  public static class JoinLineage extends BaseLineage {

    private JoinOperator join = null;
    private JoinDesc desc = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      join = (JoinOperator) nd;
      desc = join.getConf();
    }

    @Override
    protected void propagateLineage() throws SemanticException {
      // Join filters are used when outer joins exist and predicates cannot be pushed down

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
        int index = 0;
        List<ExprNodeDesc> exprs = desc.getExprs().get(tag);
        for (ColumnInfo ci : signature) {
          String internalName = ci.getInternalName();
          if (desc.getReversedExprs().get(internalName) != tag) {
            continue;
          }
          // Otherwise look up the expression corresponding to this ci
          ExprNodeDesc expr = exprs.get(index);

          ExprInfo info = ExprProcFactory.extractExprInfo(rs, ctx, expr);
          ctx.putLineage(join, internalName, info);
          index++;
        }
      }
    }

  }

  /**
   *
   * Default processor for FileSink, Forward, Filter.
   * This basically passes the input as such to the output.
   *
   */
  public static class DefaultLineage extends BaseLineage {

    // We have a single parent!
    protected Operator<? extends OperatorDesc> parent = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      parent = op.getParentOperators().get(0);
    }

    @Override
    protected void propagateLineage() throws SemanticException {
      // Get the row schema of the input operator.
      for (ColumnInfo ci : signature) {
        String internalName = ci.getInternalName();
        ExprInfo info = new ExprInfo(parent, internalName);

        ExprInfo dep = ctx.getLineage(parent, internalName);
        if (dep != null) {
          info.setHasAggrOutput(dep.hasAggrOutput());
        }

        ctx.putLineage(op, internalName, info);
      }
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

  public static NodeProcessor getDefaultProc() {
    return new DefaultLineage();
  }

  public static NodeProcessor getExceptionalProc() {
    return new ExceptionalLineage();
  }

  public static LineageCtx extractLineage(ParseContext pctx) throws SemanticException {
    LineageCtx ctx = new LineageCtx(pctx);

    // MapJoin, SMBMapJoin, DummyStore are added in physical optimizer.
    // We do not consider ListSink, as ABM rewriting is added before MapJoin is added, which is
    // before ListSink optimization.
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

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getExceptionalProc(), opRules, ctx);
    GraphWalker walker = new PostOrderPlanWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(pctx.getTopOps().values());
    walker.startWalking(topNodes, null);

    return ctx;
  }

}
