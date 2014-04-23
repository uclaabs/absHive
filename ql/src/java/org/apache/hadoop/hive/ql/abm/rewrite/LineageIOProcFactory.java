package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.abm.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

public class LineageIOProcFactory {

  private static final String LIN_SUM = "lin_sum";
  private static final String TID = "tid";

  public static class GroupByRewriter implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gby = (GroupByOperator) nd;
      LineageIOCtx ctx = (LineageIOCtx) procCtx;

      // Group By will reuse the tid column as the lineage column

      GroupByDesc desc = gby.getConf();
      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();

      boolean firstGby = !(parent instanceof ReduceSinkOperator);

      // Get tid if this is the first Gby, otherwise, get the lineage column passed from ReduceSink
      Integer index = (firstGby) ? ctx.getTidColumn(parent) : ctx.getLineageColumn(parent);
      if (index == null) {
        return null;
      }

      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>(
          Utils.generateColumnDescs(parent, index));

      if (!ctx.hasLineageToWrite(gby)) {
        if (firstGby) {
          // aggParameter[0] is tid
          addTidToAggrsAndConditionColumn(desc, aggParameters.get(0));
        }
        return null;
      }

      // Add the lineage column
      if (firstGby) {
        ArrayList<Integer> valCols = new ArrayList<Integer>();
        for (int idx : ctx.getValColsToWrite(gby)) {
          AggregationDesc aggr = aggrs.get(idx);
          ArrayList<ExprNodeDesc> params = aggr.getParameters();
          assert params.size() <= 1;
          if (params.size() > 0) {
            // If no input parameter, don't write
            // NOTE: When we try to read the column, if cannot find it, return 1 by default
            valCols.add(idx);
            aggParameters.add(aggr.getParameters().get(0));
          }
        }
        ctx.putValColsToWrite(gby, valCols);
      }

      if (firstGby) {
        // aggParameter[0] is tid
        addTidToAggrsAndConditionColumn(desc, aggParameters.get(0));
      }

      GroupByDesc.Mode amode = desc.getMode();
      GenericUDAFEvaluator.Mode emode = SemanticAnalyzer.groupByDescModeToUDAFMode(
          amode, false);
      GenericUDAFEvaluator udafEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(
          LIN_SUM, aggParameters, null, false, false);
      assert (udafEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          udafEvaluator, emode, aggParameters);
      AggregationDesc aggrDesc = new AggregationDesc(LIN_SUM,
          udaf.genericUDAFEvaluator, udaf.convertedParameters, false,
          emode);

      // colExprMap only has keys in it, so don't add this aggregation
      RowResolver rowResovler = ctx.getParseContext().getOpParseCtx().get(gby).getRowResolver();
      ArrayList<String> outputColNames = desc.getOutputColumnNames();
      int numKeys = desc.getKeys().size();

      aggrs.add(aggrDesc);
      String colName = Utils.getColumnInternalName(numKeys + aggrs.size() - 1);
      rowResovler.put("", colName, new ColumnInfo(colName, udaf.returnType, "", false));
      outputColNames.add(colName);

      if (firstGby) {
        ctx.putLineageColumn(gby, outputColNames.size() - 1);
      } else {
        // broadcast the lineage
        desc.setBroadcastId(ctx.getBroadcastId(gby));
        desc.setValColsInLineage(ctx.getValColsToWrite(gby));
      }

      return null;
    }

    // hack: insert tid into the CONDITION column
    private void addTidToAggrsAndConditionColumn(GroupByDesc desc,
        ExprNodeDesc tid) throws SemanticException {
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();
      for (int i = 0; i < aggrs.size(); ++i) {
        AggregationDesc aggr = aggrs.get(i);
        ArrayList<ExprNodeDesc> oldParameters = aggr.getParameters();
        oldParameters.add(0, tid); // tid
        GroupByDesc.Mode amode = desc.getMode();
        GenericUDAFEvaluator.Mode emode = SemanticAnalyzer.groupByDescModeToUDAFMode(
            amode, false);
        GenericUDAFEvaluator udafEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(
            aggr.getGenericUDAFName(), oldParameters, null, false, false);
        assert (udafEvaluator != null);
        GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
            udafEvaluator, emode, oldParameters);
        aggrs.set(i, new AggregationDesc(aggr.getGenericUDAFName(),
            udaf.genericUDAFEvaluator, udaf.convertedParameters, false,
            emode));
      }
    }

  }

  public static class ReduceSinkRewriter implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator rs = (ReduceSinkOperator) nd;
      LineageIOCtx ctx = (LineageIOCtx) procCtx;

      // Explicitly forward the lineage/tid column
      // Note: we still maintain the row resolver
      Operator<? extends OperatorDesc> parent = rs.getParentOperators().get(0);

      if (ctx.withTid(rs)) {
        for (ExprNodeColumnDesc col : Utils.generateColumnDescs(parent, ctx.getTidColumn(parent))) {
          addNewColumn(ctx, rs, col);

          ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
          ctx.putTidColumn(rs, colInfos.size() - 1);
        }
      }

      for (ExprNodeColumnDesc col : Utils.generateColumnDescs(parent, ctx.getLineageColumn(parent))) {
        addNewColumn(ctx, rs, col);

        ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
        ctx.putLineageColumn(rs, colInfos.size() - 1);
      }

      return null;
    }

    private void addNewColumn(LineageIOCtx ctx, ReduceSinkOperator rs, ExprNodeColumnDesc col) {
      Map<String, ExprNodeDesc> colExprMap = rs.getColumnExprMap();
      ReduceSinkDesc desc = rs.getConf();
      ArrayList<String> outputValColNames = desc.getOutputValueColumnNames();
      ArrayList<ExprNodeDesc> valCols = desc.getValueCols();
      RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(rs).getRowResolver();

      valCols.add(col);
      String valOutputName = Utils.getColumnInternalName(valCols.size() - 1);
      outputValColNames.add(valOutputName);
      String valName = Utilities.ReduceField.VALUE.toString() + "." + valOutputName;
      colExprMap.put(valName, col);
      rowResolver.put("", valName, new ColumnInfo(valName, col.getTypeInfo(), null, false));
    }

  }

  public static class SelectRewriter implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator sel = (SelectOperator) nd;
      LineageIOCtx ctx = (LineageIOCtx) procCtx;

      if (!ctx.withTid(sel)) {
        return null;
      }

      // Explicitly forward the lineage column
      // Note: we still maintain the row resolvers
      Operator<? extends OperatorDesc> parent = sel.getParentOperators().get(0);
      for (ExprNodeColumnDesc col : Utils.generateColumnDescs(parent, ctx.getTidColumn(parent))) {
        Map<String, ExprNodeDesc> colExprMap = sel.getColumnExprMap();
        SelectDesc desc = sel.getConf();
        List<String> outputColNames = desc.getOutputColumnNames();
        List<ExprNodeDesc> cols = desc.getColList();
        RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(sel).getRowResolver();

        cols.add(col);
        String outputName = Utils.getColumnInternalName(cols.size() - 1);
        outputColNames.add(outputName);
        colExprMap.put(outputName, col);
        rowResolver
            .put("", outputName, new ColumnInfo(outputName, col.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
        ctx.putTidColumn(sel, colInfos.size() - 1);
      }

      return null;
    }

  }

  public static class JoinRewriter implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      JoinOperator join = (JoinOperator) nd;
      LineageIOCtx ctx = (LineageIOCtx) procCtx;
      JoinDesc desc = join.getConf();

      if (!ctx.withTid(join)) {
        return null;
      }

      // Forward the tid columns
      List<String> outputColumnNames = desc.getOutputColumnNames();
      Map<String, Byte> reversedExprs = desc.getReversedExprs();
      Map<Byte, List<ExprNodeDesc>> exprMap = desc.getExprs();
      Map<String, ExprNodeDesc> colExprMap = join.getColumnExprMap();
      RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(join).getRowResolver();

      int count = 0;
      for (Operator<? extends OperatorDesc> parent : join.getParentOperators()) {
        ReduceSinkOperator rs = (ReduceSinkOperator) parent;
        RowResolver inputRR = ctx.getParseContext().getOpParseCtx().get(rs).getRowResolver();
        Byte tag = (byte) rs.getConf().getTag();
        for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(rs, ctx.getTidColumn(rs))) {
          ++count;
          String colName = Utils.getColumnInternalName(outputColumnNames.size());
          outputColumnNames.add(colName);
          reversedExprs.put(colName, tag);
          List<ExprNodeDesc> exprs = exprMap.get(tag);
          exprs.add(cond);
          colExprMap.put(colName, cond);

          String[] names = inputRR.reverseLookup(cond.getColumn());
          ColumnInfo ci = inputRR.get(names[0], names[1]);
          rowResolver.put(names[0], names[1],
              new ColumnInfo(colName, ci.getType(), names[0],
                  ci.getIsVirtualCol(), ci.isHiddenVirtualCol()));

          ctx.putTidColumn(join, outputColumnNames.size() - 1);
        }
      }
      assert count == 1;

      return null;
    }

  }

  public static class TableScanRewriter implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator ts = (TableScanOperator) nd;
      LineageIOCtx ctx = (LineageIOCtx) procCtx;

      if (ctx.withTid(ts)) {
        ArrayList<ColumnInfo> colInfos = ts.getSchema().getSignature();
        for (int i = 0; i < colInfos.size(); ++i) {
          if (colInfos.get(i).getInternalName().equals(TID)) {
            ctx.putTidColumn(ts, i);
          }
        }
      }

      return null;
    }
  }

  /**
   *
   * DefaultRewriter simply add the tid column index to the ctx.
   * For Forward, FileSink, Filter.
   *
   */
  public static class DefaultRewriter implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      LineageIOCtx ctx = (LineageIOCtx) procCtx;

      if (ctx.withTid(op)) {
        Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);
        RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(op).getRowResolver();
        RowResolver parentRR = ctx.getParseContext().getOpParseCtx().get(parent).getRowResolver();
        ArrayList<ColumnInfo> parentColInfos = parent.getSchema().getSignature();

        Integer index = ctx.getTidColumn(parent);
        ColumnInfo ci = parentColInfos.get(index);
        String[] name = parentRR.reverseLookup(ci.getInternalName());
        rowResolver.put(name[0], name[1], ci);
        // Maintain the tid column index
        ctx.putTidColumn(op, rowResolver.getColumnInfos().size() - 1);
      }

      return null;
    }

  }

  public static NodeProcessor getGroupByRewriter() {
    return new GroupByRewriter();
  }

  public static NodeProcessor getReduceSinkRewriter() {
    return new ReduceSinkRewriter();
  }

  public static NodeProcessor getSelectRewriter() {
    return new SelectRewriter();
  }

  public static NodeProcessor getJoinRewriter() {
    return new JoinRewriter();
  }

  public static NodeProcessor getTableScanRewriter() {
    return new TableScanRewriter();
  }

  public static NodeProcessor getDefaultRewriter() {
    return new DefaultRewriter();
  }

  private static long[] generateLineageLoadingInfo(LineageIOCtx ctx,
      Operator<? extends OperatorDesc> op, Set<AggregateInfo> conds) {
    HashSet<Long> set = new HashSet<Long>();

    for (AggregateInfo cond : conds) {
      set.add(ctx.getBroadcastId(cond.getGroupByOperator()));
    }

    long[] ret = new long[set.size()];
    int i = 0;
    for (long idx : set) {
      ret[i++] = idx;
    }
    return ret;
  }

  public static void setUpLineageIO(RewriteProcCtx rctx) throws SemanticException {
    LineageIOCtx ctx = new LineageIOCtx(rctx.getLineageCtx(),
        rctx.getAllLineagesToWrite(), rctx.getLineageReaders());

    // Writers
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", GroupByOperator.getOperatorName() + "%"),
        getGroupByRewriter());
    opRules.put(new RuleRegExp("R2", ReduceSinkOperator.getOperatorName() + "%"),
        getReduceSinkRewriter());
    opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
        getSelectRewriter());
    opRules.put(new RuleRegExp("R4", JoinOperator.getOperatorName() + "%"),
        getJoinRewriter());
    opRules.put(new RuleRegExp("R5", TableScanOperator.getOperatorName() + "%"),
        getTableScanRewriter());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultRewriter(), opRules, ctx);
    GraphWalker walker = new PreOrderWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(ctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);

    // Readers can only be Select or GroupBy
    for (Operator<? extends OperatorDesc> reader : rctx.getLineageReaders()) {
      if (reader instanceof SelectOperator) {
        SelectOperator sel = (SelectOperator) reader;
        sel.getConf().setLineageToLoad(
            generateLineageLoadingInfo(ctx, sel, rctx.getConditions(sel)));
      } else {
        GroupByOperator gby = (GroupByOperator) reader;
        gby.getConf().setLineageToLoad(
            generateLineageLoadingInfo(ctx, gby, rctx.getConditions(gby)));
      }
    }
  }

}
