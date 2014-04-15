package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;

/**
 *
 * RewriteProcFactory maintains the following information:
 * (1) Use GroupByOperator identity to identify the tuple lineage,
 * and use the index in ArrayList\<AggregationDesc\> to identify
 * the value lineage within the tuple lineage.
 * (2) Map columns generated from aggregates to the corresponding
 * AggregationDesc id (GroupByOperator identity + index).
 * (3) Map each predicate/aggregate to its correlated AggregationDesc's.
 *
 */
public class RewriteProcFactory {

  private static final String COND_MERGE = "cond_merge";
  private static final String COND_SUM = "cond_sum";

  public static GenericUDF getUdf(String udfName) {
    // Remember to register the functions:
    // in org.apache.hadoop.hive.ql.exec.FunctionRegistry, use registerUDF
    FunctionInfo fi = FunctionRegistry.getFunctionInfo(udfName);
    assert fi != null;
    // getGenericUDF() actually clones the UDF. Just call it once and reuse.
    return fi.getGenericUDF();
  }

  public static void createAndConnectSelectOperator(
      Operator<? extends OperatorDesc> op, RewriteProcCtx ctx,
      ExprNodeDesc... additionalConds) throws SemanticException {
    GenericUDF srvMerge = getUdf(COND_MERGE);

    ArrayList<ColumnInfo> colInfos = op.getSchema().getSignature();
    List<Integer> indexes = ctx.getCondColumnIndexes(op);
    assert indexes != null;
    List<ExprNodeColumnDesc> conds = Utils.generateColumnDescs(op, indexes);

    // the condition column
    ExprNodeDesc cond = conds.get(0);

    /**
     * notice: use Arrays.asList will cause bugs
     */
    for (int i = 1; i < conds.size(); ++i) {
      List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
      list.add(cond);
      list.add(conds.get(i));
      cond = ExprNodeGenericFuncDesc.newInstance(srvMerge, list);
    }
    for (ExprNodeDesc newCond : additionalConds) {
      List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
      list.add(cond);
      list.add(newCond);
      cond = ExprNodeGenericFuncDesc.newInstance(srvMerge, list);
    }

    List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
    List<String> colName = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    RowResolver rowResolver = new RowResolver();

    // Forward original columns
    for (int i = 0; i < colInfos.size(); ++i) {
      if (indexes.indexOf(i) != -1) {
        continue;
      }
      ColumnInfo colInfo = colInfos.get(i);
      ExprNodeColumnDesc column = new ExprNodeColumnDesc(colInfo.getType(),
          colInfo.getInternalName(),
          colInfo.getTabAlias(), colInfo.getIsVirtualCol(), colInfo.isSkewedCol());
      columns.add(column);
      // Here we are different from SemanticAnalyzer,
      // as we want to keep the names the same as Fil.
      colName.add(colInfo.getInternalName());
      colExprMap.put(colInfo.getInternalName(), column);
      rowResolver.put("", colInfo.getInternalName(),
          new ColumnInfo(colInfo.getInternalName(),
              colInfo.getType(), colInfo.getTabAlias(), colInfo.getIsVirtualCol()));
    }

    columns.add(cond);
    String condName = Utils.getColumnInternalName(columns.size() - 1);
    colName.add(condName);
    colExprMap.put(condName, cond);
    rowResolver.put("", condName,
        new ColumnInfo(condName, cond.getTypeInfo(), null, false));


    // Create SEL
    SelectDesc desc = new SelectDesc(columns, colName);
    @SuppressWarnings("unchecked")
    Operator<SelectDesc> sel = OperatorFactory.get((Class<SelectDesc>) desc.getClass());
    sel.setConf(desc);
    sel.setSchema(rowResolver.getRowSchema());
    sel.setColumnExprMap(colExprMap);

    // Change the connection
    List<Operator<? extends OperatorDesc>> parents =
        new ArrayList<Operator<? extends OperatorDesc>>();
    parents.add(op);
    sel.setParentOperators(parents);
    List<Operator<? extends OperatorDesc>> children =
        new ArrayList<Operator<? extends OperatorDesc>>(op.getChildOperators());
    sel.setChildOperators(children);

    for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
      List<Operator<? extends OperatorDesc>> newParents = child.getParentOperators();
      newParents.remove(op);
      newParents.add(sel);
      child.setParentOperators(newParents);
    }
    List<Operator<? extends OperatorDesc>> newChildren =
        new ArrayList<Operator<? extends OperatorDesc>>();
    newChildren.add(sel);
    op.setChildOperators(newChildren);

    // Put SEL into ParseContext
    OpParseContext opParseContext = new OpParseContext(rowResolver);
    ctx.getParseContext().getOpParseCtx().put(sel, opParseContext);

    // info (3)
    HashSet<LineageInfo> condLineage = ctx.getConditions(op);
    if (condLineage != null) {
      ctx.addConditionLineages(sel, condLineage);
    }

    // info (2)
    for (int i = 0; i < colInfos.size(); ++i) {
      if (indexes.indexOf(i) != -1) {
        continue;
      }
      ColumnInfo colInfo = colInfos.get(i);
      String internalName = colInfo.getInternalName();
      LineageInfo linfo = ctx.getLineage(op, internalName);
      if (linfo != null) {
        ctx.addLineage(sel, internalName, linfo);
      }
    }

    ctx.putCondColumnIndex(sel, colName.size() - 1);

    if (ctx.getConditions(sel) != null) {
      // Tell ctx that this SEL needs to load the lineage
      HashSet<GroupByOperator> sources = new HashSet<GroupByOperator>();
      for (LineageInfo li : ctx.getConditions(sel)) {
        sources.add(li.getGroupByOperator());
      }
      if (sources.size() > 1) {
        ctx.addToLineageReaders(sel);
      }
    }
  }

  private static void cleanRowResolver(RewriteProcCtx ctx, Operator<? extends OperatorDesc> op) {
    OpParseContext opParseCtx = ctx.getParseContext().getOpParseCtx().get(op);
    RowResolver oldRR = opParseCtx.getRowResolver();
    RowResolver newRR = new RowResolver();
    ArrayList<ColumnInfo> colInfos = op.getSchema().getSignature();
    for (ColumnInfo ci : colInfos) {
      String[] name = oldRR.reverseLookup(ci.getInternalName());
      newRR.put((name[0] == null) ? "" : name[0], name[1], ci);
    }
    op.setSchema(newRR.getRowSchema());
    opParseCtx.setRowResolver(newRR);
    assert op.getSchema().getSignature() != null;
  }

  /**
   *
   * Base1Processor maintains info (3) by propagating the correlated columns.
   *
   */
  public static class Base1Processor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      cleanRowResolver(ctx, op);

      List<Operator<? extends OperatorDesc>> parents = op.getParentOperators();
      if (parents != null) {
        for (Operator<? extends OperatorDesc> parent : parents) {
          HashSet<LineageInfo> condLineage = ctx.getConditions(parent);
          if (condLineage != null) {
            ctx.addConditionLineages(op, condLineage);
          }
        }
      }

      return null;
    }

  }

  /**
   *
   * Base2Processor maintains info (1) and (2), and propagates the correct types.
   * Since it works with the original columns (to maintain info (2) and propagate types),
   * it must be called before you make any change to the schema.
   *
   */
  public static abstract class Base2Processor extends Base1Processor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      // info (2)
      LineageCtx lctx = ctx.getLineageCtx();
      HashMap<String, ExprInfo> columnInfos = lctx.get(op);
      if (columnInfos != null) {
        for (Entry<String, ExprInfo> entry : columnInfos.entrySet()) {
          ExprInfo ei = entry.getValue();
          if (!ei.hasAggrOutput()) {
            continue;
          }
          Set<String> cols = ei.getColumns();
          assert cols.size() == 1;
          for (String col : cols) {
            Operator<? extends OperatorDesc> parent = ei.getOperator();
            ctx.addLineage(op, entry.getKey(), ctx.getLineage(parent, col));
          }
        }
      }

      // Propagates the correct types of the original columns
      ArrayList<ColumnInfo> columns = op.getSchema().getSignature();
      for (ColumnInfo ci : columns) {
        LineageInfo li = ctx.getLineage(op, ci.getInternalName());
        if (li != null) {
          ci.setType(li.getTypeInfo());
        }
      }

      return null;
    }

  }

  /**
   *
   * DefaultProcessor implicitly forwards the condition column if exists,
   * i.e., we do not need to make ExprNodeDesc or change the schema (they use their parent's
   * schema).
   * For Forward, FileSink.
   *
   */
  public static class DefaultProcessor extends Base2Processor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      if (op.getParentOperators() == null) {
        return null;
      }

      // For maintaining condition column, DO NOTHING!
      // Note: we assume a single parent
      Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);
      List<Integer> indexes = ctx.getCondColumnIndexes(parent);

      if (indexes == null) {
        return null;
      }

      RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(op).getRowResolver();
      RowResolver parentRR = ctx.getParseContext().getOpParseCtx().get(parent).getRowResolver();
      ArrayList<ColumnInfo> parentColInfos = parent.getSchema().getSignature();

      for (Integer index : indexes) {
        ColumnInfo ci = parentColInfos.get(index);
        String[] name = parentRR.reverseLookup(ci.getInternalName());
        rowResolver.put(name[0], name[1], ci);
        // Maintain the condition column index
        ctx.putCondColumnIndex(op, index);
      }

      return null;
    }

  }

  /**
   *
   * ReduceSinkProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class ReduceSinkProcessor extends Base2Processor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      ReduceSinkOperator rs = (ReduceSinkOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      // Explicitly forward the condition column
      // Note: we still maintain the row resolver
      Operator<? extends OperatorDesc> parent = rs.getParentOperators().get(0);
      for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
        Map<String, ExprNodeDesc> colExprMap = rs.getColumnExprMap();
        ReduceSinkDesc desc = rs.getConf();
        ArrayList<String> outputValColNames = desc.getOutputValueColumnNames();
        ArrayList<ExprNodeDesc> valCols = desc.getValueCols();
        // ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
        RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(rs).getRowResolver();

        valCols.add(cond);
        String valOutputName = Utils.getColumnInternalName(valCols.size() - 1);
        outputValColNames.add(valOutputName);
        String valName = Utilities.ReduceField.VALUE.toString() + "." + valOutputName;
        colExprMap.put(valName, cond);
        // colInfos.add(new ColumnInfo(valName, cond.getTypeInfo(), null, false));
        rowResolver.put("", valName, new ColumnInfo(valName, cond.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
        ctx.putCondColumnIndex(rs, colInfos.size() - 1);
      }

      return null;
    }

  }

  /**
   *
   * SelectProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class SelectProcessor extends Base2Processor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      SelectOperator sel = (SelectOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      // Explicitly forward the condition column
      // Note: we still maintain the row resolvers
      Operator<? extends OperatorDesc> parent = sel.getParentOperators().get(0);
      for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
        Map<String, ExprNodeDesc> colExprMap = sel.getColumnExprMap();
        SelectDesc desc = sel.getConf();
        List<String> outputColNames = desc.getOutputColumnNames();
        List<ExprNodeDesc> cols = desc.getColList();
        // ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
        RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(sel).getRowResolver();

        cols.add(cond);
        String outputName = Utils.getColumnInternalName(cols.size() - 1);
        outputColNames.add(outputName);
        colExprMap.put(outputName, cond);
        // colInfos.add(new ColumnInfo(outputName, cond.getTypeInfo(), null, false));
        rowResolver
            .put("", outputName, new ColumnInfo(outputName, cond.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
        ctx.putCondColumnIndex(sel, colInfos.size() - 1);
      }

      return null;
    }

  }

  /**
   *
   * GroupByProcessor maintains info (1) by putting aggregate columns --> itself into
   * RewriteContext.
   * Since GroupBy is the source of info (2), so it does not need to maintain info (2).
   * It maintains info (3) by putting itself as an extra condition.
   *
   */
  public static class GroupByProcessor extends Base1Processor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      GroupByOperator gby = (GroupByOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      if (!ctx.getLineageCtx().isSampled(gby)) {
        return null;
      }

      GroupByDesc desc = gby.getConf();
      ArrayList<ColumnInfo> cols = gby.getSchema().getSignature();
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();

      int numKeys = desc.getKeys().size();
      for (int i = numKeys; i < cols.size(); ++i) {
        // hack!: we assume no null value. Otherwise, count(x) and count(*) should be treated
        // differently and that make the lineage larger
        ctx.addLineage(gby, cols.get(i).getInternalName(), new LineageInfo(gby, i - numKeys));
      }

      // Rewrite AggregationDesc:
      // (1) Use the ABM version of SUM/COUNT/AVERAGE
      // (2) Change return type
      for (int i = 0; i < aggrs.size(); ++i) {
        // (1)
        AggregationDesc aggr = aggrs.get(i);
        String convertedUdafName = getConvertUdafName(aggr.getGenericUDAFName());
        GenericUDAFEvaluator genericUDAFEvaluator =
            SemanticAnalyzer.getGenericUDAFEvaluator(convertedUdafName,
                aggr.getParameters(), null, aggr.getDistinct(),
                false);
        assert (genericUDAFEvaluator != null);
        GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
            genericUDAFEvaluator, aggr.getMode(),
            aggr.getParameters());
        aggrs.set(i, new AggregationDesc(convertedUdafName,
            udaf.genericUDAFEvaluator, udaf.convertedParameters,
            aggr.getDistinct(), aggr.getMode()));
        // (2): we only need to change the ColumnInfo in the schema
        // as other places (e.g., RowResolver) reference to the same ColumnInfo
        gby.getSchema().getSignature().get(numKeys + i).setType(udaf.returnType);
      }

      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);
      boolean firstGby = !(parent instanceof ReduceSinkOperator);
      boolean withConditions = (ctx.getConditions(gby) != null);
      boolean dedup = aggrs.isEmpty();

      if (withConditions || dedup) {
        // Add the condition column only if this group by has conditions or it is a deduplication.
        List<ExprNodeColumnDesc> condCols = Utils.generateColumnDescs(parent,
            ctx.getCondColumnIndexes(parent));
        assert condCols.size() < 2;
        GroupByDesc.Mode amode = desc.getMode();
        GenericUDAFEvaluator.Mode emode = SemanticAnalyzer.groupByDescModeToUDAFMode(
            amode, false);
        ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>(condCols);
        GenericUDAFEvaluator udafEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(
            COND_SUM, aggParameters, null, false, false);
        assert (udafEvaluator != null);
        GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
            udafEvaluator, emode, aggParameters);
        AggregationDesc aggrDesc = new AggregationDesc(COND_SUM,
            udaf.genericUDAFEvaluator, udaf.convertedParameters, false,
            emode);

        // colExprMap only has keys in it, so don't add this aggregation
        RowResolver rowResovler = ctx.getParseContext().getOpParseCtx().get(gby).getRowResolver();
        ArrayList<String> outputColNames = desc.getOutputColumnNames();

        aggrs.add(aggrDesc);
        String colName = Utils.getColumnInternalName(numKeys + aggrs.size() - 1);
        rowResovler.put("", colName, new ColumnInfo(colName, udaf.returnType, "", false));
        outputColNames.add(colName);

        ctx.putCondColumnIndex(gby, outputColNames.size() - 1);

        if (firstGby) {
          // If this is the first group-by and this has conditions, read lineage.
          if (withConditions) {
            ctx.addToLineageReaders(gby);
          }
        } else {
          // Add itself into condition lineages only if it is the second group-by and a deduplication,
          // as COUNT>0 is implicitly implied by the aggregates.
          if (dedup) {
            ctx.addConditionLineage(gby, new LineageInfo(gby, -1));
          }
        }
      }

      return null;
    }

    private String getConvertUdafName(String udaf) {
      if (udaf.equals("sum")) {
        return "srv_sum";
      } else if (udaf.equals("avg")) {
        return "srv_avg";
      } else {
        assert udaf.equals("count");
        return "srv_count";
      }
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
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      FilterDesc desc = fil.getConf();
      Operator<? extends OperatorDesc> parent = fil.getParentOperators().get(0);
      ExprNodeDesc ret = rewrite(fil, parent, ctx, desc.getPredicate());
      if (ret != null) {
        desc.setPredicate(ret);
      }

      // Add an SEL after FIL to transform the condition column
      if (ctx.getTransform(fil) != null) {
        createAndConnectSelectOperator(fil, ctx, ctx.getTransform(fil).toArray(new ExprNodeDesc[0]));
      }

      return null;
    }

    private ExprNodeDesc rewrite(FilterOperator filter, Operator<? extends OperatorDesc> parent,
        RewriteProcCtx ctx, ExprNodeDesc pred) throws SemanticException {
      if (pred instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) pred;
        GenericUDF udf = func.getGenericUDF();

        if (udf instanceof GenericUDFOPAnd) {
          List<ExprNodeDesc> children = func.getChildExprs();
          for (int i = 0; i < children.size(); ++i) {
            ExprNodeDesc ret = rewrite(filter, parent, ctx, children.get(i));
            if (ret != null) {
              children.set(i, ret);
            }
          }
          return null;
        }

        if ((udf instanceof GenericUDFOPEqualOrGreaterThan)
            || (udf instanceof GenericUDFOPEqualOrLessThan)
            || (udf instanceof GenericUDFOPGreaterThan)
            || (udf instanceof GenericUDFOPLessThan)) {
          // Code indicating which child(ren) is(are) Srv
          // The i-th bit is 1 if the i-th child is a Srv
          int changeCode = 0;

          List<ExprNodeDesc> children = func.getChildExprs();
          assert children.size() == 2;
          for (int i = 0; i < children.size(); ++i) {
            ExprNodeDesc ret = rewrite(filter, parent, ctx, children.get(i));
            if (ret != null) {
              changeCode = ((changeCode << 1) | 1);
              children.set(i, ret);
            }
          }

          // Convert comparison to special ABM comparison
          if (changeCode != 0) {
            normalizeParameters(children, changeCode);

            // Add to ctx: filter transforms the condition set of the annotation
            ctx.addTransform(filter, ExprNodeGenericFuncDesc.newInstance(
                getUdf(getCondUdfName(udf, changeCode)), children));

            // Rewrite the predicate.
            return ExprNodeGenericFuncDesc.newInstance(
                getUdf(getPredUdfName(udf, changeCode)),
                children);
          }
        }

        return null;
      }

      if (pred instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc column = (ExprNodeColumnDesc) pred;
        LineageInfo li = ctx.getLineage(parent, column.getColumn());
        if (li != null) {
          column.setTypeInfo(li.getTypeInfo());
          ctx.addConditionLineage(filter, li);
          return column;
        }
        return null;
      }

      return null;
    }

    // Normalize the parameter list to move Srv to the left hand side
    private void normalizeParameters(List<ExprNodeDesc> children, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right not is a Srv
      // code 3: both left and right are Srvs
      if (code == 1) {
        // Swap
        ExprNodeDesc tmp = children.get(0);
        children.set(0, children.get(1));
        children.set(1, tmp);
      }
    }

    private String getCondUdfName(GenericUDF udf, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right not is a Srv
      // code 3: both left and right are Srvs
      switch (code) {
      case 1:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_less_than";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_greater_than";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_less_than";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_greater_than";
        }

      case 2:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_greater_than";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_less_than";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_greater_than";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_less_than";
        }

      default: // 3
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "srv_equal_or_greater_than_srv";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "srv_equal_or_less_than_srv";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "srv_greater_than_srv";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "srv_less_than_srv";
        }
      }
    }

    private String getPredUdfName(GenericUDF udf, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right not is a Srv
      // code 3: both left and right are Srvs
      switch (code) {
      case 1:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "x<=";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "x>=";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "x<";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "x>";
        }

      case 2:
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "x>=";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "x<=";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "x>";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "x<";
        }

      default: // 3
        if (udf instanceof GenericUDFOPEqualOrGreaterThan) {
          return "x>=x";
        } else if (udf instanceof GenericUDFOPEqualOrLessThan) {
          return "x<=x";
        } else if (udf instanceof GenericUDFOPGreaterThan) {
          return "x>x";
        } else {
          assert udf instanceof GenericUDFOPLessThan;
          return "x<x";
        }
      }
    }
  }

  public static class JoinProcessor extends Base2Processor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      JoinOperator join = (JoinOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;
      JoinDesc desc = join.getConf();

      // Forward the condition columns
      List<String> outputColumnNames = desc.getOutputColumnNames();
      Map<String, Byte> reversedExprs = desc.getReversedExprs();
      Map<Byte, List<ExprNodeDesc>> exprMap = desc.getExprs();
      Map<String, ExprNodeDesc> colExprMap = join.getColumnExprMap();
      RowResolver rowResolver = ctx.getParseContext().getOpParseCtx().get(join).getRowResolver();

      int countOfCondCols = 0;
      for (Operator<? extends OperatorDesc> parent : join.getParentOperators()) {
        ReduceSinkOperator rs = (ReduceSinkOperator) parent;
        RowResolver inputRR = ctx.getParseContext().getOpParseCtx().get(rs).getRowResolver();
        Byte tag = (byte) rs.getConf().getTag();
        for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(rs, ctx.getCondColumnIndexes(rs))) {
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

          ctx.putCondColumnIndex(join, outputColumnNames.size() - 1);
          ++countOfCondCols;
        }
      }

      if (countOfCondCols > 1) {
        createAndConnectSelectOperator(join, ctx);
      }

      return null;
    }

  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultProcessor();
  }

  public static NodeProcessor getFilterProc() {
    return new FilterProcessor();
  }

  public static NodeProcessor getReduceSinkProc() {
    return new ReduceSinkProcessor();
  }

  public static NodeProcessor getSelectProc() {
    return new SelectProcessor();
  }

  public static NodeProcessor getGroupByProc() {
    return new GroupByProcessor();
  }

  public static NodeProcessor getJoinProc() {
    return new JoinProcessor();
  }

  public static ParseContext rewritePlan(LineageCtx lctx) throws SemanticException {
    RewriteProcCtx ctx = new RewriteProcCtx(lctx);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"),
        getFilterProc());
    opRules.put(new RuleRegExp("R2", ReduceSinkOperator.getOperatorName() + "%"),
        getReduceSinkProc());
    opRules.put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"),
        getSelectProc());
    opRules.put(new RuleRegExp("R4", GroupByOperator.getOperatorName() + "%"),
        getGroupByProc());
    opRules.put(new RuleRegExp("R5", CommonJoinOperator.getOperatorName() + "%"),
        getJoinProc());

    // The dispatcher fires the processor corresponding to the closest matching rule
    // and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, ctx);
    GraphWalker walker = new PreOrderWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(ctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);

    LineageIOProcFactory.setUpLineageIO(ctx);

    return ctx.getParseContext();
  }

}
