package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.abm.lib.PreOrderWalker;
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
    // the condition column
    ExprNodeDesc condColumn = null;

    // Note: use Arrays.asList will cause bugs
    for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(op, indexes)) {
      if (condColumn == null) {
        condColumn = cond;
      } else {
        List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
        list.add(condColumn);
        list.add(cond);
        condColumn = ExprNodeGenericFuncDesc.newInstance(srvMerge, list);
      }
    }
    for (ExprNodeDesc newCond : additionalConds) {
      if (condColumn == null) {
        condColumn = newCond;
      } else {
        List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
        list.add(condColumn);
        list.add(newCond);
        condColumn = ExprNodeGenericFuncDesc.newInstance(srvMerge, list);
      }
    }

    List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
    List<String> colName = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    RowResolver rowResolver = new RowResolver();

    // Forward original columns
    for (int i = 0; i < colInfos.size(); ++i) {
      if (indexes != null && indexes.indexOf(i) != -1) {
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

    columns.add(condColumn);
    String condName = Utils.getColumnInternalName(columns.size() - 1);
    colName.add(condName);
    colExprMap.put(condName, condColumn);
    rowResolver.put("", condName,
        new ColumnInfo(condName, condColumn.getTypeInfo(), null, false));


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

    ctx.putCondColumnIndex(sel, colName.size() - 1);

    if (ctx.getConditions(sel) != null) {
      // Tell ctx that this SEL needs to load the lineage
      HashSet<GroupByOperator> sources = new HashSet<GroupByOperator>();
      for (AggregateInfo li : ctx.getConditions(sel)) {
        sources.add(li.getGroupByOperator());
      }
      if (sources.size() > 1) {
        ctx.addToLineageReaders(sel);
      }
    }
  }

  /**
   *
   * BaseProcessor propagates the correct types.
   *
   */
  public static abstract class BaseProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      // Propagates the correct types
      // Hack! We assume the columns generated by aggregates are w/ no transformation.
      ArrayList<ColumnInfo> allCols = op.getSchema().getSignature();
      for (ColumnInfo col : allCols) {
        AggregateInfo aggr = ctx.getLineage(op, col.getInternalName());
        if (aggr != null) {
          col.setType(aggr.getTypeInfo());
        }
      }

      return null;
    }

  }

  /**
   *
   * DefaultProcessor implicitly forwards the condition column if exists,
   * i.e., we do not need to make new ExprNodeDesc's (they forward their parent's columns).
   * For Forward, FileSink.
   *
   */
  public static class DefaultProcessor extends BaseProcessor {

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

      RowResolver rowResolver = ctx.getOpParseContext(op).getRowResolver();
      RowResolver parentRR = ctx.getOpParseContext(parent).getRowResolver();
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
  public static class ReduceSinkProcessor extends BaseProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      ReduceSinkOperator rs = (ReduceSinkOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      // Explicitly forward the condition column
      // Note: we still maintain the row resolver
      Map<String, ExprNodeDesc> colExprMap = rs.getColumnExprMap();
      ReduceSinkDesc desc = rs.getConf();
      ArrayList<String> outputValColNames = desc.getOutputValueColumnNames();
      ArrayList<ExprNodeDesc> valCols = desc.getValueCols();
      RowResolver rowResolver = ctx.getOpParseContext(rs).getRowResolver();

      Operator<? extends OperatorDesc> parent = rs.getParentOperators().get(0);
      for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
        valCols.add(cond);
        String valOutputName = Utils.getColumnInternalName(valCols.size() - 1);
        outputValColNames.add(valOutputName);
        String valName = Utilities.ReduceField.VALUE.toString() + "." + valOutputName;
        colExprMap.put(valName, cond);
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
  public static class SelectProcessor extends BaseProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      SelectOperator sel = (SelectOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      // Explicitly forward the condition column
      // Note: we still maintain the row resolvers
      Map<String, ExprNodeDesc> colExprMap = sel.getColumnExprMap();
      SelectDesc desc = sel.getConf();
      List<String> outputColNames = desc.getOutputColumnNames();
      List<ExprNodeDesc> cols = desc.getColList();
      // ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
      RowResolver rowResolver = ctx.getOpParseContext(sel).getRowResolver();

      Operator<? extends OperatorDesc> parent = sel.getParentOperators().get(0);
      for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
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
   * GroupByProcessor.
   *
   */
  public static class GroupByProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      GroupByOperator gby = (GroupByOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      if (!ctx.getLineageCtx().isSampled(gby)) {
        return null;
      }

      GroupByDesc desc = gby.getConf();
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();

      int numKeys = desc.getKeys().size();

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
        RowResolver rowResovler = ctx.getOpParseContext(gby).getRowResolver();
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
        }
//        else {
//          // Add itself into condition lineages only if it is the second group-by and a
//          // deduplication,
//          // as COUNT>0 is implicitly implied by the aggregates.
//          if (dedup) {
//            ctx.addCondition(gby, new AggregateInfo(gby, -1, deterministic));
//          }
//        }
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
   * FilterProcessor.
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
      ExprNodeDesc ret = rewrite(desc.getPredicate(), fil, ctx);
      if (ret != null) {
        desc.setPredicate(ret);
      }

      // Add a SEL after FIL to transform the condition column
      if (ctx.getTransform(fil) != null) {
        createAndConnectSelectOperator(fil, ctx, ctx.getTransform(fil).toArray(new ExprNodeDesc[0]));
      }

      return null;
    }

    private ExprNodeDesc rewrite(ExprNodeDesc expr, FilterOperator fil,
        RewriteProcCtx ctx) throws SemanticException {
      if (expr instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr;
        GenericUDF udf = func.getGenericUDF();

        if (udf instanceof GenericUDFOPAnd) {
          List<ExprNodeDesc> children = func.getChildExprs();
          for (int i = 0; i < children.size(); ++i) {
            ExprNodeDesc ret = rewrite(children.get(i), fil, ctx);
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
            ExprNodeDesc ret = rewrite(children.get(i), fil, ctx);
            if (ret != null) {
              changeCode = ((changeCode << 1) | 1);
              children.set(i, ret);
            }
          }

          // Convert comparison to special ABM comparison
          if (changeCode != 0) {
            normalizeParameters(children, changeCode);

            // Add to ctx: filter transforms the condition set of the annotation
            ctx.addTransform(fil, ExprNodeGenericFuncDesc.newInstance(
                getUdf(getCondUdfName(udf, changeCode)), children));

            // Rewrite the predicate.
            return ExprNodeGenericFuncDesc.newInstance(
                getUdf(getPredUdfName(udf, changeCode)),
                children);
          }
        }

        return null;
      }

      if (expr instanceof ExprNodeColumnDesc) {
        Operator<? extends OperatorDesc> parent = fil.getParentOperators().get(0);
        ExprNodeColumnDesc column = (ExprNodeColumnDesc) expr;
        AggregateInfo aggr = ctx.getLineage(parent, column.getColumn());
        if (aggr != null) {
          column.setTypeInfo(aggr.getTypeInfo());
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

  public static class JoinProcessor extends BaseProcessor {

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
      RowResolver rowResolver = ctx.getOpParseContext(join).getRowResolver();

      int countOfCondCols = 0;
      for (Operator<? extends OperatorDesc> parent : join.getParentOperators()) {
        ReduceSinkOperator rs = (ReduceSinkOperator) parent;
        RowResolver inputRR = ctx.getOpParseContext(rs).getRowResolver();
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

  public static ParseContext rewritePlan(TraceProcCtx tctx) throws SemanticException {
    RewriteProcCtx ctx = new RewriteProcCtx(tctx);

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
