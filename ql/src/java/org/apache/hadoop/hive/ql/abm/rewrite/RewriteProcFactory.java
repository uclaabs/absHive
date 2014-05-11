package org.apache.hadoop.hive.ql.abm.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.abm.lib.PostOrderPlanWalker;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

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

  private static final String SRV_SUM = "srv_sum";
  private static final String SRV_AVG = "srv_avg";
  private static final String SRV_COUNT = "srv_count";

  private static final String COND_JOIN = "cond_join";
  private static final String COND_MERGE = "cond_merge";

  private static final String GEN_ID = "gen_id";

  public static GenericUDF getUdf(String udfName) {
    // Remember to register the functions:
    // in org.apache.hadoop.hive.ql.exec.FunctionRegistry, use registerUDF
    FunctionInfo fi = FunctionRegistry.getFunctionInfo(udfName);
    assert fi != null;
    // getGenericUDF() actually clones the UDF. Just call it once and reuse.
    return fi.getGenericUDF();
  }

  public static void appendSelect(
      Operator<? extends OperatorDesc> op, RewriteProcCtx ctx, boolean afterGby,
      ExprNodeDesc... additionalConds) throws SemanticException {
    GenericUDF condJoin = getUdf(COND_JOIN);

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
        condColumn = ExprNodeGenericFuncDesc.newInstance(condJoin, list);
      }
    }
    for (ExprNodeDesc newCond : additionalConds) {
      if (condColumn == null) {
        condColumn = newCond;
      } else {
        List<ExprNodeDesc> list = new ArrayList<ExprNodeDesc>();
        list.add(condColumn);
        list.add(newCond);
        condColumn = ExprNodeGenericFuncDesc.newInstance(condJoin, list);
      }
    }

    List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
    List<String> colName = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    RowResolver rowResolver = new RowResolver();

    // Forward original columns
    HashSet<Integer> toSkip = ctx.getSpecialColumnIndexes(op);
    for (int i = 0; i < colInfos.size(); ++i) {
      if (toSkip.contains(i)) {
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

    ArrayList<Integer> keyCols = new ArrayList<Integer>();
    ArrayList<Integer> valCols = new ArrayList<Integer>();
    Integer idColIndex = null;

    if (afterGby) {
      GroupByOperator gby = (GroupByOperator) op;
      GroupByDesc gbyDesc = gby.getConf();

      int index = 0;
      ArrayList<ExprNodeDesc> keys = gbyDesc.getKeys();
      for (; index < keys.size(); ++index) {
        keyCols.add(index);
      }

      ArrayList<AggregationDesc> aggrs = gbyDesc.getAggregators();
      List<Integer> condIndexes = ctx.getCondColumnIndexes(gby);
      for (int i = 0; i < aggrs.size(); ++i, ++index) {
        if (condIndexes == null || !condIndexes.contains(index)) {
          valCols.add(index);
        }
      }
    }

    // Add a group-by id column
    if (afterGby) {
      GroupByOperator gby = (GroupByOperator) op;

      ExprNodeGenericFuncDesc idColumn =
          ExprNodeGenericFuncDesc.newInstance(getUdf(GEN_ID), new ArrayList<ExprNodeDesc>());
      columns.add(idColumn);
      idColIndex = columns.size() - 1;
      String idColName = Utils.getColumnInternalName(idColIndex);
      colName.add(idColName);
      colExprMap.put(idColName, idColumn);
      rowResolver.put("", idColName, new ColumnInfo(idColName, idColumn.getTypeInfo(), "", false));

      ctx.addGbyIdColumnIndex(op, gby, idColIndex);
    }
    // Add the original group-by id columns
    HashMap<GroupByOperator, Integer> idColMap = new HashMap<GroupByOperator, Integer>();
    Map<GroupByOperator, Integer> oldIdColMap = ctx.getGbyIdColumnIndexes(op);
    if (oldIdColMap != null) {
      for (Map.Entry<GroupByOperator, Integer> entry : oldIdColMap.entrySet()) {
        if (op.equals(ConditionAnnotation.lastUsedBy(entry.getKey()))) {
          continue;
        }

        ExprNodeColumnDesc idColumn = Utils.generateColumnDescs(op, entry.getValue()).get(0);
        columns.add(idColumn);
        int idIdx = columns.size() - 1;
        String idColName = Utils.getColumnInternalName(idIdx);
        colName.add(idColName);
        colExprMap.put(idColName, idColumn);
        rowResolver
            .put("", idColName, new ColumnInfo(idColName, idColumn.getTypeInfo(), "", false));

        idColMap.put(entry.getKey(), idIdx);
      }
    }

    // Add the condition column
    columns.add(condColumn);
    int condColIndex = columns.size() - 1;
    String condName = Utils.getColumnInternalName(condColIndex);
    colName.add(condName);
    colExprMap.put(condName, condColumn);
    rowResolver.put("", condName, new ColumnInfo(condName, condColumn.getTypeInfo(), "", false));

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

    for (Map.Entry<GroupByOperator, Integer> entry : idColMap.entrySet()) {
      ctx.addGbyIdColumnIndex(op, entry.getKey(), entry.getValue());
    }
    ctx.addCondColumnIndex(sel, condColIndex);

    if (afterGby) {
      GroupByOperator gby = (GroupByOperator) op;

      ArrayList<Integer> colsToCache = new ArrayList<Integer>();
      colsToCache.addAll(keyCols);
      colsToCache.addAll(valCols);
      colsToCache.add(idColIndex);

      desc.cache(colsToCache);
      ctx.putGroupByResult(gby, new GroupByResult(keyCols, valCols, idColIndex));
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
      // Hack! We assume the columns generated by aggregates are w/o transformation.
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

      // Note: we assume a single parent
      Operator<? extends OperatorDesc> parent = op.getParentOperators().get(0);
      RowResolver rowResolver = ctx.getOpParseContext(op).getRowResolver();
      RowResolver parentRR = ctx.getOpParseContext(parent).getRowResolver();
      ArrayList<ColumnInfo> parentColInfos = parent.getSchema().getSignature();

      // Implicitly forward the id columns
      Map<GroupByOperator, Integer> idColMap = ctx.getGbyIdColumnIndexes(parent);
      if (idColMap != null) {
        for (Map.Entry<GroupByOperator, Integer> entry : idColMap.entrySet()) {
          if (parent.equals(ConditionAnnotation.lastUsedBy(entry.getKey()))) {
            continue;
          }

          ColumnInfo ci = parentColInfos.get(entry.getValue());
          String[] name = parentRR.reverseLookup(ci.getInternalName());
          rowResolver.put(name[0], name[1], ci);
          // Maintain the id column index
          ctx.addGbyIdColumnIndex(op, entry.getKey(), entry.getValue());
        }
      }

      // Implicitly forward the condition columns
      List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
      if (condIndexes != null) {
        for (Integer index : condIndexes) {
          ColumnInfo ci = parentColInfos.get(index);
          String[] name = parentRR.reverseLookup(ci.getInternalName());
          rowResolver.put(name[0], name[1], ci);
          // Maintain the condition column index
          ctx.addCondColumnIndex(op, index);
        }
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

      Map<String, ExprNodeDesc> colExprMap = rs.getColumnExprMap();
      ReduceSinkDesc desc = rs.getConf();
      ArrayList<String> outputValColNames = desc.getOutputValueColumnNames();
      ArrayList<ExprNodeDesc> valCols = desc.getValueCols();
      RowResolver rowResolver = ctx.getOpParseContext(rs).getRowResolver();

      Operator<? extends OperatorDesc> parent = rs.getParentOperators().get(0);

      // Explicitly forward the id columns
      Map<GroupByOperator, Integer> idColMap = ctx.getGbyIdColumnIndexes(parent);
      for (Map.Entry<GroupByOperator, Integer> entry : idColMap.entrySet()) {
        if (parent.equals(ConditionAnnotation.lastUsedBy(entry.getKey()))) {
          continue;
        }

        ExprNodeColumnDesc idCol = Utils.generateColumnDescs(parent, entry.getValue()).get(0);
        valCols.add(idCol);
        String valOutputName = Utils.getColumnInternalName(valCols.size() - 1);
        outputValColNames.add(valOutputName);
        String valName = Utilities.ReduceField.VALUE.toString() + "." + valOutputName;
        colExprMap.put(valName, idCol);
        rowResolver.put("", valName, new ColumnInfo(valName, idCol.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
        ctx.addGbyIdColumnIndex(rs, entry.getKey(), colInfos.size() - 1);
      }

      // Explicitly forward the condition columns
      for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
        valCols.add(cond);
        String valOutputName = Utils.getColumnInternalName(valCols.size() - 1);
        outputValColNames.add(valOutputName);
        String valName = Utilities.ReduceField.VALUE.toString() + "." + valOutputName;
        colExprMap.put(valName, cond);
        rowResolver.put("", valName, new ColumnInfo(valName, cond.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = rs.getSchema().getSignature();
        ctx.addCondColumnIndex(rs, colInfos.size() - 1);
      }

      return null;
    }

  }

  /**
   *
   * SelectProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class SelectProcessor extends DefaultProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      SelectOperator sel = (SelectOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;
      SelectDesc desc = sel.getConf();

      if (desc.isSelStarNoCompute()) {
        return super.process(nd, stack, procCtx, nodeOutputs);
      }

      Map<String, ExprNodeDesc> colExprMap = sel.getColumnExprMap();
      List<String> outputColNames = desc.getOutputColumnNames();
      List<ExprNodeDesc> cols = desc.getColList();
      RowResolver rowResolver = ctx.getOpParseContext(sel).getRowResolver();

      Operator<? extends OperatorDesc> parent = sel.getParentOperators().get(0);

      // Explicitly forward the id columns
      Map<GroupByOperator, Integer> idColMap = ctx.getGbyIdColumnIndexes(parent);
      for (Map.Entry<GroupByOperator, Integer> entry : idColMap.entrySet()) {
        if (parent.equals(ConditionAnnotation.lastUsedBy(entry.getKey()))) {
          continue;
        }

        ExprNodeColumnDesc idCol = Utils.generateColumnDescs(parent, entry.getValue()).get(0);
        cols.add(idCol);
        String outputName = Utils.getColumnInternalName(cols.size() - 1);
        outputColNames.add(outputName);
        colExprMap.put(outputName, idCol);
        rowResolver
            .put("", outputName, new ColumnInfo(outputName, idCol.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
        ctx.addGbyIdColumnIndex(sel, entry.getKey(), colInfos.size() - 1);
      }

      // Explicitly forward the condition columns
      for (ExprNodeColumnDesc cond : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
        cols.add(cond);
        String outputName = Utils.getColumnInternalName(cols.size() - 1);
        outputColNames.add(outputName);
        colExprMap.put(outputName, cond);
        rowResolver
            .put("", outputName, new ColumnInfo(outputName, cond.getTypeInfo(), null, false));

        ArrayList<ColumnInfo> colInfos = sel.getSchema().getSignature();
        ctx.addCondColumnIndex(sel, colInfos.size() - 1);
      }

      return null;
    }

  }

  /**
   *
   * GroupByProcessor:
   * 1. rewrites the aggregates
   * (a) to use the corresponding ABM version and
   * (b) to return the correct type;
   *
   */
  public static class GroupByProcessor implements NodeProcessor {

    class AggrColInfo {
      public AggregationDesc aggregationDesc;
      public TypeInfo returnType;

      public AggrColInfo(AggregationDesc aggrDesc, TypeInfo type) {
        aggregationDesc = aggrDesc;
        returnType = type;
      }
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      GroupByOperator gby = (GroupByOperator) nd;
      RewriteProcCtx ctx = (RewriteProcCtx) procCtx;

      if (!ctx.isSampled(gby)) {
        return null;
      }

      // Insert Select as input and cache it
      insertSelect(gby, ctx);

      // Rewrite AggregationDesc to:
      // (1) Use the ABM version of SUM/COUNT/AVERAGE
      // (2) Return the correct type
      rewriteAggrs(gby, ctx);

      // Add a column to compute condition
      addCondColumn(gby, ctx);

      // Add select to generate group id
      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);
      boolean firstGby = !(parent instanceof ReduceSinkOperator);
      if (!firstGby) {
        appendSelect(gby, ctx, true);
      }

      return null;
    }

    // Insert Select as input and cache it
    private void insertSelect(GroupByOperator gby, RewriteProcCtx ctx) {
      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);
      boolean firstGby = !(parent instanceof ReduceSinkOperator);

      if (!firstGby) {
        return;
      }

      GroupByDesc gbyDesc = gby.getConf();

      List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
      List<String> colName = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      RowResolver rowResolver = new RowResolver();

      // Forward all columns
      for (ExprNodeDesc key : gbyDesc.getKeys()) {
        columns.add(key);
        String internalName = Utils.getColumnInternalName(columns.size() - 1);
        colName.add(internalName);
        colExprMap.put(internalName, key);
        rowResolver.put("", internalName,
            new ColumnInfo(internalName, key.getWritableObjectInspector(), "", false));
      }

      int idx = gbyDesc.getKeys().size();
      HashSet<Integer> toSkip = ctx.getSpecialColumnIndexes(gby);
      for (AggregationDesc aggr : gbyDesc.getAggregators()) {
        if (!toSkip.contains(idx)) {
          for (ExprNodeDesc param : aggr.getParameters()) {
            columns.add(param);
            String internalName = Utils.getColumnInternalName(columns.size() - 1);
            colName.add(internalName);
            colExprMap.put(internalName, param);
            rowResolver.put("", internalName,
                new ColumnInfo(internalName, param.getWritableObjectInspector(), "", false));
          }
        }
        ++idx;
      }

      // Add the condition columns if exist
      ArrayList<Integer> condIndexes = new ArrayList<Integer>();
      for (ExprNodeColumnDesc condColumn : Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent))) {
        columns.add(condColumn);
        String condName = Utils.getColumnInternalName(columns.size() - 1);
        colName.add(condName);
        colExprMap.put(condName, condColumn);
        rowResolver.put("", condName,
            new ColumnInfo(condName, condColumn.getTypeInfo(), "", false));

        condIndexes.add(colName.size() - 1);
      }

      // Create SEL
      SelectDesc desc = new SelectDesc(columns, colName);
      @SuppressWarnings("unchecked")
      Operator<SelectDesc> sel = OperatorFactory.get((Class<SelectDesc>) desc.getClass());
      sel.setConf(desc);
      sel.setSchema(rowResolver.getRowSchema());
      sel.setColumnExprMap(colExprMap);

      // Change group-by to use columns from this select
      ArrayList<String> gbyColNames = gbyDesc.getOutputColumnNames();
      Map<String, ExprNodeDesc> gbyColExprMap = gby.getColumnExprMap();

      ArrayList<Integer> keyCols = new ArrayList<Integer>();
      ArrayList<Integer> valCols = new ArrayList<Integer>();

      int index = 0;
      ArrayList<ExprNodeDesc> keys = gbyDesc.getKeys();
      for (; index < keys.size(); ++index) {
        ExprNodeDesc newKey = Utils.generateColumnDescs(sel, index).get(0);
        keys.set(index, newKey);
        gbyColExprMap.put(gbyColNames.get(index), newKey);

        keyCols.add(index);
      }

      ArrayList<AggregationDesc> aggrs = gbyDesc.getAggregators();
      for (int i = 0; i < aggrs.size(); ++i) {
        AggregationDesc aggr = aggrs.get(i);
        ArrayList<ExprNodeDesc> params = aggr.getParameters();
        for (int j = 0; j < params.size(); ++j, ++index) {
          ExprNodeDesc newParam = Utils.generateColumnDescs(sel, index).get(0);
          params.set(j, newParam);

          valCols.add(index);
        }
      }

      // Change the connection
      List<Operator<? extends OperatorDesc>> parents =
          new ArrayList<Operator<? extends OperatorDesc>>(gby.getParentOperators());
      sel.setParentOperators(parents);
      List<Operator<? extends OperatorDesc>> children =
          new ArrayList<Operator<? extends OperatorDesc>>();
      children.add(gby);
      sel.setChildOperators(children);

      for (Operator<? extends OperatorDesc> par : gby.getParentOperators()) {
        List<Operator<? extends OperatorDesc>> newChildren = par.getChildOperators();
        newChildren.remove(gby);
        newChildren.add(sel);
        par.setChildOperators(newChildren);
      }
      List<Operator<? extends OperatorDesc>> newParents =
          new ArrayList<Operator<? extends OperatorDesc>>();
      newParents.add(sel);
      gby.setChildOperators(newParents);

      // Put SEL into ParseContext
      OpParseContext opParseContext = new OpParseContext(rowResolver);
      ctx.getParseContext().getOpParseCtx().put(sel, opParseContext);

      for (int condIndex : condIndexes) {
        ctx.addCondColumnIndex(sel, condIndex);
      }

      // Cache it!
      ArrayList<Integer> colsToCache = new ArrayList<Integer>();
      colsToCache.addAll(keyCols);
      colsToCache.addAll(valCols);
      desc.cache(colsToCache);
      GroupByOperator gby2 = (GroupByOperator) gby.getChildOperators().get(0).getChildOperators()
          .get(0);
      ctx.putGroupByLineage(gby2, new GroupByLineage(keyCols, valCols));
    }

    private AggrColInfo createAggregate(String udafName,
        ArrayList<ExprNodeDesc> params, boolean distinct, boolean allCols,
        GenericUDAFEvaluator.Mode emode) throws SemanticException {
      GenericUDAFEvaluator udafEvaluator =
          SemanticAnalyzer.getGenericUDAFEvaluator(udafName,
              params, null, distinct, allCols);
      assert (udafEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          udafEvaluator, emode, params);
      AggregationDesc aggrDesc = new AggregationDesc(udafName,
          udaf.genericUDAFEvaluator, udaf.convertedParameters, distinct, emode);
      return new AggrColInfo(aggrDesc, udaf.returnType);
    }

    private String convertUdafName(String udaf) {
      if (udaf.equalsIgnoreCase("sum")) {
        return SRV_SUM;
      } else if (udaf.equalsIgnoreCase("avg")) {
        return SRV_AVG;
      } else {
        assert udaf.equalsIgnoreCase("count");
        return SRV_COUNT;
      }
    }

    // Rewrite AggregationDesc to:
    // (1) Use the ABM version of SUM/COUNT/AVERAGE
    // (2) Return the correct type
    private void rewriteAggrs(GroupByOperator gby, RewriteProcCtx ctx) throws SemanticException {
      GroupByDesc desc = gby.getConf();
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();
      ArrayList<ColumnInfo> allCols = gby.getSchema().getSignature();
      int numKeys = desc.getKeys().size();

      for (int i = 0; i < aggrs.size(); ++i) {
        // (1)
        AggregationDesc aggr = aggrs.get(i);
        String udafName = convertUdafName(aggr.getGenericUDAFName());
        boolean isAllCols = aggr.getGenericUDAFName().equalsIgnoreCase("count") ? true : false;
        AggrColInfo aggrInfo = createAggregate(udafName, aggr.getParameters(),
            aggr.getDistinct(), isAllCols, aggr.getMode());
        aggrs.set(i, aggrInfo.aggregationDesc);
        // (2): We only need to change the ColumnInfo in the schema
        // as other places (e.g., RowResolver) reference to the same ColumnInfo
        allCols.get(numKeys + i).setType(aggrInfo.returnType);
      }
    }

    // Add a column to compute condition
    private void addCondColumn(GroupByOperator gby, RewriteProcCtx ctx) throws SemanticException {
      Operator<? extends OperatorDesc> parent = gby.getParentOperators().get(0);

      GroupByDesc desc = gby.getConf();
      ArrayList<AggregationDesc> aggrs = desc.getAggregators();
      int numKeys = desc.getKeys().size();
      RowResolver rowResovler = ctx.getOpParseContext(gby).getRowResolver();
      ArrayList<String> outputColNames = desc.getOutputColumnNames();

      List<ExprNodeColumnDesc> condCols = Utils.generateColumnDescs(parent,
          ctx.getCondColumnIndexes(parent));
      assert condCols.size() < 2;
      GroupByDesc.Mode amode = desc.getMode();
      GenericUDAFEvaluator.Mode emode = SemanticAnalyzer.groupByDescModeToUDAFMode(
          amode, false);
      ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>(condCols);
      AggrColInfo aggrInfo = createAggregate(COND_MERGE, params, false, false, emode);
      AggregationDesc aggrDesc = aggrInfo.aggregationDesc;

      // colExprMap only has keys in it, so don't add this aggregation
      aggrs.add(aggrDesc);
      String colName = Utils.getColumnInternalName(numKeys + aggrs.size() - 1);
      rowResovler.put("", colName, new ColumnInfo(colName, aggrInfo.returnType, "", false));
      outputColNames.add(colName);

      ctx.addCondColumnIndex(gby, outputColNames.size() - 1);
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
        appendSelect(fil, ctx, false,
            ctx.getTransform(fil).toArray(new ExprNodeDesc[0]));
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
          int idx = ctx.getGbyIdColumnIndex(fil, aggr.getGroupByOperator());
          return Utils.generateColumnDescs(fil, idx).get(0);
        }
        return null;
      }

      return null;
    }

    // Normalize the parameter list to move Srv to the left hand side
    private void normalizeParameters(List<ExprNodeDesc> children, int code) {
      // code 1: left is not a Srv, right is a Srv
      // code 2: left is a Srv, right is not a Srv
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
      // code 2: left is a Srv, right is not a Srv
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

        // Forward the id columns
        Map<GroupByOperator, Integer> idColMap = ctx.getGbyIdColumnIndexes(parent);
        for (Map.Entry<GroupByOperator, Integer> entry : idColMap.entrySet()) {
          if (parent.equals(ConditionAnnotation.lastUsedBy(entry.getKey()))) {
            continue;
          }

          ExprNodeColumnDesc idCol = Utils.generateColumnDescs(parent, entry.getValue()).get(0);
          String colName = Utils.getColumnInternalName(outputColumnNames.size());
          outputColumnNames.add(colName);
          reversedExprs.put(colName, tag);
          List<ExprNodeDesc> exprs = exprMap.get(tag);
          exprs.add(idCol);
          colExprMap.put(colName, idCol);

          String[] names = inputRR.reverseLookup(idCol.getColumn());
          ColumnInfo ci = inputRR.get(names[0], names[1]);
          rowResolver.put(names[0], names[1],
              new ColumnInfo(colName, ci.getType(), names[0],
                  ci.getIsVirtualCol(), ci.isHiddenVirtualCol()));

          ctx.addGbyIdColumnIndex(join, entry.getKey(), outputColumnNames.size() - 1);
        }

        // Forward the condition columns
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

          ctx.addCondColumnIndex(join, outputColumnNames.size() - 1);
          ++countOfCondCols;
        }
      }

      if (countOfCondCols > 1) {
        appendSelect(join, ctx, false);
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
    GraphWalker walker = new PostOrderPlanWalker(disp);

    // Start walking from the top ops
    ArrayList<Node> topNodes = new ArrayList<Node>(ctx.getParseContext().getTopOps().values());
    walker.startWalking(topNodes, null);

    LineageIOProcFactory.setUpLineageIO(ctx);

    return ctx.getParseContext();
  }

}
