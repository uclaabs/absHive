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

  public static class SelectFactory {

    private final RewriteProcCtx ctx;

    private final List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    private final List<String> outputColumnNames = new ArrayList<String>();
    private final HashMap<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    private final RowResolver rowResolver = new RowResolver();

    private Integer tidIndex = null;
    private final ArrayList<Integer> condIndex = new ArrayList<Integer>();
    private final HashMap<GroupByOperator, Integer> gbyIdIndex = new HashMap<GroupByOperator, Integer>();

    public SelectFactory(RewriteProcCtx ctx) {
      this.ctx = ctx;
    }

    public int forwardColumn(Operator<? extends OperatorDesc> op, int index, boolean keepName) {
      return addColumn(Utils.generateColumnDescs(op, index).get(0),
          keepName ? op.getSchema().getSignature().get(index).getInternalName()
              : Utils.getColumnInternalName(colList.size()));
    }

    public int addColumn(ExprNodeDesc column) {
      return addColumn(column, Utils.getColumnInternalName(colList.size()));
    }

    private int addColumn(ExprNodeDesc column, String outputName) {
      colList.add(column);
      outputColumnNames.add(outputName);
      columnExprMap.put(outputName, column);
      rowResolver.put("", outputName, new ColumnInfo(outputName, column.getTypeInfo(), "", false));

      return outputColumnNames.size() - 1;
    }

    public void setTidIndex(int index) {
      tidIndex = index;
    }

    public void addCondIndex(int index) {
      condIndex.add(index);
    }

    public void addGbyIdIndex(GroupByOperator gby, int index) {
      gbyIdIndex.put(gby, index);
    }

    @SuppressWarnings("unchecked")
    public SelectOperator getSelectOperator() {
      // Create SEL
      SelectDesc desc = new SelectDesc(colList, outputColumnNames);
      SelectOperator sel = (SelectOperator) OperatorFactory
          .get((Class<SelectDesc>) desc.getClass());
      sel.setConf(desc);
      sel.setSchema(rowResolver.getRowSchema());
      sel.setColumnExprMap(columnExprMap);

      // Put SEL into ParseContext
      OpParseContext opParseContext = new OpParseContext(rowResolver);
      ctx.getParseContext().getOpParseCtx().put(sel, opParseContext);

      if (tidIndex != null) {
        ctx.putTidColumnIndex(sel, tidIndex);
      }
      for (int index : condIndex) {
        ctx.addCondColumnIndex(sel, index);
      }
      for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndex.entrySet()) {
        ctx.addGbyIdColumnIndex(sel, entry.getKey(), entry.getValue());
      }

      return sel;
    }

  }

  public static void appendSelect(
      Operator<? extends OperatorDesc> op, RewriteProcCtx ctx, boolean afterGby,
      ExprNodeDesc... additionalConds) throws SemanticException {
    SelectFactory selFactory = new SelectFactory(ctx);

    // Forward original columns
    ArrayList<ColumnInfo> signature = op.getSchema().getSignature();
    HashSet<Integer> toSkip = ctx.getSpecialColumnIndexes(op);
    for (int i = 0; i < signature.size(); ++i) {
      if (!toSkip.contains(i)) {
        selFactory.forwardColumn(op, i, true);
      }
    }

    // Forward the tid column
    if (ctx.withTid(op)) {
      assert !afterGby;
      selFactory.setTidIndex(selFactory.forwardColumn(op, ctx.getTidColumnIndex(op), false));
    }

    // Add the condition column
    List<Integer> indexes = ctx.getCondColumnIndexes(op);
    ExprNodeDesc condColumn = null;
    GenericUDF condJoin = getUdf(COND_JOIN);
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
    selFactory.addCondIndex(selFactory.addColumn(condColumn));

    // Add a group-by id column
    Integer idColIndex = null;
    if (afterGby) {
      GroupByOperator gby = (GroupByOperator) op;

      ExprNodeGenericFuncDesc idColumn =
          ExprNodeGenericFuncDesc.newInstance(getUdf(GEN_ID), new ArrayList<ExprNodeDesc>());
      idColIndex = selFactory.addColumn(idColumn);
      selFactory.addGbyIdIndex(gby, idColIndex);
    }
    // Forward the original group-by id columns
    Map<GroupByOperator, Integer> oldIdColMap = ctx.getGbyIdColumnIndexes(op);
    if (oldIdColMap != null) {
      for (Map.Entry<GroupByOperator, Integer> entry : oldIdColMap.entrySet()) {
        GroupByOperator gby = entry.getKey();
        if (op.equals(ConditionAnnotation.lastUsedBy(gby))) {
          continue;
        }
        int index = entry.getValue();
        selFactory.addGbyIdIndex(gby, selFactory.forwardColumn(op, index, false));
      }
    }

    // Create SEL
    SelectOperator sel = selFactory.getSelectOperator();

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

    if (afterGby) {
      ctx.putGroupByOutput((GroupByOperator) op, sel);
    }
  }

  public static abstract class RewriteProcessor implements NodeProcessor {

    protected RewriteProcCtx ctx = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      initialize(nd, procCtx);
      return null;
    }

    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      ctx = (RewriteProcCtx) procCtx;
    }

  }

  /**
   *
   * BaseProcessor propagates the correct types.
   *
   */
  public static abstract class BaseProcessor extends RewriteProcessor {

    protected Operator<? extends OperatorDesc> op = null;
    protected RowResolver rowResolver = null;
    protected ArrayList<ColumnInfo> signature = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      // Propagates the correct types
      // Hack! We assume the columns generated by aggregates are w/o transformation.
      for (ColumnInfo ci : signature) {
        AggregateInfo ai = ctx.getLineage(op, ci.getInternalName());
        if (ai != null) {
          ci.setType(ai.getTypeInfo());
        }
      }

      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      op = (Operator<? extends OperatorDesc>) nd;
      rowResolver = ctx.getOpParseContext(op).getRowResolver();
      signature = op.getSchema().getSignature();
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

    // Note: there should be only one parent
    protected Operator<? extends OperatorDesc> parent = null;
    protected RowResolver parentRR = null;
    protected ArrayList<ColumnInfo> parentSignature = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      // TODO: to fix
      if (op.getParentOperators() == null) {
        return null;
      }

      // Forward the tid column
      if (ctx.withTid(op)) {
        int index = ctx.getTidColumnIndex(parent);
        forwardColumn(index);
        // Maintain the tid column index
        ctx.putTidColumnIndex(op, signature.size() - 1);
      }

      // Forward the gbyId columns
      Map<GroupByOperator, Integer> gbyIdIndexes = ctx.getGbyIdColumnIndexes(parent);
      if (gbyIdIndexes != null) {
        for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndexes.entrySet()) {
          GroupByOperator gby = entry.getKey();
          if (parent.equals(ConditionAnnotation.lastUsedBy(gby))) {
            continue;
          }
          int index = entry.getValue();
          forwardColumn(index);
          // Maintain the id column index
          ctx.addGbyIdColumnIndex(op, gby, signature.size() - 1);
        }
      }

      // Forward the condition columns
      List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
      if (condIndexes != null) {
        for (int index : condIndexes) {
          forwardColumn(index);
          // Maintain the condition column index
          ctx.addCondColumnIndex(op, signature.size() - 1);
        }
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);

      parent = op.getParentOperators().get(0);
      parentRR = ctx.getOpParseContext(parent).getRowResolver();
      parentSignature = parent.getSchema().getSignature();
    }

    // Implicitly forward the column
    protected void forwardColumn(int index) {
      ColumnInfo ci = parentSignature.get(index);
      String[] name = parentRR.reverseLookup(ci.getInternalName());
      rowResolver.put(name[0], name[1], ci);
    }

  }

  /**
   *
   * ReduceSinkProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class ReduceSinkProcessor extends DefaultProcessor {

    private ReduceSinkOperator rs = null;
    private ReduceSinkDesc desc = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;
    private ArrayList<String> outputValueColumnNames = null;
    private ArrayList<ExprNodeDesc> valueCols = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      rs = (ReduceSinkOperator) nd;
      desc = rs.getConf();
      columnExprMap = rs.getColumnExprMap();
      outputValueColumnNames = desc.getOutputValueColumnNames();
      valueCols = desc.getValueCols();
    }

    // Explicitly forward the column
    @Override
    protected void forwardColumn(int index) {
      ExprNodeColumnDesc column = Utils.generateColumnDescs(parent, index).get(0);
      String columnInternalName = Utils.getColumnInternalName(valueCols.size());
      valueCols.add(column);
      outputValueColumnNames.add(columnInternalName);
      String valName = Utilities.ReduceField.VALUE.toString() + "." + columnInternalName;
      columnExprMap.put(valName, column);
      rowResolver.put("", valName, new ColumnInfo(valName, column.getTypeInfo(), "", false));
    }

  }

  /**
   *
   * SelectProcessor explicitly forwards the condition column if exists.
   *
   */
  public static class SelectProcessor extends DefaultProcessor {

    private SelectOperator sel = null;
    private SelectDesc desc = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;
    private List<String> outputColumnNames = null;
    private List<ExprNodeDesc> colList = null;

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      sel = (SelectOperator) nd;
      desc = sel.getConf();
      columnExprMap = sel.getColumnExprMap();
      outputColumnNames = desc.getOutputColumnNames();
      colList = desc.getColList();
    }

    // Explicitly or implicitly forward the column
    @Override
    protected void forwardColumn(int index) {
      if (desc.isSelStarNoCompute()) {
        super.forwardColumn(index);
        return;
      }

      ExprNodeColumnDesc column = Utils.generateColumnDescs(parent, index).get(0);
      colList.add(column);
      String outputName = Utils.getColumnInternalName(colList.size() - 1);
      outputColumnNames.add(outputName);
      columnExprMap.put(outputName, column);
      rowResolver
          .put("", outputName, new ColumnInfo(outputName, column.getTypeInfo(), "", false));
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
  public static class GroupByProcessor extends RewriteProcessor {

    private GroupByOperator gby = null;
    private GroupByDesc desc = null;
    private ArrayList<ColumnInfo> signature = null;
    private ArrayList<ExprNodeDesc> keys = null;
    private int numKeys = 0;
    private ArrayList<AggregationDesc> aggregators = null;
    private RowResolver rowResovler = null;
    private ArrayList<String> outputColumnNames = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;

    private Operator<? extends OperatorDesc> parent = null;
    private boolean firstGby = false;
    private GenericUDAFEvaluator.Mode evaluatorMode = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      if (!ctx.isSampled(gby)) {
        return null;
      }

      // Insert Select as input and cache it
      if (firstGby) {
        insertSelect();
      }

      // Rewrite AggregationDesc
      for (int i = 0; i < aggregators.size(); ++i) {
        modifyAggregator(i);
      }

      // Add the column to compute condition
      List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
      assert (condIndexes == null || condIndexes.size() < 2);
      addAggregator(COND_MERGE, parent, condIndexes);
      ctx.addCondColumnIndex(gby, signature.size() - 1);

      // TODO: Add the column to compute lineage

      // Add select to generate group id
      if (!firstGby) {
        appendSelect(gby, ctx, true);
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      gby = (GroupByOperator) nd;
      desc = gby.getConf();
      signature = gby.getSchema().getSignature();
      keys = desc.getKeys();
      numKeys = keys.size();
      aggregators = desc.getAggregators();
      rowResovler = ctx.getOpParseContext(gby).getRowResolver();
      outputColumnNames = desc.getOutputColumnNames();
      columnExprMap = gby.getColumnExprMap();

      parent = gby.getParentOperators().get(0);
      firstGby = !(parent instanceof ReduceSinkOperator);
      aggregators = desc.getAggregators();
      evaluatorMode = SemanticAnalyzer.groupByDescModeToUDAFMode(desc.getMode(), false);
    }

    // Insert Select as input and cache it
    private void insertSelect() {
      SelectFactory selFactory = new SelectFactory(ctx);

      // Forward all columns
      for (ExprNodeDesc key : keys) {
        selFactory.addColumn(key);
      }
      // Up to now there is no special column
      for (AggregationDesc aggregator : aggregators) {
        for (ExprNodeDesc parameter : aggregator.getParameters()) {
          selFactory.addColumn(parameter);
        }
      }

      // Forward the tid column
      assert ctx.withTid(parent);
      selFactory
          .setTidIndex(selFactory.forwardColumn(parent, ctx.getTidColumnIndex(parent), false));

      // Forward the condition columns if exist
      if (ctx.getCondColumnIndexes(parent) != null) {
        for (int index : ctx.getCondColumnIndexes(parent)) {
          selFactory.addCondIndex(selFactory.forwardColumn(parent, index, false));
        }
      }

      // Create SEL
      SelectOperator sel = selFactory.getSelectOperator();

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

      // Change group-by to use columns from this select
      int index = 0;
      for (; index < numKeys; ++index) {
        ExprNodeDesc newKey = Utils.generateColumnDescs(sel, index).get(0);
        keys.set(index, newKey);
        columnExprMap.put(outputColumnNames.get(index), newKey);
      }

      for (int i = 0; i < aggregators.size(); ++i) {
        ArrayList<ExprNodeDesc> params = aggregators.get(i).getParameters();
        for (int j = 0; j < params.size(); ++j, ++index) {
          ExprNodeDesc newParam = Utils.generateColumnDescs(sel, index).get(0);
          params.set(j, newParam);
        }
      }

      // Cache it!
      ctx.putGroupByInput(
          (GroupByOperator) gby.getChildOperators().get(0).getChildOperators().get(0), sel);
    }

    // Rewrite AggregationDesc to:
    // (1) Use the ABM version of SUM/COUNT/AVERAGE
    // (2) Return the correct type
    private void modifyAggregator(int index) throws SemanticException {
      // (1)
      AggregationDesc aggregator = aggregators.get(index);
      String oldUdafName = aggregator.getGenericUDAFName();
      String udafName = convertUdafName(oldUdafName);
      ArrayList<ExprNodeDesc> parameters = aggregator.getParameters();
      boolean distinct = aggregator.getDistinct();
      boolean isAllCols = oldUdafName.equalsIgnoreCase("count") ? true : false;
      GenericUDAFEvaluator udafEvaluator =
          SemanticAnalyzer.getGenericUDAFEvaluator(udafName,
              parameters, null, distinct, isAllCols);
      assert (udafEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          udafEvaluator, evaluatorMode, parameters);
      AggregationDesc aggregationDesc = new AggregationDesc(udafName,
          udaf.genericUDAFEvaluator, udaf.convertedParameters, distinct, evaluatorMode);

      aggregators.set(index, aggregationDesc);
      // (2): We only need to change the ColumnInfo in the schema
      // as other places (e.g., RowResolver) reference to the same ColumnInfo
      signature.get(numKeys + index).setType(udaf.returnType);
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

    private void addAggregator(String udafName, Operator<? extends OperatorDesc> op,
        List<Integer> indexes) throws SemanticException {
      ArrayList<ExprNodeDesc> parameters = new ArrayList<ExprNodeDesc>(
          Utils.generateColumnDescs(parent, indexes));
      GenericUDAFEvaluator udafEvaluator =
          SemanticAnalyzer.getGenericUDAFEvaluator(udafName,
              parameters, null, false, false);
      assert (udafEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
          udafEvaluator, evaluatorMode, parameters);
      AggregationDesc aggregationDesc = new AggregationDesc(udafName,
          udaf.genericUDAFEvaluator, udaf.convertedParameters, false, evaluatorMode);

      // colExprMap only has keys in it, so don't add this aggregation
      String columnInternalName = Utils.getColumnInternalName(numKeys + aggregators.size());
      aggregators.add(aggregationDesc);
      rowResovler.put("", columnInternalName, new ColumnInfo(columnInternalName, udaf.returnType,
          "", false));
      outputColumnNames.add(columnInternalName);
    }

  }

  /**
   *
   * FilterProcessor.
   *
   */
  public static class FilterProcessor extends DefaultProcessor {

    private FilterOperator fil = null;
    private FilterDesc desc = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

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

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      fil = (FilterOperator) nd;
      desc = fil.getConf();
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

    private JoinOperator join = null;
    private JoinDesc desc = null;
    private List<String> outputColumnNames = null;
    private Map<String, Byte> reversedExprs = null;
    private Map<Byte, List<ExprNodeDesc>> exprMap = null;
    private Map<String, ExprNodeDesc> columnExprMap = null;

    private ReduceSinkOperator parentRS = null;
    private RowResolver parentRR = null;
    private Byte tag = null;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);

      for (Operator<? extends OperatorDesc> parent : join.getParentOperators()) {
        parentRS = (ReduceSinkOperator) parent;
        parentRR = ctx.getOpParseContext(parentRS).getRowResolver();
        tag = (byte) parentRS.getConf().getTag();

        // Forward the tid column
        if (ctx.withTid(join)) {
          Integer index = ctx.getTidColumnIndex(parentRS);
          if (index != null) {
            forwardColumn(index);
            ctx.putTidColumnIndex(join, signature.size() - 1);
          }
        }

        // Forward the gbyId columns
        Map<GroupByOperator, Integer> gbyIdIndexes = ctx.getGbyIdColumnIndexes(parent);
        if (gbyIdIndexes != null) {
          for (Map.Entry<GroupByOperator, Integer> entry : gbyIdIndexes.entrySet()) {
            GroupByOperator gby = entry.getKey();
            if (parent.equals(ConditionAnnotation.lastUsedBy(gby))) {
              continue;
            }
            int index = entry.getValue();
            forwardColumn(index);
            ctx.addGbyIdColumnIndex(join, gby, signature.size() - 1);
          }
        }

        // Forward the condition columns
        List<Integer> condIndexes = ctx.getCondColumnIndexes(parent);
        if (condIndexes != null) {
          int countOfCondCols = 0;

          for (int index : condIndexes) {
            forwardColumn(index);
            // Maintain the condition column index
            ctx.addCondColumnIndex(op, signature.size() - 1);
            ++countOfCondCols;
          }

          if (countOfCondCols > 1) {
            appendSelect(join, ctx, false);
          }
        }
      }

      return null;
    }

    @Override
    protected void initialize(Node nd, NodeProcessorCtx procCtx) {
      super.initialize(nd, procCtx);
      join = (JoinOperator) nd;
      desc = join.getConf();
      outputColumnNames = desc.getOutputColumnNames();
      reversedExprs = desc.getReversedExprs();
      exprMap = desc.getExprs();
      columnExprMap = join.getColumnExprMap();
    }

    protected void forwardColumn(int index) throws SemanticException {
      ExprNodeColumnDesc column = Utils.generateColumnDescs(parentRS, index).get(0);
      String columnInternalName = Utils.getColumnInternalName(outputColumnNames.size());
      outputColumnNames.add(columnInternalName);
      reversedExprs.put(columnInternalName, tag);
      List<ExprNodeDesc> exprList = exprMap.get(tag);
      exprList.add(column);
      columnExprMap.put(columnInternalName, column);

      String[] names = parentRR.reverseLookup(column.getColumn());
      ColumnInfo ci = parentRR.get(names[0], names[1]);
      rowResolver.put(names[0], names[1],
          new ColumnInfo(columnInternalName, ci.getType(), names[0],
              ci.getIsVirtualCol(), ci.isHiddenVirtualCol()));
    }

  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultProcessor();
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

  public static NodeProcessor getFilterProc() {
    return new FilterProcessor();
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
