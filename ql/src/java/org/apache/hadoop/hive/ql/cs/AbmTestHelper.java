package org.apache.hadoop.hive.ql.cs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.funcdep.FuncDepProcFactory;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.abm.lineage.LineageProcFactory;
import org.apache.hadoop.hive.ql.abm.rewrite.RewriteProcFactory;
import org.apache.hadoop.hive.ql.abm.rewrite.TraceProcFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Test class for ABM
 *
 * AbmTestHelper.
 *
 */
public class AbmTestHelper {

  static LineageCtx ctx;
  static ParseContext pCtx;
  static boolean printExprMap = true;

  /**
   * a small tree with minimal info
   * @param sinkOp
   * @param level
   */
  private static void printTree(Operator<? extends OperatorDesc> sinkOp, int level) {
    if (sinkOp instanceof TableScanOperator) {
      String name = pCtx.getTopToTable().get(sinkOp).getTableName();
      println(level, sinkOp + " " +  name.toLowerCase().equals(AbmUtilities.getSampledTable()));
    } else {
      println(level, sinkOp);
    }

    List<Operator<? extends OperatorDesc>> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        printTree(l, level + 1);
      }
    }
  }

  private static void visit(Operator<? extends OperatorDesc> sinkOp, int level) {
    println(level, sinkOp.toString());
    println(level, "Schema: ");
    RowSchema schema = sinkOp.getSchema();
    if (schema != null) {
      if (schema.getSignature() != null) {
        for (ColumnInfo info: schema.getSignature()) {
          println(level, info.getInternalName());
        }
      }
    }

    if (printExprMap) {
      println(level, "Column Expr Map: ");
      Map<String, ExprNodeDesc> map = sinkOp.getColumnExprMap();
      if (map != null && map.entrySet() != null) {
        for (Entry<String, ExprNodeDesc> entry: map.entrySet()) {
          if (entry.getValue() instanceof ExprNodeColumnDesc) {
            //ExprNodeColumnDesc expr = (ExprNodeColumnDesc) entry.getValue();
            //String[] names = expr.getColumn().split("\\.");

            println(level, entry.getKey() + ": "
                + ((ExprNodeColumnDesc)entry.getValue()).getTabAlias()
                + ((ExprNodeColumnDesc)entry.getValue()).getCols());
          } else if (entry.getValue() instanceof ExprNodeConstantDesc) {
            println(level, entry.getKey() + ":: "
                + ((ExprNodeConstantDesc)entry.getValue()).getExprString());
          } else {
            println(level, entry.getKey() + ":"
                + entry.getValue().toString());
          }
        }
      }
    }

    if (sinkOp instanceof TableScanOperator) {
      String name = pCtx.getTopToTable().get(sinkOp).getTableName();
      println(level, "[TableScan] TabName: " + name + " isSampleTable: " + name.toLowerCase().equals(AbmUtilities.getSampledTable().toLowerCase()));
    }
    else if (sinkOp instanceof GroupByOperator) {
      println(level, "[GroupBy] Aggregators:");
      ArrayList<AggregationDesc> aggrs =
          ((GroupByOperator)sinkOp).getConf().getAggregators();
      if (aggrs != null) {
        for (AggregationDesc desc : aggrs) {
          println(level, "isDistinct: " + " " + desc.getDistinct() + " ExprString: " + desc.getExprString());
        }
      }
    }
    else if (sinkOp instanceof FilterOperator) {
      println(level, "[Filter] Predicate:");
      println(level, ((FilterOperator)sinkOp).getConf().getPredicate().getExprString());
    }
    else if (sinkOp instanceof JoinOperator) {
      println(level, "[Join]");
    }
    else if (sinkOp instanceof SelectOperator) {
      println(level, "[Select ExprNodeDesc]");
      for (ExprNodeDesc exprNodeDesc: ((SelectOperator) sinkOp).getConf().getColList()) {
          println(level, exprNodeDesc.getExprString());
      }
    }


    //print lineage info
    //println(level, ctx.get(sinkOp));
    println();

    List<Operator<? extends OperatorDesc>> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        visit(l, level + 1);
      }
    }
  }

  /**
   * Main Test Entry Point
   * @param sinkOp
   * @param pCtx
   */
  public static void test(Operator<? extends OperatorDesc> sinkOp, ParseContext pCtx) {
    try {
      if (pCtx.getFetchTask() == null) {
        println(0, "####### Before Rewrite #########");
        analyzeHelper(sinkOp, 0);
      } else {
        println(0, "FileSink Operator has been changed into ListSinkOperator!");
      }

      //opParseCtx = pCtx.getOpParseCtx();
      AbmTestHelper.pCtx = pCtx;
      try {
        ctx = LineageProcFactory.extractLineage(pCtx);
        FuncDepProcFactory.checkFuncDep(ctx);
      }
      catch (Exception e) {
        logExceptions("exceptions.txt", e.getMessage());
        e.printStackTrace();
      }

      try {
        RewriteProcFactory.rewritePlan(TraceProcFactory.trace(ctx));
      }
      catch (Exception e) {
        logExceptions("exceptions.txt", e.getMessage() + " " + Arrays.asList(e.getStackTrace()).toString());
        e.printStackTrace();
      }

      if (pCtx.getFetchTask() == null) {
        println(0, "####### After Rewrite #########");
        visit(sinkOp, 0);
      } else {
        println(0, "FileSink Operator has been changed into ListSinkOperator!");
        //println(0, ctx);
      }

      println(0, "####### Tree #########");
      printTree(sinkOp, 0);

    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void analyzeHelper(Operator<? extends OperatorDesc> sinkOp, int level) {

    println(level, sinkOp.getClass() + " " + sinkOp.toString());
    println(level, "Column Expr Map: ");

    Map<String, ExprNodeDesc> map = sinkOp.getColumnExprMap();
    if (map != null && map.entrySet() != null) {
      for (Entry<String, ExprNodeDesc> entry: map.entrySet()) {
        if (entry.getValue() instanceof ExprNodeColumnDesc) {
          //ExprNodeColumnDesc expr = (ExprNodeColumnDesc) entry.getValue();
          //String[] names = expr.getColumn().split("\\.");
          //println(level, "@@@"+ expr.getColumn()+ "@@@" + Arrays.asList(names) +"@@@@");

          println(level, entry.getKey() + ": "
              + ((ExprNodeColumnDesc)entry.getValue()).getTabAlias()
              + ((ExprNodeColumnDesc)entry.getValue()).getCols());
        } else if (entry.getValue() instanceof ExprNodeConstantDesc) {
          println(level, entry.getKey() + ":: "
              + ((ExprNodeConstantDesc)entry.getValue()).getExprString());
          //+ ((ExprNodeConstantDesc)entry.getValue()).getCols());
        } else {
          println(level, entry.getValue().getExprString());
          //throw new RuntimeException("ExprNode Type does not supported!");
        }
      }
    }

    println(level, "Schema: ");
    RowSchema schema = sinkOp.getSchema();
    if (schema != null) {
      if (schema.getSignature() != null) {
        for (ColumnInfo info: schema.getSignature()) {
          println(level, info.getTabAlias() + "[" + info.getInternalName() + "]");
        }
      }
    }

    if (sinkOp instanceof TableScanOperator) {
      //System.out.println("=========== " + opParseCtx.get(sinkOp).getRowResolver().tableOriginalName);

      //System.out.println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumnIDs());
      //System.out.println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumns());
      //System.out.println("======Table Desc " + ((TableScanOperator)(sinkOp)).getTableDesc());
      //System.out.println(qb.getTabNameForAlias("a"));
      //System.out.println(qb.getTabNameForAlias("b"));

      //TableScanDesc desc = ((TableScanOperator)sinkOp).getConf();
      //println(level, desc.getAlias());

      //println(level, desc.getFilterExpr());
      //println(level, desc.getBucketFileNameMapping());
      //println(level, desc.getVirtualCols());
      //println(level, desc.getPartColumns());
    }


    if (sinkOp instanceof JoinOperator) {

      JoinDesc desc = ((JoinOperator)sinkOp).getConf();

      List<String> outputNames = desc.getOutputColumnNames();
      println(level, "outputColumnNames size: " + outputNames.size());
      println(level, "Exprs: " + desc.getExprs());
      println(level, "Exprs String Map: " + desc.getExprsStringMap());
      for (String name: outputNames) {
        println(level, name);
      }

      //println(level, ((JoinOperator) sinkOp).getPosToAliasMap());
      //println(level, "Reversed Mapping: " + ((JoinOperator)sinkOp).getConf().getReversedExprs());

      /*
      println(level, "JOIN");

      for (List<ExprNodeDesc> lst : ((JoinOperator)sinkOp).getConf().getExprs().values()) {
        printLevel(level);
        for (ExprNodeDesc desc: lst) {
          print(((ExprNodeColumnDesc)desc).getTabAlias() + " " + ((ExprNodeColumnDesc)desc).getCols());
        }
        println();
      }

      //for filters
      Map<Byte, List<ExprNodeDesc>> filterMap = ((JoinOperator)sinkOp).getConf().getFilters();

      //if (filterMap != null && filterMap.size() != 0) {
      //print("HAS JOIN FILTERS");
      //}

      for (List<ExprNodeDesc> lst : ((JoinOperator)sinkOp).getConf().getFilters().values()) {
        printLevel(level);
        //print(((JoinOperator)sinkOp).getConf().getFilters());
        for (ExprNodeDesc desc: lst) {
          print(desc.getClass() + " ");
          if (desc instanceof ExprNodeGenericFuncDesc) {
            print(((ExprNodeGenericFuncDesc)desc));
          }
        }
        println();
      }

       */

      //println(level, "output");

      //println(level, ((JoinOperator)sinkOp).getConf().getOutputColumnNames());


      //println(level, ((JoinOperator)sinkOp).getConf().getExprsStringMap());
    }

    if (sinkOp instanceof ReduceSinkOperator) {
      //println(level, ((ReduceSinkOperator)sinkOp).getConf().getOutputKeyColumnNames());
      /*
      for (ExprNodeDesc desc: ((ReduceSinkOperator)sinkOp).getConf().getValueCols()) {
        println(level, ((ExprNodeColumnDesc)desc).getTabAlias() + " "
                + ((ExprNodeColumnDesc)desc).getCols());
      }
       */
      //ReduceSinkOperator op = (ReduceSinkOperator) sinkOp;
      //System.out.println("@@@@@@@@@@@@@" + op.toString() +  " " + op.getConf().getKeyCols().size() + " " + op.getConf().getValueCols().size());
      //System.out.println("@@@@@@@@@@@" + op.getConf().getNumDistributionKeys() + " " + op.getConf().getKeyCols().toString());
      //System.out.println("@@@@@@@@@@@" + op.getConf().getValueCols().toString());

    }

    if (sinkOp instanceof SelectOperator) {
      println(level, "Select " + ((SelectOperator)sinkOp).getConf().getColList());
      //println(level, "Select" + ((SelectOperator)sinkOp).getConf().getOutputColumnNames());
      /*
      for (ExprNodeDesc desc: ((SelectOperator)sinkOp).getConf().getColList()) {
        println(level, ((ExprNodeColumnDesc)desc).getTabAlias() + " "
                + ((ExprNodeColumnDesc)desc).getCols());
      }*/
      //println(level, ((SelectOperator)sinkOp).getConf().getColList());
      //println(level, ((SelectOperator)sinkOp).getConf().getOutputColumnNames());
    }

    if (sinkOp instanceof FilterOperator) {
      println(level, ((FilterOperator)sinkOp).getConf().getPredicate().getExprString());
      //ExprNodeDesc desc = ((FilterOperator)sinkOp).getConf().getPredicate();
      //(ExprNodeGenericFuncDesc)((FilterOperator)sinkOp).getConf().getPredicate()
      //println(level, ((ExprNodeGenericFuncDesc)desc).getExprString());
      //println(level, ((ExprNodeGenericFuncDesc)desc).getCols());
    }

    if (sinkOp instanceof LimitOperator) {
      println(level, ((LimitOperator)sinkOp).getConf().getClass());
      //ExprNodeDesc desc = ((FilterOperator)sinkOp).getConf().getPredicate();
      //(ExprNodeGenericFuncDesc)((FilterOperator)sinkOp).getConf().getPredicate()
      //println(level, ((ExprNodeGenericFuncDesc)desc).getExprString());
      //println(level, ((ExprNodeGenericFuncDesc)desc).getCols());
    }

    if (sinkOp instanceof GroupByOperator) {
      println(level, "Aggregators: ");
      ArrayList<AggregationDesc> aggrs =
          ((GroupByOperator)sinkOp).getConf().getAggregators();
      if (aggrs != null) {
        for (AggregationDesc desc : aggrs) {
          println(level, "isDistinct: " + " " + desc.getDistinct() + " ExprString: " + desc.getExprString());
        }
      }
    }

    List<Operator<? extends OperatorDesc>> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator<? extends OperatorDesc> l: lst) {
        analyzeHelper(l, level + 1);
      }
    }

  }

  public static void println(int level, Object content) {
    for (int i=0; i< level; i++) {
      System.out.print("  ");
    }
    System.out.println(content);
  }

  public static void println() {
    System.out.println();
  }

  public static void printLevel(int level) {
    for (int i=0; i< level; i++) {
      System.out.print("  ");
    }
  }

  public static void print(Object content) {
    System.out.print(content + " ");
  }

  private static void logExceptions(String path, String msg) throws SemanticException {
    try {
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(path, true)));
      out.println("q" + AbmUtilities.getLabel() + ":");
      out.println(msg);
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}