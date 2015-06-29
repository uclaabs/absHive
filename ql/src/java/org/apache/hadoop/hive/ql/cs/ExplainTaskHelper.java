/*
 * Copyright (C) 2015 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.cs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.abm.AbmUtilities;
import org.apache.hadoop.hive.ql.abm.funcdep.FuncDepProcFactory;
import org.apache.hadoop.hive.ql.abm.lineage.LineageCtx;
import org.apache.hadoop.hive.ql.abm.lineage.LineageProcFactory;
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
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

//victor

/**
 * This is a hacking class.
 * Another modification is Driver.java which needs to call compile when call execute
 *
 * @author victor
 */
public class ExplainTaskHelper {

  PrintStream out;
  ExplainWork work;
  static LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
  //
  static ParseContext pCtx;
  static LineageCtx ctx;

  public ExplainTaskHelper(PrintStream out, ExplainWork work) {
    this.out = out;
    this.work = work;
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

  private static void visit(Operator<? extends OperatorDesc> sinkOp, int level) {
    println(level, sinkOp.toString());
    println(level, "Schema: ");
    RowSchema schema = sinkOp.getSchema();
    if (schema != null) {
      if (schema.getSignature() != null) {
        for (ColumnInfo info: schema.getSignature()) {
          //println(level, info.getTabAlias() + "[" + info.getInternalName() + "]");
          //println(level, info.toString());
          println(level, info.getInternalName());
        }
      }
    }

    boolean printExprMap = true;
    if (printExprMap) {
      println(level, "Column Expr Map: ");
      Map<String, ExprNodeDesc> map = sinkOp.getColumnExprMap();
      if (map != null && map.entrySet() != null) {
        for (Entry<String, ExprNodeDesc> entry: map.entrySet()) {
          if (entry.getValue() instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc expr = (ExprNodeColumnDesc) entry.getValue();
            String[] names = expr.getColumn().split("\\.");

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
      //println(level, "TabName:" + pCtx.getTopToTable().get(sinkOp).getTableName() + " Alias:" + ((TableScanOperator)sinkOp).getConf().getAlias());
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

  public static void test(Operator<? extends OperatorDesc> sinkOp, ParseContext pCtx) {
    try {
      if (pCtx.getFetchTask() == null) {
        println(0, "####### Before Rewrite #########");
        analyzeHelper(sinkOp, 0);
      } else {
        println(0, "FileSink Operator has been changed into ListSinkOperator!");
        //println(0, ctx);
      }

      //opParseCtx = pCtx.getOpParseCtx();
      ExplainTaskHelper.pCtx = pCtx;
      try {
        ctx = LineageProcFactory.extractLineage(pCtx);
        FuncDepProcFactory.checkFuncDep(ctx);
      }
      catch (Exception e) {
        logExceptions("exceptions.txt", e.getMessage());
        e.printStackTrace();
      }


//      try {
//        RewriteProcFactory.rewritePlan(ctx);
//      }
//      catch (Exception e) {
//        logExceptions("exceptions.txt", e.getMessage() + " " + Arrays.asList(e.getStackTrace()).toString());
//        e.printStackTrace();
//      }

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

  /**
   * called from {@link SemanticAnalyzer#analyzeInternal(org.apache.hadoop.hive.ql.parse.ASTNode)}
   */
  public static void analyze(Operator sinkOp, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
    //System.out.println("SinkOp passed in");
    if (sinkOp == null) {
      return;
    }
    ExplainTaskHelper.opParseCtx = opParseCtx;

    try {
      //SOperator sop = SOperatorFactory.generateSOperatorTree(sinkOp, opParseCtx);
      //sop.setup();
      //printSop(0, sop);
      //if (TestSQLTypes.mode) {
      //System.out.println("!!!!!!TYPE: " +  new TestSQLTypes().test(sop));
      //}
      //System.out.println(TestSQLTypes.tableToPrimaryKeyMap);
    }
    catch (Exception e) {
      System.out.println("----Error in generateSOperatorTree ---");
      e.printStackTrace();
    }

    System.out.println("------------");
    //analyzeRowResolver(sinkOp, opParseCtx);
    analyzeHelper(sinkOp, 0);
    System.out.println("------Tree------");
    printTree(sinkOp, 0);
    //FunctionDependencyTest.printInfo();
  }

  private static void printTree(Operator sinkOp, int level) {
    if (sinkOp instanceof TableScanOperator) {
      String name = pCtx.getTopToTable().get(sinkOp).getTableName();
      println(level, sinkOp + " " +  name.toLowerCase().equals(AbmUtilities.getSampledTable()));
    } else {
      println(level, sinkOp);
    }
    List<Operator> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator l: lst) {
        printTree(l, level + 1);
      }
    }
  }

  public static void analyzeRowResolver(Operator sinkOp, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx) {
    //System.out.println("JQ in rr");
    printRRLevel(sinkOp, 0);
  }

  public static void printRRLevel(Operator sinkOp, int level) {

    RowResolver rr = opParseCtx.get(sinkOp).getRowResolver();
    //    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    //    for (ColumnInfo colInfo : rr.getColumnInfos()) {
    //      if (colInfo.isHiddenVirtualCol()) {
    //        continue;
    //      }
    //      String colName = rr.reverseLookup(colInfo.getInternalName())[1];
    //      fieldSchemas.add(new FieldSchema(colName,
    //          colInfo.getType().getTypeName(), null));
    //    }

    //println(level, sinkOp.getClass() + " " + sinkOp.toString());
    //println(level, rr);
    //println(level, rr.getRowSchema());

    ObjectInspector[] inspectors = sinkOp.getInputObjInspectors();
    if (inspectors != null) {
      for (ObjectInspector ins : inspectors) {
        //println(level, ins.getTypeName() + ":" + ins.getCategory());
        println(level, ins);
      }
    }


    //recursive call
    List<Operator> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator l: lst) {
        printRRLevel(l, level + 1);
      }
    }
  }

  //main work
  @SuppressWarnings("unchecked")
  public static void analyzeHelper(Operator sinkOp, int level) {

    //println(level, sinkOp.getClass());
    println(level, sinkOp.getClass() + " " + sinkOp.toString());
    if (sinkOp instanceof TableScanOperator) {
      //System.out.println("=========== " + opParseCtx.get(sinkOp).getRowResolver().tableOriginalName);

      //System.out.println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumnIDs());
      //System.out.println("========= " + ((TableScanOperator)(sinkOp)).getNeededColumns());
      //System.out.println("======Table Desc " + ((TableScanOperator)(sinkOp)).getTableDesc());
      //System.out.println(qb.getTabNameForAlias("a"));
      //System.out.println(qb.getTabNameForAlias("b"));
    }

    println(level, "Column Expr Map: ");

    Map<String, ExprNodeDesc> map = sinkOp.getColumnExprMap();
    if (map != null && map.entrySet() != null) {
      for (Entry<String, ExprNodeDesc> entry: map.entrySet()) {
        if (entry.getValue() instanceof ExprNodeColumnDesc) {
          ExprNodeColumnDesc expr = (ExprNodeColumnDesc) entry.getValue();
          String[] names = expr.getColumn().split("\\.");
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
      ReduceSinkOperator op = (ReduceSinkOperator) sinkOp;
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

    if (sinkOp instanceof TableScanOperator) {
      //TableScanDesc desc = ((TableScanOperator)sinkOp).getConf();
      //println(level, desc.getAlias());

      //println(level, desc.getFilterExpr());
      //println(level, desc.getBucketFileNameMapping());
      //println(level, desc.getVirtualCols());
      //println(level, desc.getPartColumns());
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

    List<Operator> lst = sinkOp.getParentOperators();
    if (lst != null) {
      for (Operator l: lst) {
        analyzeHelper(l, level + 1);
      }
    }

  }

  //helpers
  public static void printSop(int level, SOperator sop) {
    println(level, sop.op.getClass().toString());
    //println(level, sop);
    println(level, sop.prettyString());
    println();

    for (SOperator op: sop.parents) {
      printSop(level+1, op);
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

}