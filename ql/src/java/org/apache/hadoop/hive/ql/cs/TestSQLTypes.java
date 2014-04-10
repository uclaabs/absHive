package org.apache.hadoop.hive.ql.cs;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;

public class TestSQLTypes {

	public static boolean mode = true;
	public static String factTableName = "lineitem"; //all to lower cases to test
	public static HashMap<String, HashSet<String>> tableToPrimaryKeyMap;
	public static String path = "/home/victor/primarykey.txt";

	public boolean r1 = false;
	public boolean r2 = false;
	public boolean r3 = false;
	public boolean r4 = false;

	public SOperator groupByOpStore;

//	static {
//		Scanner sc = null;
//		try {
//			sc = new Scanner(new File(path));
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		}
//
//		tableToPrimaryKeyMap = new HashMap<String, HashSet<String>>();
//
//		try {
//			String tmp = null;
//
//			while (sc.hasNextLine()) {
//				tmp = sc.nextLine();
//				String[] arrs = tmp.split(":");
//
//				String name = arrs[0].trim();
//				HashSet<String> set = new HashSet<String>();
//				String[] cols = arrs[1].split(",");
//				for (String col: cols) {
//					set.add(col.trim());
//				}
//
//				tableToPrimaryKeyMap.put(name, set);
//			}
//		}
//		catch (Exception e) {
//			throw new RuntimeException("Invalid Input file!");
//		}
//	}

	public TestSQLTypes() {
	}

	public int test(SOperator sOperator) {

		//if (!hasGroupBy(sOperator)) return 4;
		if (sOperator.hasUnionOperators()// || sOperator.hasNestedAggregates()
				) 
			return 0;

		if (sOperator.hasSpecialAggregates()) {
			return 1;
		}
		
		if (sOperator.hasAggregates() && !sOperator.isComplex(false)) {
			return 2;
		}
		
		if (sOperator.hasSelfJoin()) {
			return 3;
		}
		
		if (sOperator.hasCorrelatedAggregates()) {
			return 5;
		}
		
		//if (isType3(sOperator)) return 3;
		//if (isType4(sOperator)) return 4;

		return 4;
	}

//	private Collection<SBaseColumn> getPrimaryKeyColumns(STableScanOperator scanOp) {
//		HashSet<String> primaryKeyBasics = tableToPrimaryKeyMap.get(scanOp.tableName.toLowerCase());
//
//		HashSet<SBaseColumn> set = new HashSet<SBaseColumn>();
//		for (String key: primaryKeyBasics) {
//			set.add(new SBaseColumn(key, scanOp, null));
//		}
//		return set;
//	}

	private void extractFDs(ExprNodeDesc rootExpr, HashSet<FD> fdSet, SOperator sop) {
		/*
		if (rootExpr instanceof ExprNodeGenericFuncDesc) {
			ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc)rootExpr;
			GenericUDF udf = funcDesc.getGenericUDF();

			List<ExprNodeDesc> childExprs = funcDesc.getChildExprs();
			if (udf instanceof GenericUDFOPAnd) {
				for (ExprNodeDesc d: childExprs) {
					extractFDs(d, fdSet, sop);
				}

			} else if (udf instanceof GenericUDFOPEqual) {

				ExprNodeDesc leftChild = childExprs.get(0);
				ExprNodeDesc rightChild = childExprs.get(1);

				SBaseColumn det = null;
				SBaseColumn dep = null;
				if (leftChild instanceof ExprNodeColumnDesc) {
					String c1 = leftChild.getExprString();
					if (rightChild instanceof ExprNodeColumnDesc) {
						String c2 = rightChild.getExprString();
						det = sop.getRootColumn(new SColumn(c1, ((ExprNodeColumnDesc) leftChild).getTabAlias()));
						dep = sop.getRootColumn(new SColumn(c2, ((ExprNodeColumnDesc) rightChild).getTabAlias()));

					} else if (rightChild instanceof ExprNodeConstantDesc) {
						//String v2 = rightChild.getExprString();
						det = sop.getRootColumn(new SColumn(c1, ((ExprNodeColumnDesc) leftChild).getTabAlias()));
						dep = null;

					}
				} else if (leftChild instanceof ExprNodeConstantDesc) {
					//String v1 = leftChild.getExprString();
					if (rightChild instanceof ExprNodeColumnDesc) {
						String c2 = rightChild.getExprString();
						det = null;
						dep = sop.getRootColumn(new SColumn(c2, ((ExprNodeColumnDesc) rightChild).getTabAlias()));

					} else if (rightChild instanceof ExprNodeConstantDesc) {
						//String v2 = rightChild.getExprString();
						det = null;
						dep = null;
					}
				}

				if (det != null || dep != null) {
					fdSet.add(new FD(det, dep));
					fdSet.add(new FD(dep, det));
				}
			}
		} else {
			//nothing to do
		}*/
	}

//	private boolean isType4(SOperator sOperator) {
//		checkType4(sOperator, new HashSet<FD>());
//		return r4;
//	}

//	private STableScanOperator checkType4(SOperator sOperator, HashSet<FD> fdSet) {
//
//		//recursive call
//		STableScanOperator scanOp = null;
//		for (SOperator parent : sOperator.parents) {
//			scanOp = checkType4(parent, fdSet);
//			if (scanOp != null) {
//				break; //two parents have scanOp are excluded from type3
//			}
//		}
//
//		//case analysis
//		if (sOperator.op instanceof TableScanOperator) {
//			//store FD
//			String tableName = ((STableScanOperator)sOperator).tableName.toLowerCase();
//
//			Collection<SBaseColumn> primaryKeys = getPrimaryKeyColumns(((STableScanOperator)sOperator));
//			Collection<SBaseColumn> allColumns = ((STableScanOperator)sOperator).baseColumnMap.values();
//			fdSet.add(new FD(primaryKeys, allColumns));
//
//			if (!tableName.equals(factTableName)) {
//				return null;
//			} else {
//				return (STableScanOperator) sOperator;
//			}
//
//		} else if (sOperator.op instanceof FilterOperator) {
//			//store FD
//			extractFDs(((SFilterOperator) sOperator).expr, fdSet, sOperator);
//		} else if (sOperator.op instanceof JoinOperator) {
//			//since Join Conditions will be extracted to Filter, we do nothing here
//		} else if (sOperator.op instanceof SelectOperator) {
//			if (scanOp == null) {
//				return null;
//			}
//			infer(sOperator, fdSet, scanOp);
//		} else if (sOperator.op instanceof GroupByOperator) {
//			if (scanOp == null) {
//				return null;
//			}
//			infer(sOperator, fdSet, scanOp);
//		}
//
//		return scanOp;
//	}

	private void infer(SOperator sOperator, HashSet<FD> fdHashSet,
			STableScanOperator scanOp) {
		//judge
		/*
		Collection<SBaseColumn> outputColumns = sOperator.getAllRootColumns();
		Set<SBaseColumn> det = new HashSet<SBaseColumn>();
		for (SBaseColumn bcol: outputColumns) {
			if (bcol != null) {
				det.add(bcol);
			}
		}

		Collection<SBaseColumn> inputColumns = sOperator.parents.get(0).getAllRootColumns();
		Set<SBaseColumn> inputs = new HashSet<SBaseColumn>();
		for (SBaseColumn bcol: inputColumns) {
			if (bcol != null) {
				inputs.add(bcol);
			}
		}

		det.addAll(getPrimaryKeyColumns(scanOp));

		if (FD.judge(FD.infer(det, fdHashSet), inputs)) {
			r4 = true;
		}*/

	}

//	private boolean checkFactTable(SOperator sOperator) {
//
//		List<Boolean> results = new ArrayList<Boolean>();
//
//		for (SOperator parent : sOperator.parents) {
//			results.add(checkFactTable(parent));
//		}
//
//		if (sOperator.op instanceof TableScanOperator) {
//			if (((STableScanOperator)sOperator).tableName.toLowerCase().equals(factTableName)) {
//				return true;
//			} else {
//				return false;
//			}
//		} else if (sOperator.op instanceof JoinOperator) {
//			boolean t = false;
//			//System.out.println(results.size());
//			for (Boolean b: results) {
//				//System.out.println("====t  " + t);
//				if (t && b) {
//					r3 = true;
//				}
//				t = t || b;
//			}
//		} else if (sOperator.op instanceof GroupByOperator) {
//			return false;
//		}
//
//		boolean re = false;
//		for (Boolean r: results) {
//			re = re || r;
//		}
//
//		return re;
//	}

//	private boolean isType3(SOperator sOperator) {
//		checkFactTable(sOperator);
//		return r3;
//	}

//	private boolean hasGroupBy(SOperator sOperator) {
//		boolean e = false;
//
//		if (sOperator.parents.size() > 0) {
//			for (SOperator parent : sOperator.parents) {
//				e = e || exist(parent);
//			}
//		}
//
//		if (sOperator instanceof SGroupByOperator) {
//			e = true;
//		}
//
//		return e;
//	}

	private void testAggregate(String name) {
		if (name.equals("sum") || name.equals("avg") || name.equals("count")) {}
		else {
			r1 = true;
		}
	}

	private void checkType1(SOperator sOperator) {
		if (sOperator.parents.size() > 0) {
			for (SOperator parent : sOperator.parents) {
				checkType1(parent);
			}
		}

		if (sOperator.op instanceof GroupByOperator) {
			GroupByOperator op = (GroupByOperator)sOperator.op;
			ArrayList<AggregationDesc> lst = op.getConf().getAggregators();

			for (AggregationDesc desc: lst) {
				testAggregate(desc.getGenericUDAFName());
			}
		}
	}

	private boolean isType1(SOperator sOperator) {
		checkType1(sOperator);
		if (r1) {
			return true;
		} else {
			return false;
		}
	}

//	private boolean isType2(SOperator sOperator) {
//		if (!hasGroupBy(sOperator)) return false;
//
//		r2 = true;
//		exist(sOperator);
//		return r2;
//	}

//	private boolean exist(SOperator sOperator) {
//		boolean e = false;
//
//		if (sOperator.parents.size() > 0) {
//			for (SOperator parent : sOperator.parents) {
//				e = e || exist(parent);
//			}
//		}
//
//		if (e) {
//			Operator rootOp = sOperator.op;
//			if (rootOp instanceof FilterOperator ||
//					rootOp instanceof CommonJoinOperator ||
//					rootOp instanceof GroupByOperator) {
//				r2 = false;
//			} else if (rootOp instanceof SelectOperator) {
//				try {
//					if (sOperator.getAllRootColumns().containsAll(groupByOpStore.getAllRootColumns())) {}
//					else {
//						r2 = false;
//					}
//				}
//				catch (Exception ex) {}
//			}
//
//			//LimitOperator //limit
//			//ExtractOperator) //order by
//		}
/*
		if (sOperator instanceof SGroupByOperator && !((SGroupByOperator)sOperator).isDeduplication) {
			groupByOpStore = sOperator;
			return true;
		}
		else {
			return e;
		}
	}*/

}
