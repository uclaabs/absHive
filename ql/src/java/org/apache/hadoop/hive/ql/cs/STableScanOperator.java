package org.apache.hadoop.hive.ql.cs;


import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

class RootColumnNotFoundException extends RuntimeException {
	private static final long serialVersionUID = 1L;
}

/**
 *
 * STableScanOperator is special, its mapping from cols itself to itself, but in two different types
 * @author victor
 *
 */
public class STableScanOperator extends SOperator {

	/**
	 * Table original name, not alias
	 */
	Long id = 0L;
	Map<SDerivedColumn, SBaseColumn> baseColumnMap = new HashMap<SDerivedColumn, SBaseColumn>();

	public STableScanOperator(TableScanOperator op, List<SOperator> parents, LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> ctx) {
		super(op, parents, ctx);

		id = UniqueIdGenerater.getNextId();
	}

	@Override
	protected void buildColumns() {
		RowSchema schema = op.getSchema();
		for (ColumnInfo info: schema.getSignature()) {
			columns.add(SDerivedColumn.create(
					info.getInternalName(), info.getTabAlias(), this,
					new ExprNodeColumnDesc(double.class, info.getInternalName(), info.getTabAlias(), false)));
		}
	}

	@Override
	public void setup() {
		for (SDerivedColumn scol: columns) {
			//convert sCol to sBaseCcol
		  /*
			baseColumnMap.put(scol, new SBaseColumn(
					scol.name,
					ctx.get(op).getRowResolver().tableOriginalName));
					*/
		}
	}

	@Override
	public boolean hasNestedAggregates() {
		return false;
	}

//	@Override
//	public boolean isEligible(HashSet<FD> rules, HashSet<SBaseColumn> bases) {
//		// add base FD
//		// TODO
//
//		bases.addAll(baseColumnMap.values());
//
//		return true;
//	}
}