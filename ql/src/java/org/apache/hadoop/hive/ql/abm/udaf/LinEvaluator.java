package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LinEvaluator extends GenericUDAFEvaluator {

	protected PrimitiveObjectInspector inputKeyOI;
	protected List<PrimitiveObjectInspector> inputValueOI;
	protected StandardMapObjectInspector loi;
	protected StructObjectInspector outputOI;
	protected StructObjectInspector partialOutputOI;
	protected StructObjectInspector internalMergeOI;
	protected int groupid = 0;

	@Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
    throws HiveException {
		super.init(m, parameters);

		String[] finalCols = {"ID", "keys", "Values"};
		ObjectInspector[] finalInspectors = {PrimitiveObjectInspectorFactory.javaIntObjectInspector,
					PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
					ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector))};

		String[] interCols = {"keys", "Values"};
		ObjectInspector[] interInspectors = {finalInspectors[1], finalInspectors[2]};
		int parameterSize = parameters.length;


		if (m == Mode.PARTIAL1)
		{
			inputKeyOI = (PrimitiveObjectInspector) parameters[0];
			inputValueOI = new ArrayList<PrimitiveObjectInspector>();

      for(int i = 1; i < parameterSize; i++) {
        inputValueOI.add((PrimitiveObjectInspector) parameters[i]);
      }

			partialOutputOI =  ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(interCols) , Arrays.asList(interInspectors));
			return partialOutputOI;
		}
		else {
			if (!(parameters[0] instanceof StructObjectInspector))
			{
				inputKeyOI = (PrimitiveObjectInspector) parameters[0];
				inputValueOI = new ArrayList<PrimitiveObjectInspector>();

	      for(int i = 1; i < parameterSize; i++) {
          inputValueOI.add((PrimitiveObjectInspector) parameters[i]);
        }

				partialOutputOI =  ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(interCols) , Arrays.asList(interInspectors));
				return partialOutputOI;
			}
			else
			{
				internalMergeOI = (StructObjectInspector) parameters[0];
				outputOI =	ObjectInspectorFactory.getStandardStructObjectInspector(Arrays.asList(finalCols), Arrays.asList(finalInspectors));
				return outputOI;
			}
		}
	}

	static class MkMapAggregationBuffer implements AggregationBuffer
	{
		TupleMap map;
		EWAHCompressedBitmap keyMap;
		List<DoubleArrayList> valueArray;
	}

	protected void initContainer(MkMapAggregationBuffer ret)
	{
		ret.map.clear();
		ret.keyMap = null;
		ret.valueArray.clear();
	}

	protected void mergeAdd(MkMapAggregationBuffer myagg, EWAHCompressedBitmap ewahBitmap1, List<DoubleArrayList> partialValues)
	{

		if(myagg.keyMap == null)
		{
			myagg.keyMap = ewahBitmap1;
			myagg.valueArray.addAll(partialValues);
		}
		else
		{
			Iterator<Integer> newit = ewahBitmap1.iterator();
			Iterator<Integer> oldit = myagg.keyMap.iterator();

			int oldint = -1, newint = -1;
			int itemCnt = 0, newIdx = -1;
			int totalCnt = myagg.valueArray.size() + partialValues.size();

			while(itemCnt < totalCnt)
			{
				if(oldint < 0 && oldit.hasNext()) {
          oldint = oldit.next();
        }

				if(newint < 0 && newit.hasNext())
				{
					newint = newit.next();
					newIdx ++;
				}
				if(newint < oldint || oldint == -1)
				{
					myagg.valueArray.add(itemCnt, partialValues.get(newIdx));
					newint = -1;
				} else {
          oldint = -1;
        }

				itemCnt ++;
			}

			myagg.keyMap = myagg.keyMap.or(ewahBitmap1);
		}
	}

	@Override
  public void reset(AggregationBuffer agg) throws HiveException
	{
		this.groupid ++;
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer)agg;
		initContainer(myagg);
	}

	@Override
  public AggregationBuffer getNewAggregationBuffer() throws HiveException
	{
		MkMapAggregationBuffer ret = new MkMapAggregationBuffer();
		ret.map = new TupleMap();
		ret.keyMap = null;
		ret.valueArray = new ArrayList<DoubleArrayList>();
		initContainer(ret);
		return ret;
	}

	@Override
  public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException
	{
		if(parameters.length == 0 || parameters[0] == null) {
      return;
    }

		int key = ((IntObjectInspector)inputKeyOI).get(parameters[0]);
		DoubleArrayList values = new DoubleArrayList();

		for(int i = 1; i < parameters.length; i ++) {
      values.add(PrimitiveObjectInspectorUtils.getDouble(parameters[i], inputValueOI.get(i-1)));
    }

		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
		myagg.map.put(key, values);
	}

	@Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException
	{
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
		myagg.map.sort();
		Object[] ret = new Object[2];
		ret[0] = myagg.map.getKeyObject();
		ret[1] = myagg.map.getValueObject();
		myagg.map.clear();
		return ret;
	}

	@Override
  public void merge(AggregationBuffer agg, Object partialRes) throws HiveException
	{
//		System.out.println(partialRes);
		if(partialRes == null) {
      return;
    }

		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
    EWAHCompressedBitmap keys = LinMergeUtil.getKeysFrom(partialRes, internalMergeOI);
    List<DoubleArrayList> values = LinMergeUtil.getValuesFrom(partialRes, internalMergeOI);

		this.mergeAdd(myagg, keys, values);
	}

	@Override
  public Object terminate(AggregationBuffer agg) throws HiveException
	{
		MkMapAggregationBuffer myagg = (MkMapAggregationBuffer) agg;
		Object[] ret = new Object[3];
		ret[0] = this.groupid;

		if(myagg.valueArray.size() > 0)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oo;
			try {
				oo = new ObjectOutputStream(baos);
				myagg.keyMap.writeExternal(oo);
				oo.close();
			} catch (IOException e) {
			e.printStackTrace();
			}
			Object lineage = ObjectInspectorUtils.copyToStandardObject( baos.toByteArray(), PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
			ret[1] = lineage;

		  List<Object> list = new ArrayList<Object>();
		  for (DoubleArrayList v: myagg.valueArray) {
		    list.add(v.toArray());
		  }
		  ret[2] = list.toArray();

		}
		else
		{
			ret[1] = null;
			ret[2] = null;
		}

		return ret;
	}

}
