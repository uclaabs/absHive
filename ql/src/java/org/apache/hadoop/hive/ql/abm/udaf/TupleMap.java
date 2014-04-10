package org.apache.hadoop.hive.ql.abm.udaf;

import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.abm.udaf.LinMergeUtil;

public class TupleMap implements IntComparator, Swapper{

	IntArrayList keys = new IntArrayList();
	/*
	 * needs to be changed to ArrayList<DoubleArrayList>
	 * since many columns exist
	 *
	 * key, col1, col2, col3, ...
	 *
	 */
	List<DoubleArrayList> values = new ArrayList<DoubleArrayList>();

	public void put(int key, DoubleArrayList value)
	{
		this.keys.add(key);
		this.values.add(value);
	}

	public void swap(int arg0, int arg1) {
	  int tmp = keys.get(arg0);
	  keys.set(arg0, keys.get(arg1));
	  keys.set(arg1, tmp);

	  DoubleArrayList tmpV = values.get(arg0);
	  values.set(arg0, values.get(arg1));
	  values.set(arg1, tmpV);
  }

	public int compare(Integer o1, Integer o2) {
	  return Double.compare(keys.get(o1), keys.get(o1));
  }

	public int compare(int arg0, int arg1) {
		return Double.compare(keys.get(arg0), keys.get(arg1));
  }

	public void print()
	{
		for(int i = 0; i < keys.size(); i ++) {
      System.out.println(keys.get(i)+ "--" + values.get(i));
    }
	}

	public void sort()
	{
		Arrays.quickSort(0, keys.size(), this, this);
	}

	public int size()
	{
		return keys.size();
	}

	public void clear()
	{
		keys.clear();
		values.clear();
	}

	public Object getKeyObject()
	{
	  return LinMergeUtil.getIntArrayListObject(this.keys);
	}

	public Object getValueObject()
	{
	  List<Object> list = new ArrayList<Object>();
	  for (DoubleArrayList v: values) {
	    list.add(v.toArray());
	  }
		return list.toArray();
	}

	public void putValue(DoubleArrayList value)
	{
		values.add(value);
	}
}
