package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.IntWritable;

/**
 * GenRowID
 */
@Description(name = "GenRowID",
    value = "_FUNC_() - Returns a generated row sequence number starting from 1")
@UDFType(stateful = true)
public class GenRowID extends UDF
{
  private final IntWritable result = new IntWritable();

  public GenRowID() {
    result.set(0);
  }

  public IntWritable evaluate() {
    result.set(result.get() + 1);
    //System.out.println("GenRowIDUDF:\t" + result.get());
    return result;
  }
}