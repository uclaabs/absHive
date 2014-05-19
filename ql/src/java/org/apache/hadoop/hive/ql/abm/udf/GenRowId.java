package org.apache.hadoop.hive.ql.abm.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *
 * GenRowId generates unique ID for each row.
 *
 */
@Description(name = "GenRowId", value = "_FUNC_() - Returns a unique ID (split ID + sequence ID)")
@UDFType(stateful = true)
public class GenRowId extends GenericUDF {

	private Long id = 0L;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
	  if (arguments.length != 0) {
      throw new UDFArgumentException("This function takes no argument!");
    }
    return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  }

  public void setSplitId(long split) {
    id = split << 32;
  }

	@Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    return id++;
  }

  @Override
  public String getDisplayString(String[] arg0) {
	  return "GenRowId()";
  }

}
