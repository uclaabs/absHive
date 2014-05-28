package org.apache.hadoop.hive.ql.abm.datatypes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import com.googlecode.javaewah.EWAHCompressedBitmap;

public class LineageList {
  
  public static Object toArray(List<EWAHCompressedBitmap> lineageList) throws IOException {
    
    Object[] ret = new Object[lineageList.size()];
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oo = new ObjectOutputStream(baos);
    
    for(int i = 0; i < lineageList.size(); i ++) {
      
      baos.reset();
      oo.reset();
      lineageList.get(i).writeExternal(oo);
      ret[i] = ObjectInspectorUtils.copyToStandardObject(baos.toByteArray(), PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    }
    oo.close();
    return ret;
  }

}
