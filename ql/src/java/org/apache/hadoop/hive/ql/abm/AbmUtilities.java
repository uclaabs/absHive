package org.apache.hadoop.hive.ql.abm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.abm.rewrite.ErrorMeasure;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 * AbmUtilities: utilities for analytical bootstrap method (ABM).
 *
 */
public final class AbmUtilities {

  private static final Log LOG = LogFactory.getLog(AbmUtilities.class.getName());

  public static final String ABM_CACHE_DB_NAME = "AbmCache";
  public static final String ABM_CACHE_INPUT_PREFIX = "Input_";
  public static final String ABM_CACHE_OUTPUT_PREFIX = "Output_";

  private static boolean inAbmMode = false;
  private static HashMap<HiveConf.ConfVars, Boolean> prevSetting =
      new HashMap<HiveConf.ConfVars, Boolean>();

  private static String sampledTable;
  private static String queryResultFileFormat;
  private static final Map<String, Set<String>> schemaPrimaryKeyMap = new HashMap<String, Set<String>>();
  private static ErrorMeasure measure;
  private static final ArrayList<String> fieldNames = new ArrayList<String>();

  private static String label;

  public static void setAbmMode(HiveConf conf) throws SemanticException {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_ABM)) {
      inAbmMode = true;
      // Turn off skewed data support, because of
      // (1) group-by with map-side group-by and skewed data, and
      // (2) skew join optimizer.
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVEGROUPBYSKEW, false);
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME, false);

      // Turn off index-related optimization, because of
      // (1) rewrite group-by using index (RewriteGBUsingIndex).
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVEOPTGBYUSINGINDEX, false);

      // Turn off map join hints
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVEIGNOREMAPJOINHINT, false); // BUT no one is using this!
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN, false);
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN, false);

      // Turn on this:
      // (1) Make sure CommonJoinResolver will go through our code;
      // (2) Prevent ReduceSinkDeDuplication to apply JOIN...RS rewriting
      setAndRecordBoolVar(conf, HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASK, true);

      // No correlation optimizer support in hive 0.11
      // Turn off correlation optimizer.
      // Anyway, shark does not support it (no Demux and Mux).
      //conf.setBoolVar(HiveConf.ConfVars.HIVEOPTCORRELATION, false);

      // Configure schema functional dependency
      String path = conf.getVar(HiveConf.ConfVars.HIVE_ABM_SCHEMA);
      loadSchemaPrimaryKeyMap(path);

      // Configure sampled table
      sampledTable = conf.getVar(HiveConf.ConfVars.HIVE_ABM_SAMPLED_TABLE);

      // QueryResultFileFormat: for caching Select outputs
      queryResultFileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);

      // Error measure
      measure = ErrorMeasure.get(HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_ABM_MEASURE));

      // Label -- only for debugging purpose
      label = conf.getVar(HiveConf.ConfVars.HIVE_ABM_LABEL);
    } else {
      inAbmMode = false;
      for (Map.Entry<HiveConf.ConfVars, Boolean> entry : prevSetting.entrySet()) {
        conf.setBoolVar(entry.getKey(), entry.getValue());
      }
    }
  }

  private static void setAndRecordBoolVar(HiveConf conf, HiveConf.ConfVars confVar, boolean on) {
    prevSetting.put(confVar, conf.getBoolVar(confVar));
    conf.setBoolVar(confVar, on);
  }

  public static boolean inAbmMode() {
    return inAbmMode;
  }

  public static String getLabel() {
    return label;
  }

  public static void checkAndReport(ErrorMsg msg) throws SemanticException {
    if (inAbmMode) {
      report(msg);
    }
  }

  private static void loadSchemaPrimaryKeyMap(String path) throws SemanticException {
    LOG.info("ABM_SCHEMA_PATH: " + path);

    BufferedReader in = null;
    try {
      in = new BufferedReader(new FileReader(path));

      String text;
      while ((text = in.readLine()) != null) {
        String[] line = text.split(":");
        String tableName = line[0].trim();

        String[] cols = line[1].split(",");
        HashSet<String> colList = new HashSet<String>();
        for (String col : cols) {
          colList.add(col.trim());
        }

        schemaPrimaryKeyMap.put(tableName, colList);
      }

      in.close();
    } catch (Exception e) {
      report(ErrorMsg.SCHEMA_MISSING_ABM);
    }

    LOG.info("Primary Keys: \n" + schemaPrimaryKeyMap);
  }

  /**
   * Return schema functional dependency
   */
  public static Map<String, Set<String>> getSchemaPrimaryKeyMap() {
    return schemaPrimaryKeyMap;
  }

  /**
   * Return sampled table;
   */
  public static String getSampledTable() {
    return sampledTable;
  }

  public static void report(ErrorMsg msg) throws SemanticException {
    throw new SemanticException(msg.getMsg());
  }

  public static void report(ErrorMsg msg, String reason) throws SemanticException {
    throw new SemanticException(msg.format(reason));
  }

  public static void warn(ErrorMsg msg) {
    LOG.warn(msg.getMsg());
  }

  public static void recordViewSchema(RowResolver rr) {
    fieldNames.clear();
    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      if (colInfo.isHiddenVirtualCol()) {
        continue;
      }
      String colName = rr.reverseLookup(colInfo.getInternalName())[1];
      fieldNames.add(colName);
    }
    fieldNames.add("_existence_prob.");
  }

  public static ArrayList<String> getViewSchema() {
    return fieldNames;
  }

  public static String getQueryResultFileFormat() {
    return queryResultFileFormat;
  }

  public static ErrorMeasure getErrorMeasure() {
    return measure;
  }
  
  //TODO fix this
  public static int getTotalTupleNumber() {
    return 6000000;
  }

}
