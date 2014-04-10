package org.apache.hadoop.hive.ql.abm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 *
 * AbmUtilities: utilities for analytical bootstrap method (ABM).
 *
 */
public final class AbmUtilities {

  private static final Log LOG = LogFactory.getLog(AbmUtilities.class.getName());

  private static String sampledTable;
  private static String label;

  private static final Map<String, Set<String>> schemaPrimaryKeyMap = new HashMap<String, Set<String>>();

  private static final AtomicLong broadcastId = new AtomicLong(-1);

  public static void setAbmMode(HiveConf conf) throws SemanticException {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_ABM)) {
      // Turn off skewed data support, because of
      // (1) group-by with map-side group-by and skewed data, and
      // (2) skew join optimizer.
      conf.setBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW, false);
      conf.setBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME, false);

      // Turn off index-related optimization, because of
      // (1) rewrite group-by using index (RewriteGBUsingIndex).
      conf.setBoolVar(HiveConf.ConfVars.HIVEOPTGBYUSINGINDEX, false);

      // Turn off map join hints
      conf.setBoolVar(HiveConf.ConfVars.HIVEIGNOREMAPJOINHINT, true);

      // No correlation optimizer support in hive 0.11
      // Turn off correlation optimizer.
      // Anyway, shark does not support it (no Demux and Mux).
      //conf.setBoolVar(HiveConf.ConfVars.HIVEOPTCORRELATION, false);

      // Configure schema functional dependency
      String path = conf.getVar(HiveConf.ConfVars.HIVE_ABM_SCHEMA);
      loadSchemaPrimaryKeyMap(path);

      // Configure sampled table
      sampledTable = conf.getVar(HiveConf.ConfVars.HIVE_ABM_SAMPLED_TABLE);

      // Label -- only for debugging purpose
      label = conf.getVar(HiveConf.ConfVars.HIVE_ABM_LABEL);
    }
  }

  public static String getLabel() {
    return label;
  }

  public static void checkAndReport(HiveConf conf, ErrorMsg msg) throws SemanticException {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_ABM)) {
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

  public static long newBroadcastId() {
    return broadcastId.getAndDecrement();
  }

}