package com.dbschema;

import java.sql.DriverPropertyInfo;
import java.util.ArrayList;
import java.util.Locale;

import static com.datastax.driver.core.QueryOptions.DEFAULT_CONSISTENCY_LEVEL;

public class DriverPropertyInfoHelper {
  public static final String ENABLE_SSL = "sslenabled";
  public static final String ENABLE_SSL_DEFAULT = "false";
  public static final String VERIFY_SERVER_CERTIFICATE = "verifyServerCertificate";
  public static final String VERIFY_SERVER_CERTIFICATE_DEFAULT = "true";
  private static final String[] choices = new String[]{"true", "false"};

  public static final String CONSISTENCY_LEVEL = "consistencyLevel";
  public static final String CONSISTENCY_LEVEL_DEFAULT = DEFAULT_CONSISTENCY_LEVEL.name();
  public static final String[] CONSISTENCY_LEVEL_CHOICES = new String[]{"ALL", "EACH_QUORUM", "QUORUM", "LOCAL_QUORUM", "ONE", "TWO", "THREE", "LOCAL_ONE", "ANY", "SERIAL", "LOCAL_SERIAL"};


  public static DriverPropertyInfo[] getPropertyInfo() {
    ArrayList<DriverPropertyInfo> propInfos = new ArrayList<>();

    addPropInfo(propInfos, ENABLE_SSL, ENABLE_SSL_DEFAULT, "Enable ssl.", choices);

    addPropInfo(propInfos, VERIFY_SERVER_CERTIFICATE, VERIFY_SERVER_CERTIFICATE_DEFAULT,
        "Configure a connection that uses SSL but does not verify the identity of the server.", choices);

    addPropInfo(propInfos, CONSISTENCY_LEVEL, CONSISTENCY_LEVEL_DEFAULT,
        "Consistency level determines how many nodes in the replica must respond for the coordinator node to successfully process a non-lightweight transaction.",
        CONSISTENCY_LEVEL_CHOICES);

    return propInfos.toArray(new DriverPropertyInfo[0]);
  }

  private static void addPropInfo(final ArrayList<DriverPropertyInfo> propInfos, final String propName,
                                  final String defaultVal, final String description, final String[] choices) {
    DriverPropertyInfo newProp = new DriverPropertyInfo(propName, defaultVal);
    newProp.description = description;
    if (choices != null) {
      newProp.choices = choices;
    }
    propInfos.add(newProp);
  }

  public static boolean isTrue(String value) {
    return value != null && (value.equals("1") || value.toLowerCase(Locale.ENGLISH).equals("true"));
  }
}
