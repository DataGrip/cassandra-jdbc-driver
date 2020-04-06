package com.dbschema;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.google.common.base.Strings;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyStore;
import java.util.*;
import java.util.logging.Logger;

import static com.dbschema.DriverPropertyInfoHelper.*;
import static com.dbschema.SSLUtil.getTrustEverybodySSLContext;

public class CassandraClientURI {

    private static final Logger logger = Logger.getLogger("CassandraClientURILogger");

    static final String PREFIX = "jdbc:cassandra://";

    private final List<String> hosts;
    private final String keyspace;
    private final String collection;
    private final String uri;
    private final String userName;
    private final String password;
    private final boolean sslEnabled;
    private final boolean verifyServerCert;

    public CassandraClientURI(String uri, Properties info) {
        this.uri = uri;
        if (!uri.startsWith(PREFIX))
            throw new IllegalArgumentException("URI needs to start with " + PREFIX);

        uri = uri.substring(PREFIX.length());


        String serverPart;
        String nsPart;
        Map<String, List<String>> options = null;

        {
            int lastSlashIndex = uri.lastIndexOf("/");
            if (lastSlashIndex < 0) {
                if (uri.contains("?")) {
                    throw new IllegalArgumentException("URI contains options without trailing slash");
                }
                serverPart = uri;
                nsPart = null;
            } else {
                serverPart = uri.substring(0, lastSlashIndex);
                nsPart = uri.substring(lastSlashIndex + 1);

                int questionMarkIndex = nsPart.indexOf("?");
                if (questionMarkIndex >= 0) {
                    options = parseOptions(nsPart.substring(questionMarkIndex + 1));
                    nsPart = nsPart.substring(0, questionMarkIndex);
                }
            }
        }

        this.userName = getOption(info, options, "user");
        this.password = getOption(info, options, "password");
        String sslEnabledOption = getOption(info, options, "sslenabled");
        this.sslEnabled = isTrue(sslEnabledOption);
        String verifyServerCertOption = getOption(info, options, VERIFY_SERVER_CERTIFICATE);
        this.verifyServerCert = isTrue(verifyServerCertOption);


        { // userName,password,hosts
            List<String> all = new LinkedList<>();

            Collections.addAll(all, serverPart.split(","));

            hosts = Collections.unmodifiableList(all);
        }

        if (nsPart != null && nsPart.length() != 0) { // keyspace._collection
            int dotIndex = nsPart.indexOf(".");
            if (dotIndex < 0) {
                keyspace = nsPart;
                collection = null;
            } else {
                keyspace = nsPart.substring(0, dotIndex);
                collection = nsPart.substring(dotIndex + 1);
            }
        } else {
            keyspace = null;
            collection = null;
        }
    }

    /**
     * @return option from properties or from uri if it is not found in properties.
     * null if options was not found.
     */
    private String getOption(Properties properties, Map<String, List<String>> options, String optionName) {
        if (properties != null) {
            String option = (String) properties.get(optionName);
            if (option != null) {
                return option;
            }
        }
        return getLastValue(options, optionName);
    }

    Cluster createCluster() throws java.net.UnknownHostException, SSLParamsException {
        Cluster.Builder builder = Cluster.builder();
        int port = -1;
        for (String host : hosts) {
            int idx = host.indexOf(":");
            if (idx > 0) {
                port = Integer.parseInt(host.substring(idx + 1).trim());
                host = host.substring(0, idx).trim();
            }
            builder.addContactPoints(InetAddress.getByName(host));
            logger.info("sslenabled: " + sslEnabled);
            if (sslEnabled) {
                if (verifyServerCert) {
                    builder.withSSL();
                }
                else {
                    String keyStoreType = System.getProperty("javax.net.ssl.keyStoreType", KeyStore.getDefaultType());
                    String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword", "");
                    String keyStoreUrl = System.getProperty("javax.net.ssl.keyStore", "");
                    // check keyStoreUrl
                    if (!Strings.isNullOrEmpty(keyStoreUrl)) {
                        try {
                            new URL(keyStoreUrl);
                        } catch (MalformedURLException e) {
                            keyStoreUrl = "file:" + keyStoreUrl;
                        }
                    }
                    SSLOptions options = RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(getTrustEverybodySSLContext(keyStoreUrl, keyStoreType, keyStorePassword)).build();
                    builder.withSSL(options);
                }
            }
        }
        if ( port > -1 ){
            builder.withPort( port );

        }
        if (userName != null && !userName.isEmpty() && password != null) {
            builder.withCredentials(userName, password);
            System.out.println("Using authentication as user '" + userName + "'");
        }
        return builder.build();
    }


    private String getLastValue(final Map<String, List<String>> optionsMap, final String key) {
        if (optionsMap == null) return null;
        List<String> valueList = optionsMap.get(key);
        if (valueList == null || valueList.size() == 0) return null;
        return valueList.get(valueList.size() - 1);
    }

    private Map<String, List<String>> parseOptions(String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<>();

        for (String _part : optionsPart.split("[&;]")) {
            int idx = _part.indexOf("=");
            if (idx >= 0) {
                String key = _part.substring(0, idx).toLowerCase(Locale.ENGLISH);
                String value = _part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<>(1);
                }
                valueList.add(value);
                optionsMap.put(key, valueList);
            }
        }

        return optionsMap;
    }


    // ---------------------------------

    /**
     * Gets the username
     *
     * @return the username
     */
    public String getUsername() {
        return userName;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Gets the ssl enabled property
     *
     * @return the ssl enabled property
     */
    public Boolean getSslEnabled() {
        return sslEnabled;
    }

    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    public List<String> getHosts() {
        return hosts;
    }

    /**
     * Gets the keyspace name
     *
     * @return the keyspace name
     */
    public String getKeyspace() {
        return keyspace;
    }


    /**
     * Gets the collection name
     *
     * @return the collection name
     */
    public String getCollection() {
        return collection;
    }

    /**
     * Get the unparsed URI.
     *
     * @return the URI
     */
    public String getURI() {
        return uri;
    }



    @Override
    public String toString() {
        return uri;
    }
}
