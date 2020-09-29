package com.dbschema;


import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

public class CassandraConnection implements Connection {
    /**
     * This query retrieves Cassandra 2.x columns in DataGrip.
     * <p>
     * DataGrip before 2019.3 version assumes that index_name column does not contain null values.
     * Driver since v1.3.2 version returns null value if a string is null (used to return "null")
     * It means that DataGrip <2019.3 and driver v1.3.2 are incompatible.
     * <p>
     * To make driver and DG compatible driver will return "null" strings instead of null values
     * for this particular query in PreparedStatement.
     * See also https://youtrack.jetbrains.com/issue/DBE-9091
     */
    private static final String SELECT_COLUMNS_INTRO_QUERY = "SELECT column_name as name,\n       validator,\n       columnfamily_name as table_name,\n       type,\n       index_name,\n       index_options,\n       index_type,\n       component_index as position\nFROM system.schema_columns\nWHERE keyspace_name = ?";

    private final Session session;
    private final CassandraJdbcDriver driver;
    private final boolean returnNullStringsFromIntroQuery;
    private boolean isClosed = false;
    private boolean isReadOnly = false;
    private ConsistencyLevel consistencyLevel;

    CassandraConnection(Session session, CassandraJdbcDriver cassandraJdbcDriver, boolean returnNullStringsFromIntroQuery, ConsistencyLevel consistencyLevel) {
        this.session = session;
        driver = cassandraJdbcDriver;
        this.returnNullStringsFromIntroQuery = returnNullStringsFromIntroQuery;
        this.consistencyLevel = consistencyLevel;
    }

    public String getCatalog() throws SQLException {
        checkClosed();
        try {
            return session.getLoggedKeyspace();
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public Session getSession() {
        return session;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        try {
            return new CassandraStatement(session, consistencyLevel);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        try {
            com.datastax.driver.core.PreparedStatement statement = session.prepare(sql);
            statement.setConsistencyLevel(consistencyLevel);
            return new CassandraPreparedStatement(session, statement, returnNullStringsFromIntroQuery || !SELECT_COLUMNS_INTRO_QUERY.equals(sql));
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Cassandra does not support SQL natively.");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
    }

    @Override
    public void close() {
        // Improved the physical connection to be closed.( https://github.com/DataGrip/cassandra-jdbc-driver/issues/4 )
        if(!isClosed) {
        	final Cluster _cluster = session.getCluster();
        	session.close();
        	_cluster.close();
        }
        
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new CassandraMetaData(this, driver);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        isReadOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return isReadOnly;
    }

    @Override
    public void setCatalog(String catalog) {

    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        // Since the only valid value for MongDB is Connection.TRANSACTION_NONE, and the javadoc for this method
        // indicates that this is not a valid value for level here, throw unsupported operation exception.
        throw new UnsupportedOperationException("Cassandra provides no support for transactions.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                              int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) {
        /* Cassandra does not support setting client information in the database. */
    }

    @Override
    public void setClientInfo(Properties properties) {
        /* Cassandra does not support setting client information in the database. */
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        checkClosed();

        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        checkClosed();
        return null;
    }


    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public void setSchema(String schema) {
        setCatalog(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return getCatalog();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


}
