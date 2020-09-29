package com.dbschema;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;

import java.sql.*;

/**
 * @author Liudmila Kornilova
 **/
public abstract class CassandraBaseStatement implements Statement {
    final com.datastax.driver.core.Session session;
    BatchStatement batchStatement = null;
    private boolean isClosed = false;
    ResultSet result;

    CassandraBaseStatement(Session session) {
        this.session = session;
    }

    @Override
    public void close() throws SQLException {
        isClosed = true;
    }

    void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Statement was previously closed.");
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    boolean executeInner(com.datastax.driver.core.ResultSet resultSet, boolean returnNullStrings) throws SQLException {
        try {
            CassandraResultSet cassandraResultSet = new CassandraResultSet(this, resultSet, returnNullStrings);
            if (!cassandraResultSet.isQuery()) {
                this.result = null;
                return false;
            }
            this.result = cassandraResultSet;
            return true;
        } catch (SyntaxError ex) {
            throw new SQLSyntaxErrorException(ex.getMessage(), ex);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        }
    }

    @Override
    public boolean getMoreResults() {
        // todo
        return false;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return -1;
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (batchStatement == null) throw new SQLException("No batch statements were submitted");
        int statementsCount = batchStatement.size();
        try {
            session.execute(batchStatement);
        } catch (Throwable t) {
            throw new SQLException(t.getMessage(), t);
        } finally {
            batchStatement = null;
        }
        int[] res = new int[statementsCount];
        for (int i = 0; i < statementsCount; i++) {
            res[i] = SUCCESS_NO_INFO;
        }
        return res;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxRows(int max) {
        // todo
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cassandra provides no support for interrupting an operation.");
    }

    @Override
    public SQLWarning getWarnings() {
        return null; // todo
    }

    @Override
    public void clearWarnings() {
        // todo
    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        checkClosed();
        // Driver doesn't support positioned updates for now, so no-op.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {
        // todo
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
