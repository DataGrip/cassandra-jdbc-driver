package com.dbschema;

import com.datastax.driver.core.ConsistencyLevel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.CASE_INSENSITIVE;

public interface StatementExecutor {
  ExecutionResult execute(CassandraConnection connection, String sql) throws SQLException;

  class ExecutionResult {
    final ResultSet resultSet;

    public ExecutionResult(ResultSet resultSet) {
      this.resultSet = resultSet;
    }
  }

  class SetConsistencyLevelExecutor implements StatementExecutor {
    public static final SetConsistencyLevelExecutor INSTANCE = new SetConsistencyLevelExecutor();
    private static final Pattern PATTERN = Pattern.compile("CONSISTENCY (\\w+)", CASE_INSENSITIVE);

    @Override
    public ExecutionResult execute(CassandraConnection connection, String sql) throws SQLException {
      Matcher matcher = PATTERN.matcher(sql.trim());
      if (!matcher.matches()) return null;
      String level = matcher.group(1);
      try {
        connection.setConsistencyLevel(ConsistencyLevel.valueOf(level.toUpperCase(Locale.ENGLISH)));
      }
      catch (IllegalArgumentException e) {
        throw new SQLException(e);
      }
      return new ExecutionResult(null);
    }
  }

  class GetConsistencyLevelExecutor implements StatementExecutor {
    public static final GetConsistencyLevelExecutor INSTANCE = new GetConsistencyLevelExecutor();
    private static final Pattern PATTERN = Pattern.compile("CONSISTENCY", CASE_INSENSITIVE);

    @Override
    public ExecutionResult execute(CassandraConnection connection, String sql) throws SQLException {
      Matcher matcher = PATTERN.matcher(sql.trim());
      return matcher.matches() ? new ExecutionResult(new ListResultSet(connection.getConsistencyLevel().name(), "consistency_level")) : null;
    }
  }
}
