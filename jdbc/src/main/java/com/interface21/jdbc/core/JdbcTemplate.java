package com.interface21.jdbc.core;

import com.interface21.dao.DataAccessException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(JdbcTemplate.class);

    private final DataSource dataSource;

    public JdbcTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void update(Connection connection, String sql, Object... parameters) {
        update(connection, sql, bindParameters(parameters));
    }

    public void update(Connection connection, String sql, PreparedStatementSetter setter) {
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            log.debug("query : {}", sql);
            setter.setValues(pstmt);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new DataAccessException(e);
        }
    }

    public <T> List<T> query(String sql, RowMapper<T> mapper, Object... parameters) {
        return query(sql, bindParameters(parameters), mapper);
    }

    public <T> List<T> query(String sql, PreparedStatementSetter setter, RowMapper<T> mapper) {
        try (Connection con = dataSource.getConnection();
             PreparedStatement pstmt = con.prepareStatement(sql)) {

            log.debug("query : {}", sql);
            setter.setValues(pstmt);

            try (ResultSet rs = pstmt.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapper.mapRow(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new DataAccessException(e);
        }
    }

    public <T> T queryForObject(String sql, RowMapper<T> mapper, Object... parameters) {
        List<T> results = query(sql, mapper, parameters);
        if (results.isEmpty()) {
            return null;
        }
        return results.getFirst();
    }

    private PreparedStatementSetter bindParameters(Object... parameters) {
        return pstmt -> {
            for (int i = 0; i < parameters.length; i++) {
                pstmt.setObject(i + 1, parameters[i]);
            }
        };
    }
}
