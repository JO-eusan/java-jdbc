package com.interface21.jdbc.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(JdbcTemplate.class);

    private final DataSource dataSource;

    public JdbcTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void update(String sql, Object... parameters) {
        update(sql, pstmt -> {
            try {
                for (int i = 0; i < parameters.length; i++) {
                    pstmt.setObject(i + 1, parameters[i]);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void update(String sql, Consumer<PreparedStatement> parameterSetter) {
        try (Connection con = dataSource.getConnection();
             PreparedStatement pstmt = con.prepareStatement(sql)) {

            log.debug("query : {}", sql);
            parameterSetter.accept(pstmt);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public <T> List<T> query(String sql, Function<ResultSet, T> rowMapper, Object... parameters) {
        return query(sql, pstmt -> {
            try {
                for (int i = 0; i < parameters.length; i++) {
                    pstmt.setObject(i + 1, parameters[i]);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, rowMapper);
    }

    public <T> List<T> query(String sql, Consumer<PreparedStatement> parameterSetter,
                             Function<ResultSet, T> rowMapper) {
        try (Connection con = dataSource.getConnection();
             PreparedStatement pstmt = con.prepareStatement(sql)) {

            log.debug("query : {}", sql);
            parameterSetter.accept(pstmt);

            try (final var rs = pstmt.executeQuery()) {
                final var results = new ArrayList<T>();
                while (rs.next()) {
                    results.add(rowMapper.apply(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public <T> T queryForObject(String sql, Consumer<PreparedStatement> parameterSetter,
                                Function<ResultSet, T> rowMapper) {
        var results = query(sql, parameterSetter, rowMapper);
        if (results.isEmpty()) {
            return null;
        }
        return results.getFirst();
    }

    public <T> T queryForObject(String sql, Function<ResultSet, T> rowMapper, Object... parameters) {
        var results = query(sql, rowMapper, parameters);
        if (results.isEmpty()) {
            return null;
        }
        return results.getFirst();
    }
}
