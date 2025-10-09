package com.interface21.jdbc.core;

import com.interface21.dao.DataAccessException;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class BeanPropertyRowMapper<T> implements RowMapper<T> {

    private final Class<T> type;

    public BeanPropertyRowMapper(Class<T> type) {
        this.type = type;
    }

    @Override
    public T mapRow(ResultSet rs) throws SQLException {
        try {
            Constructor<?> constructor = findAllArgumentConstructor();
            Object[] args = extractConstructorArgs(rs, constructor);
            return instantiate(constructor, args);
        } catch (Exception e) {
            throw new DataAccessException("Failed to map row to " + type.getSimpleName(), e);
        }
    }

    private Constructor<?> findAllArgumentConstructor() {
        Constructor<?> target = null;
        int maxParams = -1;
        for (Constructor<?> c : type.getDeclaredConstructors()) {
            if (c.getParameterCount() > maxParams) {
                target = c;
                maxParams = c.getParameterCount();
            }
        }
        return target;
    }

    private Object[] extractConstructorArgs(ResultSet rs, Constructor<?> constructor) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        int paramCount = constructor.getParameterCount();
        Object[] args = new Object[Math.min(columnCount, paramCount)];
        for (int i = 0; i < args.length; i++) {
            args[i] = rs.getObject(i + 1);
        }
        return args;
    }

    @SuppressWarnings("unchecked")
    private T instantiate(Constructor<?> constructor, Object[] args) throws Exception {
        constructor.setAccessible(true);
        return (T) constructor.newInstance(args);
    }
}
