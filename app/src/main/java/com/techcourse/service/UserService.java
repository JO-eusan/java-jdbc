package com.techcourse.service;

import com.interface21.dao.DataAccessException;
import com.techcourse.config.DataSourceConfig;
import com.techcourse.dao.UserDao;
import com.techcourse.dao.UserHistoryDao;
import com.techcourse.domain.User;
import com.techcourse.domain.UserHistory;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

public class UserService {

    private final UserDao userDao;
    private final UserHistoryDao userHistoryDao;
    private final DataSource dataSource;

    public UserService(final UserDao userDao, final UserHistoryDao userHistoryDao) {
        this.userDao = userDao;
        this.userHistoryDao = userHistoryDao;
        this.dataSource = DataSourceConfig.getInstance();
    }

    public User findById(final long id) {
        return userDao.findById(id);
    }

    public void insert(User user) {
        executeTransaction(connection -> userDao.insert(connection, user));
    }

    public void changePassword(long id, String newPassword, String createBy) {
        executeTransaction(connection -> {
            User user = findById(id);
            user.changePassword(newPassword);
            userDao.update(connection, user);
            userHistoryDao.log(connection, new UserHistory(user, createBy));
        });
    }

    private void executeTransaction(TransactionOperation operation) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                operation.execute(connection);
                connection.commit();
            } catch (Exception e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackEx) {
                    throw new DataAccessException("Rollback failed", rollbackEx);
                }
                throw new DataAccessException("Transaction failed", e);
            }
        } catch (SQLException e) {
            throw new DataAccessException("Connection error", e);
        }
    }

    @FunctionalInterface
    private interface TransactionOperation {
        void execute(Connection connection) throws Exception;
    }
}
