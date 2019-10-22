/*
 * Copyright (c) 2011-2019, hubin (jobob@qq.com).
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.baomidou.mybatisplus.test.toolkit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baomidou.mybatisplus.annotation.DbType;

/**
 * JDBC工具类。
 * 提供新的连接用于查询并验证其它操作方式的结果是否正确（之前的连接可能还处于事务中）。
 *
 * @author sandynz
 */
public class JdbcUtils {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);

    /**
     * 获取新连接。
     */
    public static Connection getNewConnection(DbType dbType, JdbcConnCfg jdbcConnCfg) throws SQLException {
        String url = jdbcConnCfg.getUrl();
        if (url == null) {
            url = String.format("jdbc:%s://%s:%d/%s", dbType.getDb(), jdbcConnCfg.getHost(), jdbcConnCfg.getPort(), jdbcConnCfg.getDatabase());
        }
        switch (dbType) {
            case MYSQL: {
                url += "?useSSL=false&useUnicode=true&characterEncoding=UTF-8";
                break;
            }
            default:
                break;
        }
        return DriverManager.getConnection(url, jdbcConnCfg.getUsername(), jdbcConnCfg.getPassword());
    }

    /**
     * 查询数据个数
     *
     * @param connection 仅用于查询，不会关闭连接
     * @param sql        查询数据个数的语句，e.g. SELECT COUNT(1) FROM table1 WHERE id<1001;
     */
    public static int selectCount(Connection connection, String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("sql null");
        }
        logger.info("selectCount, sql={}", sql);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next()) {
                    int count = resultSet.getInt(1);
                    logger.info("count={}", count);
                    return count;
                } else {
                    logger.info("count={}", 0);
                    return 0;
                }
            }
        }
    }

    /**
     * 查询单条数据
     *
     * @param connection 仅用于查询，不会关闭连接
     * @param sql        查询语句
     */
    public static Map<String, Object> selectOne(Connection connection, String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("sql null");
        }
        logger.info("selectList, sql={}", sql);
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                List<String> columnNameList = new ArrayList<>(metaData.getColumnCount());
                for (int column = 1, columnCount = metaData.getColumnCount(); column <= columnCount; column++) {
                    columnNameList.add(metaData.getColumnName(column));
                }
                if (resultSet.next()) {
                    Map<String, Object> map = new HashMap<>();
                    for (String columnName : columnNameList) {
                        map.put(columnName, resultSet.getObject(columnName));
                    }
                    logger.info("result map={}", map);
                    return map;
                } else {
                    logger.info("result empty");
                    return Collections.emptyMap();
                }
            }
        }
    }

}
