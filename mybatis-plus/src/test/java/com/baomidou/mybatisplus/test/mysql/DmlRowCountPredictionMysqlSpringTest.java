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
package com.baomidou.mybatisplus.test.mysql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.annotation.Resource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.support.TransactionTemplate;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.support.Range;
import com.baomidou.mybatisplus.test.base.entity.CommonData;
import com.baomidou.mybatisplus.test.base.enums.TestEnum;
import com.baomidou.mybatisplus.test.base.mapper.commons.CommonDataMapper;
import com.baomidou.mybatisplus.test.mysql.config.DBConfig;
import com.baomidou.mybatisplus.test.toolkit.JdbcUtils;

/**
 * DML操作记录数预言拦截器测试，MySQL、Spring 模式
 *
 * @author sandynz
 */
@DirtiesContext
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(locations = {"classpath:mysql/spring-test-mysql.xml"})
class DmlRowCountPredictionMysqlSpringTest {

    private static final Logger logger = LoggerFactory.getLogger(DmlRowCountPredictionMysqlSpringTest.class);

    @Resource
    private CommonDataMapper commonDataMapper;
    @Resource
    private TransactionTemplate transactionTemplate;

    @Test
    void expectedDmlRowCountTest() throws SQLException {
        DBConfig dbConfig = new DBConfig();
        // 用于校验数据的新连接
        Connection validateConnection = JdbcUtils.getNewConnection(DbType.MYSQL, dbConfig.getJdbcConnCfg());

        String str = "prediction1", strNew = str + "New";
        commonDataMapper.delete(
                new QueryWrapper<CommonData>().lambda().in(CommonData::getTestStr, str, strNew)
        );
        final long id1 = 1;
        commonDataMapper.insert(new CommonData().setTestInt(1).setTestStr(str).setId(id1).setTestEnum(TestEnum.ONE));
        Assertions.assertEquals(1, JdbcUtils.selectCount(validateConnection, "select count(1) from common_data where id=" + id1));

        int updateCount = commonDataMapper.update(new CommonData(),
                new UpdateWrapper<CommonData>().lambda().setExpectedDmlRowCount(Range.is(1))
                        .eq(CommonData::getId, id1)
                        .eq(CommonData::getTestStr, str)
                        .set(CommonData::getTestStr, strNew)
        );
        Assertions.assertEquals(1, updateCount);
        Map<String, Object> commonData = JdbcUtils.selectList(validateConnection, "select * from common_data where id=" + id1);
        Assertions.assertEquals(strNew, commonData.get("test_str"));

        try {
            commonDataMapper.update(new CommonData(),
                    new UpdateWrapper<CommonData>().lambda().setExpectedDmlRowCount(Range.is(100))
                            .eq(CommonData::getTestStr, strNew)
                            .set(CommonData::getTestStr, str)
            );
            Assertions.fail();
        } catch (DataAccessException e) {
            logger.info("expected ex, exClassName={}, exMsg={}", e.getClass().getName(), e.getMessage());
        }

        Boolean executeRet = transactionTemplate.execute(transactionStatus -> {
            int count = commonDataMapper.update(new CommonData(),
                    new UpdateWrapper<CommonData>().lambda().setExpectedDmlRowCount(Range.is(1))
                            .eq(CommonData::getId, id1)
                            .eq(CommonData::getTestStr, strNew)
                            .set(CommonData::getTestStr, str)
            );
            return count == 1;
        });
        Assertions.assertTrue(executeRet != null && executeRet);
        commonData = JdbcUtils.selectList(validateConnection, "select * from common_data where id=" + id1);
        Assertions.assertEquals(str, commonData.get("test_str"));

        try {
            transactionTemplate.execute(transactionStatus -> {
                commonDataMapper.update(new CommonData(),
                        new UpdateWrapper<CommonData>().lambda().setExpectedDmlRowCount(Range.is(100))
                                .eq(CommonData::getTestStr, str)
                                .set(CommonData::getTestStr, strNew)
                );
                return false;
            });
            Assertions.fail();
        } catch (DataAccessException e) {
            logger.info("expected ex, exClassName={}, exMsg={}", e.getClass().getName(), e.getMessage());
        }

        validateConnection.close();
    }

}
