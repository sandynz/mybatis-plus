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
import java.time.LocalDateTime;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisXMLLanguageDriver;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.toolkit.support.Range;
import com.baomidou.mybatisplus.extension.plugins.DmlRowCountPredictionInterceptor;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.baomidou.mybatisplus.test.base.entity.CommonData;
import com.baomidou.mybatisplus.test.base.mapper.commons.CommonDataMapper;
import com.baomidou.mybatisplus.test.mysql.config.DBConfig;
import com.baomidou.mybatisplus.test.toolkit.JdbcUtils;

/**
 * DML操作记录数预言拦截器测试，MySQL、JDBC 模式
 *
 * @author sandynz
 */
class DmlRowCountPredictionMysqlJdbcTest {

    private static final Logger logger = LoggerFactory.getLogger(DmlRowCountPredictionMysqlJdbcTest.class);

    // 非Spring环境测试；数据库连接池测试；
    @Test
    void expectedDmlRowCountTest() throws Exception {
        DBConfig dbConfig = new DBConfig();
        DataSource dataSource = dbConfig.dataSource();
        MybatisSqlSessionFactoryBean factoryBean = new MybatisSqlSessionFactoryBean();
        factoryBean.setDataSource(dataSource);
        //factoryBean.setVfs(SpringBootVFS.class);
        factoryBean.setTypeAliasesPackage("com.baomidou.mybatisplus.test.mysql.entity");

        Resource[] mapperResources = new PathMatchingResourcePatternResolver().getResources("classpath*:mapper/**/*.xml");
        factoryBean.setMapperLocations(mapperResources);

        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setDefaultScriptingLanguage(MybatisXMLLanguageDriver.class);
        configuration.setJdbcTypeForNull(JdbcType.NULL);
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.addInterceptor(new DmlRowCountPredictionInterceptor());
        factoryBean.setConfiguration(configuration);
        // @MapperScan({"com.baomidou.mybatisplus.test.base.mapper.children", "com.baomidou.mybatisplus.test.base.mapper.commons", "com.baomidou.mybatisplus.test.mysql.mapper"})
        configuration.addMappers("com.baomidou.mybatisplus.test.base.mapper.children");
        configuration.addMappers("com.baomidou.mybatisplus.test.base.mapper.commons");
        configuration.addMappers("com.baomidou.mybatisplus.test.mysql.mapper");

        SqlSessionFactory sqlSessionFactory = factoryBean.getObject();
        SqlSession sqlSession = sqlSessionFactory.openSession(true);
        CommonDataMapper commonDataMapper = sqlSession.getMapper(CommonDataMapper.class);

        // 用于校验数据的新连接
        Connection validateConnection = JdbcUtils.getNewConnection(DbType.MYSQL, dbConfig.getJdbcConnCfg());

        String str = "prediction1", strNew = "prediction1New";
        commonDataMapper.delete(
                new QueryWrapper<CommonData>().lambda().in(CommonData::getTestStr, str, strNew)
        );
        final long id1 = 1;
        commonDataMapper.insert(new CommonData().setTestInt(1).setTestStr(str).setId(id1));
        Assertions.assertEquals(1, JdbcUtils.selectCount(validateConnection, "select count(1) from common_data where id=" + id1));

        int updateCount = commonDataMapper.update(new CommonData(),
                new UpdateWrapper<CommonData>().lambda().setExpectedDmlRowCount(Range.is(1))
                        .eq(CommonData::getId, id1)
                        .eq(CommonData::getTestStr, str)
                        .set(CommonData::getTestStr, strNew)
                        .set(CommonData::getUpdateDatetime, LocalDateTime.now())
        );
        Assertions.assertEquals(1, updateCount);
        Map<String, Object> commonData = JdbcUtils.selectList(validateConnection, "select * from common_data where id=" + id1);
        Assertions.assertEquals(strNew, commonData.get("test_str"));

        try {
            updateCount = commonDataMapper.update(new CommonData(),
                    new UpdateWrapper<CommonData>().lambda().setExpectedDmlRowCount(Range.is(100))
                            .eq(CommonData::getTestStr, strNew)
                            .set(CommonData::getTestStr, str)
                            .set(CommonData::getUpdateDatetime, LocalDateTime.now())
            );
            Assertions.assertEquals(1, updateCount);
        } catch (Throwable e) {
            logger.info("expected ex, exClassName={}, exMsg={}", e.getClass().getName(), e.getMessage());
        }

        validateConnection.close();
        sqlSession.close();
    }

}
