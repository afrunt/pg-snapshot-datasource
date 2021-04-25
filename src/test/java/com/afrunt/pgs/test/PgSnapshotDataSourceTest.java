/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.afrunt.pgs.test;

import com.afrunt.pgs.PgSnapshot;
import com.afrunt.pgs.PgSnapshotDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Andrii Frunt
 */
public class PgSnapshotDataSourceTest {
    @Test
    public void testSnapshotCreateRestore() throws Exception {
        PgSnapshotDataSource dataSource = new PgSnapshotDataSource();

        PgSnapshot initialState = dataSource.createSnapshot("Initial state");

        JdbcTemplate jdbc = new JdbcTemplate(dataSource);

        jdbc.execute("CREATE TABLE dummy(id bigint PRIMARY KEY)");

        for (int i = 0; i < 10; i++) {
            final var id = i;
            jdbc.update("INSERT INTO dummy (id) VALUES (?)", ps -> ps.setInt(1, id));
        }

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM dummy", Integer.class)).isEqualTo(10);

        PgSnapshot mutatedState = dataSource.createSnapshot("Mutated state");

        dataSource.restoreFromSnapshot(initialState);

        assertThatThrownBy(() -> jdbc.queryForObject("SELECT COUNT(*) FROM dummy", Integer.class)).isInstanceOf(BadSqlGrammarException.class);

        dataSource.restoreFromSnapshot(mutatedState);

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM dummy", Integer.class)).isEqualTo(10);

        dataSource.close();
    }
}
