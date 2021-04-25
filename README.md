[![Build Status](https://travis-ci.org/afrunt/pg-snapshot-datasource.svg?branch=main)](https://travis-ci.org/afrunt/pg-snapshot-datasource)
## Java library that helps you to create simple embedded postgres database and make the snapshots
Add `pg-snapshot-datasource` to your project. For maven projects just add this dependency:
```xml
<dependency>
    <groupId>com.afrunt.pgs</groupId>
    <artifactId>pg-snapshot-datasource</artifactId>
    <version>0.1</version>
</dependency>
```
  
### Usage 
```java
package com.afrunt.pgs.test;

import com.afrunt.pgs.PgSnapshot;
import com.afrunt.pgs.PgSnapshotDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
```