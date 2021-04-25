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
package com.afrunt.pgs;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.attribute.PosixFilePermission.*;

/**
 * @author Andrii Frunt
 */
public class PgSnapshotDataSource implements DataSource, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgSnapshotDataSource.class);
    private volatile DataSource dataSource;
    private volatile EmbeddedPostgres pg;
    private Path currentDataDirectory;
    private final List<Consumer<DataSource>> dataSourceMutations = new CopyOnWriteArrayList<>();
    private final Map<EmbeddedPostgres, List<Connection>> connectionsToDatabase = new ConcurrentHashMap<>();
    private Integer port;

    public PgSnapshotDataSource() {
        this(null);
    }

    public PgSnapshotDataSource(Integer port) {
        this.port = port;
        initDatabase(createSnapshotDirectory());
    }

    public synchronized PgSnapshot createSnapshot(String name) {
        long start = System.currentTimeMillis();
        PgSnapshot snapshot = new PgSnapshot(name, createSnapshot());
        LOGGER.debug("Snapshot '{}' created. {}ms. {}", snapshot.getName(), System.currentTimeMillis() - start, snapshot.getPath());
        return snapshot;
    }

    public synchronized void restoreFromSnapshot(PgSnapshot snapshot) {
        long start = System.currentTimeMillis();
        restoreFromSnapshot(snapshot.getPath());
        LOGGER.debug("Database restored from snapshot '{}'. {}ms. {}", snapshot.getName(), System.currentTimeMillis() - start, snapshot.getPath());
    }

    @Override
    public synchronized Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        storeConnection(connection);
        handleOutdatedDatabases();
        return connection;
    }

    @Override
    public synchronized Connection getConnection(String username, String password) throws SQLException {
        Connection connection = dataSource.getConnection(username, password);
        storeConnection(connection);
        handleOutdatedDatabases();
        return connection;
    }

    private void storeConnection(Connection connection) {
        List<Connection> connections = connectionsToDatabase.getOrDefault(pg, new ArrayList<>());
        connections.add(connection);
        connectionsToDatabase.put(pg, connections);
    }

    private void handleOutdatedDatabases() {
        connectionsToDatabase.replaceAll((db, connections) -> onlyOpenConnections(connections));

        List<EmbeddedPostgres> databasesToStop = connectionsToDatabase.entrySet().stream()
                .filter(e -> e.getKey() != pg)
                .filter(e -> e.getValue().isEmpty())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        if (!databasesToStop.isEmpty()) {
            LOGGER.debug("{} databases to stop", databasesToStop.size());
            databasesToStop.forEach(this::stopDatabase);
            databasesToStop.forEach(connectionsToDatabase::remove);
        }
    }

    private List<Connection> onlyOpenConnections(List<Connection> connections) {
        return connections.stream()
                .filter(this::connectionIsOpen)
                .collect(Collectors.toList());
    }

    private boolean connectionIsOpen(Connection connection) {
        return !connectionIsClosed(connection);
    }

    private boolean connectionIsClosed(Connection connection) {
        try {
            return connection.isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void stopDatabase(EmbeddedPostgres database) {
        try {
            LOGGER.debug("Stopping database {}", database);
            database.close();
            LOGGER.debug("Database {} stopped", database);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSourceMutations.add(ds -> {
            try {
                ds.setLogWriter(out);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSourceMutations.add(ds -> {
            try {
                ds.setLoginTimeout(seconds);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    @Override
    public ConnectionBuilder createConnectionBuilder() throws SQLException {
        return dataSource.createConnectionBuilder();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return dataSource.getParentLogger();
    }

    @Override
    public ShardingKeyBuilder createShardingKeyBuilder() throws SQLException {
        return dataSource.createShardingKeyBuilder();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return dataSource.isWrapperFor(iface);
    }

    @Override
    public void close() throws Exception {
        if (pg != null) {
            connectionsToDatabase.keySet().forEach(this::stopDatabase);
        }
    }

    private void initDatabase(Path path) {
        try {
            long start = System.currentTimeMillis();
            currentDataDirectory = path;
            EmbeddedPostgres.Builder builder = EmbeddedPostgres.builder();

            if (port != null) {
                builder = builder.setPort(port);
            }

            pg = builder
                    .setDataDirectory(currentDataDirectory)
                    .setCleanDataDirectory(false)
                    .start();

            dataSource = pg.getPostgresDatabase();
            dataSourceMutations.forEach(m -> m.accept(dataSource));
            connectionsToDatabase.put(pg, new ArrayList<>());
            LOGGER.debug("Database {} started. {}ms", pg, System.currentTimeMillis() - start);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path createSnapshot() {
        Path snapshotDirectory = createSnapshotDirectory();
        flushPgDataToFileSystem();
        return copyDataDirectory(currentDataDirectory, snapshotDirectory);
    }

    private void flushPgDataToFileSystem() {
        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().execute("CHECKPOINT");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void restoreFromSnapshot(Path snapshotPath) {
        Path snapshotDirectory = copyDataDirectory(snapshotPath, createSnapshotDirectory());
        initDatabase(snapshotDirectory);
    }

    private Path createSnapshotDirectory() {
        return deleteDirectoryOnShutdown(
                createDirectories(randomNonExistingTempDirectoryPath())
        );
    }

    private boolean deleteDirectory(Path path) {
        return deleteDirectory(path.toFile());
    }

    private Path copyDataDirectory(Path source, Path target) {
        LOGGER.debug("Copying data directories {} -> {}", source, target);

        filesToCopyFromDataDirectory(source)
                .forEach(p -> copyFileFromSourceDataDirectoryToTarget(source, target, p));

        return target;
    }

    private List<Path> filesToCopyFromDataDirectory(Path source) {
        try {
            return Files.walk(source)
                    .skip(1)
                    .filter(p -> !p.endsWith("epg-lock") && !p.endsWith("postmaster.pid"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path copyFileFromSourceDataDirectoryToTarget(Path sourceDir, Path targetDir, Path sourceFile) {
        try {
            Path targetFile = targetDir.resolve(sourceDir.relativize(sourceFile));
            LOGGER.trace("{} -> {}", sourceFile, targetFile);
            if (sourceFile.toFile().isDirectory()) {
                return createDirectories(targetFile);
            } else {
                return Files.copy(sourceFile, targetFile, REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private Path randomNonExistingTempDirectoryPath() {
        return randomNonExistingDirectoryPath(Path.of(System.getProperty("java.io.tmpdir")));
    }

    private Path randomNonExistingDirectoryPath(Path root) {
        Path path;
        do {
            path = root.resolve(UUID.randomUUID().toString());
        } while (path.toFile().exists());

        return path;
    }

    private Path createDirectories(Path path) {
        try {
            return fixPosixPermissions(Files.createDirectories(path));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory " + path.toAbsolutePath(), e);
        }
    }

    private Path deleteDirectoryOnShutdown(Path pathToDelete) {
        final Runnable deleteAction = () -> {
            final boolean success = deleteDirectory(pathToDelete);
            LOGGER.debug("Temporary directory {} deleted {}", pathToDelete.toAbsolutePath(), success);
        };

        Runtime.getRuntime().addShutdownHook(new Thread(deleteAction));

        return pathToDelete;
    }

    private Path fixPosixPermissions(Path path) throws IOException {
        if (!isWindows()) {
            Set<PosixFilePermission> perms = Files.readAttributes(path, PosixFileAttributes.class).permissions();

            perms.addAll(Set.of(OWNER_WRITE, OWNER_READ, OWNER_EXECUTE, GROUP_READ, GROUP_EXECUTE));
            perms.removeAll(Set.of(GROUP_WRITE, OTHERS_WRITE, OTHERS_READ, OTHERS_EXECUTE));

            Files.setPosixFilePermissions(path, perms);
            LOGGER.trace("Dir {} created {}", path, PosixFilePermissions.toString(perms));
        }

        return path;
    }

    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("windows");
    }
}
