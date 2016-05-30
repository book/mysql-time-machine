package com.booking.replication;

import com.booking.replication.util.StartupParameters;
import com.google.common.base.Joiner;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Stores configuration properties
 */
public class Configuration {

    /**
     * Constructor
     */
    public Configuration() {}

    private String          applierType;

    @JsonDeserialize
    private ReplicationSchema replication_schema;

    private static class ReplicationSchema implements Serializable {
        public String       name;
        public String       username;
        public String       password;
        public List<String> slaves;
        public int          port        = 3306;
        public int          server_id   = 1;
        public int          shard_id;
    }

    @JsonDeserialize
    private HBaseConfiguration hbase;

    private static class HBaseConfiguration {
        public String       namespace;
        public List<String> zookeeper_quorum;
        public boolean      writeRecentChangesToDeltaTables;


        @JsonDeserialize
        public HiveImports     hive_imports = new HiveImports();

        private static class HiveImports {
            public List<String> tables = Collections.<String>emptyList();
        }

    }

    @JsonDeserialize
    private MetadataStore metadata_store;

    private static class MetadataStore {
        public String       username;
        public String       password;
        public String       host;
        public String       database;
        public List<String> zookeeper_quorum;
    }

    @JsonDeserialize
    private GraphiteConfig graphite;

    private static class GraphiteConfig {
        public String       namespace;
        public String       url;
    }


    private boolean         initialSnapshotMode;
    private long            startingBinlogPosition;
    private String          startingBinlogFileName;
    private String          endingBinlogFileName;

    public void loadStartupParameters(StartupParameters startupParameters ) {

        applierType = startupParameters.getApplier();

        if(applierType == "hbase" && hbase == null) {
            throw new RuntimeException("HBase not configured");
        }

        // staring position
        startingBinlogFileName = startupParameters.getBinlogFileName();
        startingBinlogPosition = startupParameters.getBinlogPosition();
        endingBinlogFileName   = startupParameters.getLastBinlogFileName();

        replication_schema.shard_id = startupParameters.getShard();

        // delta tables
        hbase.writeRecentChangesToDeltaTables = startupParameters.isDeltaTables();

        // initial snapshot mode
        initialSnapshotMode = startupParameters.isInitialSnapshot();


        //Hbase namespace
        if (startupParameters.getHbaseNamespace() != null) {
            hbase.namespace = startupParameters.getHbaseNamespace();
        }
    }

    public String toString() {

        Joiner joiner = Joiner.on(", ");

        if (hbase.hive_imports.tables != null) {
            return new StringBuilder()
                    .append("\n")
                    .append("\tapplierType                       : ")
                    .append(applierType)
                    .append("\n")
                    .append("\tdeltaTables                       : ")
                    .append(hbase.writeRecentChangesToDeltaTables)
                    .append("\n")
                    .append("\treplicantSchemaName               : ")
                    .append(replication_schema.name)
                    .append("\n")
                    .append("\tuser name                         : ")
                    .append(replication_schema.username)
                    .append("\n")
                    .append("\treplicantDBSlaves                 : ")
                    .append(Joiner.on(" | ").join(replication_schema.slaves))
                    .append("\n")
                    .append("\treplicantDBActiveHost             : ")
                    .append(this.getActiveSchemaHost())
                    .append("\n")
                    .append("\tactiveSchemaUserName              : ")
                    .append(metadata_store.username)
                    .append("\n")
                    .append("\tactiveSchemaHost                  : ")
                    .append(metadata_store.host)
                    .append("\n")
                    .append("\tactiveSchemaDB                    : ")
                    .append(metadata_store.database)
                    .append("\n")
                    .append("\tgraphiteStatsNamesapce            : ")
                    .append(graphite.namespace)
                    .append("\n")
                    .append("\tgraphiteStatsUrl                  : ")
                    .append(graphite.url)
                    .append("\n")
                    .append("\tdeltaTables                       : ")
                    .append(hbase.writeRecentChangesToDeltaTables)
                    .append("\n")
                    .append("\tinitialSnapshotMode               : ")
                    .append(initialSnapshotMode)
                    .append("\n")
                    .append("\ttablesForWhichToTrackDailyChanges : ")
                    .append(joiner.join(hbase.hive_imports.tables))
                    .append("\n")
                    .toString();
        }
        else {
            return new StringBuilder()
                    .append("\n")
                    .append("\tapplierType                       : ")
                    .append(applierType)
                    .append("\n")
                    .append("\tdeltaTables                       : ")
                    .append(hbase.writeRecentChangesToDeltaTables)
                    .append("\n")
                    .append("\treplicantSchemaName               : ")
                    .append(replication_schema.name)
                    .append("\n")
                    .append("\tuser name                         : ")
                    .append(replication_schema.username)
                    .append("\n")
                    .append("\treplicantDBSlaves             : ")
                    .append(Joiner.on(" | ").join(replication_schema.slaves))
                    .append("\n")
                    .append("\treplicantDBActiveHost             : ")
                    .append(this.getActiveSchemaHost())
                    .append("\n")
                    .append("\tactiveSchemaUserName              : ")
                    .append(metadata_store.username)
                    .append("\n")
                    .append("\tgraphiteStatsNamesapce            : ")
                    .append(graphite.namespace)
                    .append("\n")
                    .append("\tdeltaTables                       : ")
                    .append(hbase.writeRecentChangesToDeltaTables)
                    .append("\n")
                    .append("\tinitialSnapshotMode               : ")
                    .append(initialSnapshotMode)
                    .append("\n")
                    .toString();
        }
    }

    public int getReplicantPort() {
        return replication_schema.port;
    }

    public int getReplicantDBServerID() {
        return replication_schema.server_id;
    }

    public long getStartingBinlogPosition() {
        return startingBinlogPosition;
    }

    public String getReplicantDBActiveHost() {
        return this.replication_schema.slaves.get(0);
    }

    public String getReplicantDBUserName() {
        return replication_schema.username;
    }

    public String getReplicantDBPassword() {
        return replication_schema.password;
    }

    public String getStartingBinlogFileName() {
        return startingBinlogFileName;
    }

    public String getLastBinlogFileName() {
        return endingBinlogFileName;
    }

    public String getReplicantSchemaName() {
        return replication_schema.name;
    }

    public String getApplierType() {
        return applierType;
    }

    public String getActiveSchemaDSN() {
        return String.format("jdbc:mysql://%s/%s", metadata_store.host, metadata_store.database);
    }

    public String getActiveSchemaHost() {
        return metadata_store.host;
    }

    public String getActiveSchemaUserName() {
        return metadata_store.username;
    }

    public String getActiveSchemaPassword() {
        return metadata_store.password;
    }

    public String getActiveSchemaDB() {
        return metadata_store.database;
    }

    public int getReplicantShardID() {
        return replication_schema.shard_id;
    }

    public String getHBaseQuorum() {
        return Joiner.on(",").join(hbase.zookeeper_quorum);
    }

    public String getGraphiteStatsNamesapce() {
        return graphite.namespace;
    }

    public String getGraphiteUrl() {
        return graphite.url;
    }

    public boolean isWriteRecentChangesToDeltaTables() {
        return hbase.writeRecentChangesToDeltaTables;
    }

    public List<String> getTablesForWhichToTrackDailyChanges() {
        return hbase.hive_imports.tables;
    }

    public boolean isInitialSnapshotMode() {
        return initialSnapshotMode;
    }

    public String getHbaseNamespace() {
        return hbase.namespace;
    }
}
