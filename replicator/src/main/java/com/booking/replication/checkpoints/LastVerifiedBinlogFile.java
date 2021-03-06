package com.booking.replication.checkpoints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by bosko on 5/30/16.
 */
public class LastVerifiedBinlogFile implements SafeCheckPoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(LastVerifiedBinlogFile.class);

    private final int checkpointType = SafeCheckpointType.BINLOG_FILENAME;

    private String lastVerifiedBinlogFileName;
    private long lastVerifiedBinlogPosition = 4L;

    private int slaveId;

    public LastVerifiedBinlogFile() {}

    public LastVerifiedBinlogFile(int slaveId, String binlogFileName) {
        this(slaveId, binlogFileName, 4L);
    }

    /**
     * Represents the last processed binlog file.
     *
     * @param slaveId           Id of the slave that originated the binlog.
     * @param binlogFileName    File name
     * @param binlogPosition    File position
     */
    public LastVerifiedBinlogFile(int slaveId, String binlogFileName, long binlogPosition) {
        this.slaveId = slaveId;
        lastVerifiedBinlogFileName = binlogFileName;
        lastVerifiedBinlogPosition = binlogPosition;
    }

    @Override
    public int getCheckpointType() {
        return this.checkpointType;
    }

    public int getSlaveId() {
        return slaveId;
    }

    public void setSafeCheckPointPosition( long lastVerifiedBinlogPosition) {
        this.lastVerifiedBinlogPosition = lastVerifiedBinlogPosition;
    }

    public Long getSafeCheckPointPosition() {
        return  lastVerifiedBinlogPosition;
    }

    @Override
    public String getSafeCheckPointMarker() {
        return lastVerifiedBinlogFileName;
    }

    @Override
    public void setSafeCheckPointMarker(String marker) {
        lastVerifiedBinlogFileName = marker;
        LOGGER.info("SafeCheckPoint marter set to: " + lastVerifiedBinlogFileName);
    }

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String toJson() {
        String json = null;
        try {
            json = mapper.writeValueAsString(this);
        } catch (IOException e) {
            LOGGER.error("ERROR: could not serialize event", e);
        }
        return json;
    }
}
