package org.apache.hudi.sink.index;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

public class HoodieIndexMessage {

    private final String recordKey;
    private final HoodieRecordGlobalLocation index;

    public HoodieIndexMessage(String recordKey, HoodieRecordGlobalLocation index) {
        this.recordKey = recordKey;
        this.index = index;
    }

    public HoodieRecordGlobalLocation getIndex() {
        return index;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public boolean isIndex() {
        return index != null;
    }
}
