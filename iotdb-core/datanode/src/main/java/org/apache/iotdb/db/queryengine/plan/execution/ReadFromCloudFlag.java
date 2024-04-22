package org.apache.iotdb.db.queryengine.plan.execution;

public class ReadFromCloudFlag {
    private static ReadFromCloudFlag instance;
    public boolean readFromCloudFlag;

    private ReadFromCloudFlag() {
        readFromCloudFlag= false;
    }
    public void setFlag(boolean value) {
        readFromCloudFlag = value;
    }

    public static ReadFromCloudFlag getInstance() {
        if (instance == null) {
            instance = new ReadFromCloudFlag();
        }
        return instance;
    }
}
