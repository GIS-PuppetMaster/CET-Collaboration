package org.apache.iotdb.db.queryengine.plan.execution;

public class ScanStatusInfo{
    private int sourceId=0;
    private int edgeFragmentId=0;
    private int cloudFragmentId=0;
    private boolean status=false;//是否传输数据
    private int offset=0;//算子偏移量
    private boolean setOffset=false;

    ScanStatusInfo(int sourceId,int edgeFragmentId){
        this.edgeFragmentId=edgeFragmentId;
        this.sourceId=sourceId;
    }

    public void setEdgeFragmentId(int edgeFragmentId) {
        this.edgeFragmentId = edgeFragmentId;
    }

    public void setCloudFragmentId(int cloudFragmentId) {
        this.cloudFragmentId = cloudFragmentId;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getEdgeFragmentId() {
        return edgeFragmentId;
    }

    public int getCloudFragmentId() {
        return cloudFragmentId;
    }

    public boolean isStatus() {
        return status;
    }

    public int getOffset() {
        return offset;
    }

    public boolean isSetOffset() {
        return setOffset;
    }

    public void setSetOffset(boolean setOffset) {
        this.setOffset = setOffset;
    }

}
