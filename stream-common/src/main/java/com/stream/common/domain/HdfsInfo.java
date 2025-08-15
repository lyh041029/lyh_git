package com.stream.common.domain;

/**
 * @BelongsProject: lyh_git
 * @BelongsPackage: com.stream.common.domain
 * @Author: liyuhuan
 * @CreateTime: 2025-08-15  09:43
 * @Description: TODO
 * @Version: 1.0
 */
public class HdfsInfo {
    private String hdfsUrl;
    private boolean hdfsNeedPartition;
    private int hdfsPartitionMode;
    private String hdfsPartitionField;

    public HdfsInfo() {
    }

    public HdfsInfo(String hdfsUrl, boolean hdfsNeedPartition, int hdfsPartitionMode, String hdfsPartitionField) {
        this.hdfsUrl = hdfsUrl;
        this.hdfsNeedPartition = hdfsNeedPartition;
        this.hdfsPartitionMode = hdfsPartitionMode;
        this.hdfsPartitionField = hdfsPartitionField;
    }

    public String getHdfsUrl() {
        return hdfsUrl;
    }

    public void setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    public boolean isHdfsNeedPartition() {
        return hdfsNeedPartition;
    }

    public void setHdfsNeedPartition(boolean hdfsNeedPartition) {
        this.hdfsNeedPartition = hdfsNeedPartition;
    }

    public int getHdfsPartitionMode() {
        return hdfsPartitionMode;
    }

    public void setHdfsPartitionMode(int hdfsPartitionMode) {
        this.hdfsPartitionMode = hdfsPartitionMode;
    }

    public String getHdfsPartitionField() {
        return hdfsPartitionField;
    }

    public void setHdfsPartitionField(String hdfsPartitionField) {
        this.hdfsPartitionField = hdfsPartitionField;
    }

    @Override
    public String toString() {
        return "HdfsInfo{" +
                "hdfsUrl='" + hdfsUrl + '\'' +
                ", hdfsNeedPartition=" + hdfsNeedPartition +
                ", hdfsPartitionMode=" + hdfsPartitionMode +
                ", hdfsPartitionField='" + hdfsPartitionField + '\'' +
                '}';
    }
}
