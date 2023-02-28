package com.bigdata.dis.sdk.demo.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloud.dis.DIS;
import com.cloud.dis.iface.stream.request.UpdatePartitionCountRequest;
import com.cloud.dis.iface.stream.response.UpdatePartitionCountResult;

/**
 * Update Partition Count Example
 */
public class UpdatePartitionCount
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdatePartitionCount.class);
    
    public static void main(String[] args)
    {
        DIS dic = DISUtil.getInstance();
        String streamName = DISUtil.getStreamName();
        // 目标分区数量
        int targetPartitionCount = 2;
        
        UpdatePartitionCountRequest update = new UpdatePartitionCountRequest();
        update.setStreamName(streamName);
        update.setTargetPartitionCount(targetPartitionCount);
        try
        {
            UpdatePartitionCountResult updatePartitionCountResult = dic.updatePartitionCount(update);
            LOGGER.info("Success to update partition count, {}", updatePartitionCountResult);
        }
        catch (Exception e)
        {
            LOGGER.error("Failed to update partition count", e);
        }
    }
}
