//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

/**
 * Created by Soumyajit Sahu on 4/11/2016.
 */
public class Partition {
    public int brokerId;
    public String topicName;
    public int partitionId;

    public Partition() {
    }

    public Partition(int brokerId, String topicName, int partitionId) {
        this.brokerId = brokerId;
        this.topicName = topicName;
        this.partitionId = partitionId;
    }

    public static Partition createPartition(String partitionStringWithBrokerId) {
        String[] split = partitionStringWithBrokerId.split("-");
        return new Partition(
                Integer.parseInt(split[0]),
                split[1],
                Integer.parseInt(split[2])
        );
    }

    public String getPartitionFullName() {
        return topicName + "-" + partitionId;
    }

    public String getPartitionFullNameWithBrokerId() {
        return "" + brokerId + "-" + getPartitionFullName();
    }
}
