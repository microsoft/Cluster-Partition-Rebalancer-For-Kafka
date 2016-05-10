//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

/**
 * Created by Soumyajit Sahu on 4/11/2016.
 */
public interface IAdoptionLogic {
    Partition findLocalPartitionToGiveOut() throws Exception;

    Partition findRemotePartitionToTakeIn()throws Exception;

    void advertisePartitionForAdoption(Partition partitionToGiveOut)throws Exception;

    void adoptRemotePartition(Partition partitionToTakeIn)throws Exception;
}
