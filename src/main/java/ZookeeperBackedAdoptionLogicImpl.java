//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

/**
 * Created by Soumyajit Sahu on 4/11/2016.
 */

import kafka.admin.ReassignPartitionsCommand;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ZookeeperBackedAdoptionLogicImpl implements IAdoptionLogic, Runnable {
    private Logger logger = LoggerFactory.getLogger(ZookeeperBackedAdoptionLogicImpl.class);

    public static final String BROKERS_ROOT_ZNODE = "/brokers";
    public static final String ADOPTION_ADS_ROOT_ZNODE = "/siphon/adoptionAds";
    public static final String ADOPTION_ADS_LOCK_ROOT_ZNODE = ADOPTION_ADS_ROOT_ZNODE + "/lock";

    private static final String AD_POSTING_TEXT = "ACTIVE";
    private static final String AD_ADOPTION_COMPLETE_TEXT = "INACTIVE";
    private static final String ENABLE_ADOPTION_TEXT = "ENABLE";

    private static final long AD_EXPIRY_MS = 20 * 60 * 1000;

    private Random rand;

    private Properties props;

    private CuratorFramework client;

    private long sleepIntervalInMs;

    public ZookeeperBackedAdoptionLogicImpl() throws Exception {
        rand = new Random(System.currentTimeMillis());

        props = new Properties();
        props.load(new FileReader("settings.properties"));

        String zkConnString = Utils.getZookeeperConnectionString(2181);
        logger.info("Zookeeper connection string is {}", zkConnString);

        client = CuratorFrameworkFactory.newClient(
                zkConnString,
                new ExponentialBackoffRetry(1000, 30)
        );
        client.start();
        try {
            client.create().creatingParentsIfNeeded().forPath(ADOPTION_ADS_ROOT_ZNODE);
            client.create().creatingParentsIfNeeded().forPath(ADOPTION_ADS_LOCK_ROOT_ZNODE);
        }
        catch(KeeperException.NodeExistsException nodeExistsEx) {
            logger.debug("Ignoring Node Exists Exception for {}", ADOPTION_ADS_ROOT_ZNODE);
        }

        sleepIntervalInMs = Long.parseLong(props.getProperty("sleep.interval.ms", "120000"));
    }

    @Override
    protected void finalize() throws Throwable {
        if(client != null) {
            client.close();
        }
        super.finalize();
    }

    public void run() {
        while(true) {
            try {
                if(!isAdoptionEnabled()) {
                    logger.debug("Adoption isn't enabled. Will sleep for sometime");
                    Thread.sleep(5 * 60000); //sleep 5 minutes
                    continue;
                }

                Partition partitionToGiveOut;
                Partition partitionToTakeIn;

                //eager to give out only 50% of the time
                double randomFlip = rand.nextDouble();

                if(randomFlip < .50) {
                    if ((partitionToGiveOut = findLocalPartitionToGiveOut()) != null) {
                        logger.debug("Going to advertise partition {}", partitionToGiveOut.getPartitionFullNameWithBrokerId());
                        advertisePartitionForAdoption(partitionToGiveOut);
                    } else {
                        logger.debug("No partitions to give out for adoption");
                    }

                    if ((partitionToTakeIn = findRemotePartitionToTakeIn()) != null) {
                        logger.debug("Going to adopt partition {}", partitionToTakeIn.getPartitionFullNameWithBrokerId());
                        adoptRemotePartition(partitionToTakeIn);
                    } else {
                        logger.debug("No partitions to take in for adoption");
                    }
                }
                else {
                    if ((partitionToTakeIn = findRemotePartitionToTakeIn()) != null) {
                        logger.debug("Going to adopt partition {}", partitionToTakeIn.getPartitionFullNameWithBrokerId());
                        adoptRemotePartition(partitionToTakeIn);
                    } else {
                        logger.debug("No partitions to take in for adoption");
                    }

                    if ((partitionToGiveOut = findLocalPartitionToGiveOut()) != null) {
                        logger.debug("Going to advertise partition {}", partitionToGiveOut.getPartitionFullNameWithBrokerId());
                        advertisePartitionForAdoption(partitionToGiveOut);
                    } else {
                        logger.debug("No partitions to give out for adoption");
                    }
                }
            }
            catch(Exception ex) {
                logger.error("Exception in run(): " + ex);
                ex.printStackTrace(System.out);
                return;
            }

            try {
                long sleepMillis = sleepIntervalInMs + rand.nextInt(60000);
                logger.debug("Sleeping for {} ms", sleepMillis);
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                logger.debug("Thread sleep interrupted: " + e);
                return;
            }
        }
    }

    private boolean isAdoptionEnabled() throws Exception {
        return new String(client.getData().forPath(ADOPTION_ADS_ROOT_ZNODE)).equals(ENABLE_ADOPTION_TEXT);
    }

    public Partition findLocalPartitionToGiveOut() throws Exception {
        logger.debug("Finding Partition to Give out");
        List<String> topics = Utils.getTopics(client);
        for (String topic : topics) {
            int averageDirectoryCountPerBroker = (int)Math.ceil((double) (Utils.getPartitionCount(client, topic) * Utils.getReplicationFactor(client, topic)) / (double)Utils.getBrokerCount());
            logger.debug("Average Directory count per Broker for topic {}: {}", topic, averageDirectoryCountPerBroker);
            String zNodeDataForTopic = Utils.getDataFromTopicZNode(client, topic);
            String localBrokerIdStr = "" + Utils.getLocalBrokerId();
            int directoryCountOnLocalBroker = Utils.getSubStringOccurrenceCount(zNodeDataForTopic, localBrokerIdStr);
            logger.debug("Directory count on local broker for topic {} is {}", topic, directoryCountOnLocalBroker);
            if(directoryCountOnLocalBroker > averageDirectoryCountPerBroker) {
                logger.debug("Topic {} has directoryCountOnLocalBroker > averageDirectoryCountPerBroker", topic);
                Partition retPartition = new Partition(
                        Integer.parseInt(localBrokerIdStr),
                        topic,
                        Utils.getAnyPartitionNumberForBrokerAndTopic(zNodeDataForTopic, localBrokerIdStr)
                );
                logger.debug("Partition to Give out: {}", retPartition.getPartitionFullName());
                return retPartition;
            }
        }
        logger.debug("Partition to Give out: none");
        return null;
    }

    public Partition findRemotePartitionToTakeIn() throws Exception {
        logger.debug("Finding Partition to Take in");
        //get the adoption ads and create Partition objects out of them
        List<String> adoptionAds = Utils.getAdoptionAds(client);
        logger.debug("Total Ads discovered = {}", adoptionAds.size());

        HashMap<String, Partition> adoptionAdsTopicsPartitionsMap = new HashMap<String, Partition>();
        for (String adoptionAd : adoptionAds) {
            try {
                Partition tmpPartition = Partition.createPartition(adoptionAd);
                //note that this will overwrite any previous partitions for this topic, but that is ok
                adoptionAdsTopicsPartitionsMap.put(tmpPartition.topicName, tmpPartition);
            }catch(Exception ex) {
                logger.debug("Ad {} seems to be of invalid structure. Ignoring.", adoptionAd);
            }
        }

        List<String> topics = Utils.getTopics(client);
        for (String topic : topics) {
            int averageDirectoryCountPerBroker = (int)Math.ceil((double) (Utils.getPartitionCount(client, topic) * Utils.getReplicationFactor(client, topic)) / (double)Utils.getBrokerCount());
            logger.debug("Average Directory count per Broker for topic {}: {}", topic, averageDirectoryCountPerBroker);
            String zNodeDataForTopic = Utils.getDataFromTopicZNode(client, topic);
            String localBrokerIdStr = "" + Utils.getLocalBrokerId();
            int directoryCountOnLocalBroker = Utils.getSubStringOccurrenceCount(zNodeDataForTopic, localBrokerIdStr);
            logger.debug("Directory count on local broker for topic {} is {}", topic, directoryCountOnLocalBroker);
            if(directoryCountOnLocalBroker < averageDirectoryCountPerBroker) {
                logger.debug("Topic {} has directoryCountOnLocalBroker < averageDirectoryCountPerBroker", topic);
                //look for any adoption ads for this topic
                Partition adForTopic = adoptionAdsTopicsPartitionsMap.get(topic);
                if(adForTopic != null) {
                    return adForTopic;
                }
            }
        }
        logger.debug("Partition to Take in: none");
        return null;
    }

    public void advertisePartitionForAdoption(Partition partitionToGiveOut) throws Exception {
        //clear any previous ads by this broker
        List<String> prevAds = Utils.getAdoptionAds(client);
        String localBrokerId = "" + Utils.getLocalBrokerId();
        for (String prevAd : prevAds) {
            if(prevAd.startsWith(localBrokerId)) {
                logger.debug("Deleting previous Ad posting {}", prevAd);
                client.delete().forPath(ADOPTION_ADS_ROOT_ZNODE + "/" + prevAd);
            }
        }

        String adZNode = ADOPTION_ADS_ROOT_ZNODE + "/" + partitionToGiveOut.getPartitionFullNameWithBrokerId();
        logger.debug("Posting Ad for {}", adZNode);
        //create an ephemeral ad post
        client.create().withMode(CreateMode.EPHEMERAL).forPath(adZNode, AD_POSTING_TEXT.getBytes());
        long adPostTime = System.currentTimeMillis();
        logger.debug("Ad {} created at {}", adZNode, adPostTime);
        while(new String(client.getData().forPath(adZNode)).equals(AD_POSTING_TEXT) && System.currentTimeMillis() - adPostTime < AD_EXPIRY_MS) {
            logger.debug("Ad {} is active. Sleeping 1 min", adZNode);
            Thread.sleep(60000);
        }

        if(!new String(client.getData().forPath(adZNode)).equals(AD_POSTING_TEXT)) {
            logger.debug("Partition {} got adopted", adZNode);
        }
        else {
            logger.debug("Expired the Ad for {}", adZNode);
        }

        //Someone has adopted this partition, or the Ad needs to expire. Delete the Ad
        logger.debug("Deleting Ad node {}", adZNode);
        client.delete().forPath(adZNode);
    }

    public void adoptRemotePartition(Partition partitionToTakeIn) throws Exception {
        String adZNode = ADOPTION_ADS_ROOT_ZNODE + "/" + partitionToTakeIn.getPartitionFullNameWithBrokerId();
        logger.debug("Will try to adopt Partition {}", adZNode);
        //try to lock the Ad first and then initiate the adoption process
        String adZNodeLockPath = ADOPTION_ADS_LOCK_ROOT_ZNODE + "/" + partitionToTakeIn.getPartitionFullNameWithBrokerId();
        InterProcessMutex lock = new InterProcessMutex(client, adZNodeLockPath);
        if ( lock.acquire(30000, TimeUnit.MILLISECONDS) )
        {
            logger.debug("Successfully acquired lock on Ad: {}", adZNodeLockPath);
            try
            {
                //check again if the Ad is still valid
                if(new String(client.getData().forPath(adZNode)).equals(AD_POSTING_TEXT)) {
                    boolean reassignmentSucceeded = reassignPartitionToLocalBroker(
                            partitionToTakeIn.brokerId,
                            partitionToTakeIn.topicName,
                            partitionToTakeIn.partitionId,
                            Utils.getLocalBrokerId(),
                            adZNode);
                    logger.debug("Reassignment succeeded: {}", reassignmentSucceeded);
                }
                else {
                    logger.debug("Ad {} seems stale. Skipping adoption process", adZNode);
                }
            }
            finally
            {
                lock.release();
                while(true) {
                    try {
                        client.delete().forPath(adZNodeLockPath);
                        break;
                    }
                    catch(KeeperException.NoNodeException noNodeEx) {
                        //ignore and break
                        break;
                    }
                    catch (Exception ex) {
                        logger.debug("Trying to delete {} after releasing lock but got exception {}. Will retry", adZNodeLockPath, ex);
                    }
                }
            }
        }
        else {
            logger.debug("Failed to acquire lock on Ad: {}", adZNodeLockPath);
        }
    }

    private boolean reassignPartitionToLocalBroker(int remoteBrokerId, String topicName, int partitionId, int localBrokerId, String adZNode) throws Exception {
        boolean succeeded = false;
        String reassignmentConfigFileName = "partitions-to-move.json." + System.currentTimeMillis();
        String reassignmentProcessLockPath = ADOPTION_ADS_LOCK_ROOT_ZNODE;
        InterProcessMutex lock = new InterProcessMutex(client, reassignmentProcessLockPath);
        while(!succeeded) {
            if (lock.acquire(30000, TimeUnit.MILLISECONDS) )
            {
                logger.debug("Locking {} succeeded", reassignmentProcessLockPath);
                try {
                    String currentReplicas = Utils.getReplicasOfTopicPartition(client, topicName, partitionId);
                    String desiredReplicas = currentReplicas.replace(""+remoteBrokerId, ""+localBrokerId);
                    String reassignmentJson = String.format("{\"partitions\":[{\"topic\":\"%s\",\"partition\":%d,\"replicas\":[%s]}],\"version\":1}", topicName, partitionId, desiredReplicas);
                    //do kafka reassignment
                    PrintWriter out = new PrintWriter(reassignmentConfigFileName);
                    out.println( reassignmentJson );
                    out.close();
                    logger.debug("Reassignment will be kicked for {}", reassignmentJson);
                    String[] reassignCmdArgs = {
                            "--reassignment-json-file=" + reassignmentConfigFileName,
                            "--zookeeper=" + client.getZookeeperClient().getCurrentConnectionString(),
                            "--execute"
                    };
                    ReassignPartitionsCommand.main(reassignCmdArgs);
                    //Hacky: Restart kafka controller. Controller seems buggy sometimes
                    Utils.restartKafkaController(client);
                    //Hacky: Sleep for 5 mins to let the reassignment process complete
                    logger.debug("Reassignment command has been initiated. Will sleep for {} ms", 10 * 60000);
                    Thread.sleep(10 * 60000);

                    Files.deleteIfExists(Paths.get(reassignmentConfigFileName));

                    logger.debug("Setting data for Ad {} as {}", adZNode, AD_ADOPTION_COMPLETE_TEXT + "-" + localBrokerId);
                    //mark the ad as done
                    client.setData().forPath(adZNode, (AD_ADOPTION_COMPLETE_TEXT + "-" + localBrokerId).getBytes());
                    succeeded = true;
                } finally {
                    lock.release();
                }
            } else {
                logger.debug("Locking {} failed. Will probably retry", reassignmentProcessLockPath);
                //check if ad is still valid, otherwise break retry loop
                if(!new String(client.getData().forPath(adZNode)).equals(AD_POSTING_TEXT)) {
                    logger.debug("Ad {} has expired. Quit trying to reassign", adZNode);
                    break;
                }
                else {
                    logger.debug("Ad {} is still valid. Will retry", adZNode);
                }
            }
        }

        return succeeded;
    }
}
