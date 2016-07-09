//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

import com.google.gson.Gson;
import kafka.admin.ReassignPartitionsCommand;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by Soumyajit Sahu on 6/23/2016.
 */
public class NoPartitionLeaderWatcher {
    static Logger logger = LoggerFactory.getLogger(NoPartitionLeaderWatcher.class);

    public static void main(String args[]) throws Exception {
        logger.info("Application has started");

        Properties props = new Properties();
        props.load(new FileReader("settings.properties"));
        long sleepIntervalInMs = Long.parseLong(props.getProperty("sleep.interval.ms", "120000"));

        NoPartitionLeaderWatchHelper helper = new NoPartitionLeaderWatchHelper(props);

        while(true) {
            try {
                logger.debug("Sleeping for milliseconds = " + sleepIntervalInMs);
                Thread.sleep(sleepIntervalInMs);

                List<Partition> noLeaderPartitionList = helper.getTopicPartitionsWithNoLeader();

                if (noLeaderPartitionList.size() > 0) {
                    List<Tuple2<Partition, String[]>> partitionsToBeReassignedList = new ArrayList<Tuple2<Partition, String[]>>();
                    logger.info("No. of partitions with -1 leader = " + noLeaderPartitionList.size());
                    List<String> liveBrokers = helper.getLiveBrokers();
                    int liveBrokerIndex = 0;
                    if (liveBrokers.size() > 0) {
                        for (Partition partition : noLeaderPartitionList) {
                            try {
                                int replicationFactor = helper.getReplicationFactor(partition.topicName);
                                if (replicationFactor > liveBrokers.size()) {
                                    logger.error("Not enough live brokers to resolve leader -1 issue for " + partition.getPartitionFullName());
                                    continue;
                                } else {
                                    String[] replicaArray = new String[replicationFactor];
                                    for (int i = 0; i < replicationFactor; i++) {
                                        replicaArray[i] = liveBrokers.get(liveBrokerIndex);
                                        liveBrokerIndex = (liveBrokerIndex == liveBrokers.size() - 1)? 0 : liveBrokerIndex + 1;
                                    }
                                    partitionsToBeReassignedList.add(new Tuple2<Partition, String[]>(partition, replicaArray));
                                }
                            } catch (Exception ex) {
                                logger.error("Exception {} while reassigning {}", ex, partition.getPartitionFullName());
                            }
                        }

                        if (partitionsToBeReassignedList.size() > 0) {
                            reassignPartitionWithNoLeader(partitionsToBeReassignedList, helper);
                        }
                    } else {
                        logger.error("No Live brokers found. Cannot fix any leader -1");
                    }
                } else {
                    logger.info("No partition has -1 leader");
                }
            }
            catch(Exception ex) {
                logger.error(ex.toString());
                ex.printStackTrace();
                break;
            }
        }
        logger.info("Application has stopped");
    }

    private static void reassignPartitionWithNoLeader(List<Tuple2<Partition, String[]>> partitionsToBeReassignedList, NoPartitionLeaderWatchHelper helper) throws Exception {
        String reassignmentConfigFileName = "partitions-to-move.json." + System.currentTimeMillis();

        Gson gson = new Gson();
        //build json string
        MovePartitionsJsonHelper mvPartitionsHelper = new MovePartitionsJsonHelper(partitionsToBeReassignedList.size());
        for (int i = 0; i < partitionsToBeReassignedList.size(); i++) {
            Tuple2<Partition, String[]> partitionToBeReassigned = partitionsToBeReassignedList.get(i);
            PartitionToMoveJsonHelper partitionObj = new PartitionToMoveJsonHelper(partitionToBeReassigned._1.topicName,
                    partitionToBeReassigned._1.partitionId,
                    partitionToBeReassigned._2);
            mvPartitionsHelper.partitions[i] = partitionObj;
        }

        String reassignmentJson = gson.toJson(mvPartitionsHelper);
        //do kafka reassignment
        PrintWriter out = new PrintWriter(reassignmentConfigFileName);
        out.println( reassignmentJson );
        out.close();
        logger.debug("Reassignment will be kicked for {}", reassignmentJson);
        String[] reassignCmdArgs = {
                "--reassignment-json-file=" + reassignmentConfigFileName,
                "--zookeeper=" + Utils.getZookeeperConnectionString(2181),
                "--execute"
        };
        ReassignPartitionsCommand.main(reassignCmdArgs);
        //Hacky: Restart kafka controller. Controller seems buggy sometimes
        helper.restartKafkaController();
        //Hacky: Sleep for sometime to let the reassignment process complete
        logger.debug("Reassignment command has been initiated. Will sleep for {} ms", 5 * 60000);
        Thread.sleep(5 * 60000);

        try {
            Files.deleteIfExists(Paths.get(reassignmentConfigFileName));
        }
        catch (Exception ex) {
            //ignore
        }
    }
}

class MovePartitionsJsonHelper {
    PartitionToMoveJsonHelper[] partitions;

    protected MovePartitionsJsonHelper(int partitionToMoveCount) {
        partitions = new PartitionToMoveJsonHelper[partitionToMoveCount];
    }
}

class PartitionToMoveJsonHelper {
    String topic;
    int partition;
    int[] replicas;

    protected PartitionToMoveJsonHelper(String topic, int partition, String[] replicas) {
        this.topic = topic;
        this.partition = partition;
        this.replicas = new int[replicas.length];
        for(int i=0; i<replicas.length; i++) {
            this.replicas[i] = Integer.parseInt(replicas[i]);
        }
    }
}

class NoPartitionLeaderWatchHelper {
    Properties props;
    private Logger logger = LoggerFactory.getLogger(NoPartitionLeaderWatchHelper.class);
    private CuratorFramework client;

    protected NoPartitionLeaderWatchHelper(Properties props) throws Exception {
        this.props = props;
        String zkConnString = Utils.getZookeeperConnectionString(2181);
        logger.info("Zookeeper connection string is {}", zkConnString);

        client = CuratorFrameworkFactory.newClient(
                zkConnString,
                new ExponentialBackoffRetry(1000, 30)
        );
        client.start();
    }

    protected int getReplicationFactor(String topic) throws Exception {
        return Utils.getReplicationFactor(client, topic);
    }

    protected List<String> getLiveBrokers() throws Exception {
        return Utils.getLiveBrokers(client);
    }

    protected List<Partition> getTopicPartitionsWithNoLeader() throws Exception {
        List<Partition> noLeaderPartitionsList = new ArrayList<Partition>();

        List<String> topics = null;

        String topicList = props.getProperty("topic.list");
        if(topicList == null || topicList.equals("")) {
            topics = Utils.getTopics(client);
        }
        else {
            String[] topicsToMonitor = topicList.split(",");
            topics = new ArrayList<String>(Arrays.asList(topicsToMonitor));
        }

        if(topics == null) {
            return noLeaderPartitionsList;
        }

        for (String topic: topics) {
            int partitionCount = Utils.getPartitionCount(client, topic);
            for (int partition = 0; partition < partitionCount; partition++) {
                int leader = Utils.getLeaderOfTopicPartition(client, topic, partition);
                if(leader == -1) {
                    Partition p = new Partition(-1, topic, partition);
                    noLeaderPartitionsList.add(p);
                }
            }
        }

        return noLeaderPartitionsList;
    }

    @Override
    protected void finalize() throws Throwable {
        if(client != null) {
            client.close();
        }
        super.finalize();
    }

    public void restartKafkaController() throws  Exception {
        Utils.restartKafkaController(client);
    }
}
