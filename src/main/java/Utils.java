//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

/**
 * Created by Soumyajit Sahu on 4/12/2016.
 */

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    private static final String OS = System.getProperty("os.name").toLowerCase();
    private static Properties props = new Properties();
    static {
        try {
            props.load(new FileReader("settings.properties"));
        }
        catch (Exception ex) {
            logger.error(ex.toString());
        }
    }

    public static String getZookeeperConnectionString(int port) throws Exception {
        //TODO: Following code is specific to my cluster
        if(OS.indexOf("win") >= 0) {
            logger.debug("Creating Zookeeper string from D:\\data\\machineinfo.csv");
            StringBuilder zkStrBuilder = new StringBuilder();
            while (true) {
                try {
                    Scanner scan = new Scanner(new File("D:\\data\\machineinfo.csv"));
                    while (scan.hasNextLine()) {
                        String line = scan.nextLine();
                        if (line.contains("Zookeeper")) {
                            String zkMachineName = line.split(",")[0];
                            zkStrBuilder.append(zkMachineName);
                            zkStrBuilder.append(":2181,");
                        }
                    }
                    break;
                } catch (Exception ex) {
                    logger.debug("Failed to read {} because of {}. Will retry after a sleep", "D:\\data\\machineinfo.csv", ex);
                    Thread.sleep(1000);
                }
            }

            String zkConnStr = zkStrBuilder.substring(0, zkStrBuilder.length() - 1);
            logger.debug("Zookeeper connection string=" + zkConnStr);
            return zkConnStr;
        }
        else {
            return props.getProperty("zookeeper.connect");
            //return "zk1-k09v2:2181,zk2-k09v2:2181,zk4-k09v2:2181";
        }
    }

    public static List<String> getLiveBrokers(CuratorFramework client) throws Exception {
        String brokerIdsZNode = ZookeeperBackedAdoptionLogicImpl.BROKERS_ROOT_ZNODE + "/ids";
        logger.debug("Getting children of {}", brokerIdsZNode);
        List<String> children = client.getChildren().forPath(brokerIdsZNode);
        logger.debug("Found {} children of {}", children.size(), brokerIdsZNode);
        return children;
    }

    public static List<String> getTopics(CuratorFramework client) throws Exception {
        String topicsZNode = ZookeeperBackedAdoptionLogicImpl.BROKERS_ROOT_ZNODE + "/topics";
        logger.debug("Getting children of {}", topicsZNode);
        List<String> children = client.getChildren().forPath(topicsZNode);
        logger.debug("Found {} children of {}", children.size(), topicsZNode);
        return children;
    }

    public static List<String> getAdoptionAds(CuratorFramework client) throws Exception {
        logger.debug("Getting children of {}", ZookeeperBackedAdoptionLogicImpl.ADOPTION_ADS_ROOT_ZNODE);
        List<String> children = client.getChildren().forPath(ZookeeperBackedAdoptionLogicImpl.ADOPTION_ADS_ROOT_ZNODE);
        logger.debug("Found {} children of {}", children.size(), ZookeeperBackedAdoptionLogicImpl.ADOPTION_ADS_ROOT_ZNODE);
        return children;
    }

    public static int getBrokerCount() throws Exception {
        //TODO: Following code is specific to my cluster
        if(OS.indexOf("win") >= 0) {
            logger.debug("Getting broker count from D:\\data\\machineinfo.csv");
            int brokerCount = 0;
            while (true) {
                try {
                    Scanner scan = new Scanner(new File("D:\\data\\machineinfo.csv"));
                    while (scan.hasNextLine()) {
                        if (scan.nextLine().contains("Kafka")) {
                            brokerCount++;
                        }
                    }
                    break;
                } catch (Exception ex) {
                    logger.debug("Failed to read {} because of {}. Will retry after a sleep", "D:\\data\\machineinfo.csv", ex);
                    Thread.sleep(1000);
                }
            }
            logger.debug("Broker count=" + brokerCount);
            return brokerCount;
        }
        else {
            return Integer.parseInt(props.getProperty("broker.count"));
        }
    }

    public static int getPartitionCount(CuratorFramework client, String topic) throws Exception {
        String partitionsPath = ZookeeperBackedAdoptionLogicImpl.BROKERS_ROOT_ZNODE + "/topics/" + topic + "/partitions";
        logger.debug("Getting partitions of {}", partitionsPath);
        List<String> children = client.getChildren().forPath(partitionsPath);
        logger.debug("Found {} children of {}", children.size(), partitionsPath);
        return children.size();
    }

    public static int getReplicationFactor(CuratorFramework client, String topic) throws Exception {
        String replicasOfPartitionZero = getReplicasOfTopicPartition(client, topic, 0);
        int replicationFactor = replicasOfPartitionZero.split(",").length;
        logger.debug("Replication factor of {} is {}", topic, replicationFactor);
        return replicationFactor;
    }

    public static String getDataFromTopicZNode(CuratorFramework client, String topic) throws Exception {
        String topicPath = ZookeeperBackedAdoptionLogicImpl.BROKERS_ROOT_ZNODE + "/topics/" + topic;
        logger.debug("Getting data of {}", topicPath);
        String nodeData = new String(client.getData().forPath(topicPath));
        logger.debug("Found data of znode {} as {}", topicPath, nodeData);
        return nodeData;
    }

    public static int getLocalBrokerId() throws Exception {
        if(OS.indexOf("win") >= 0) {
            //TODO: Following code is specific to my cluster
            Properties props = new Properties();
            props.load(new FileReader("D:\\data\\Kafka\\conf\\server.properties"));
            return Integer.parseInt(props.getProperty("broker.id"));
        }
        else {
            return Integer.parseInt(props.getProperty("broker.id"));
        }
    }

    public static int getSubStringOccurrenceCount(String fullString, String subString) {
        String newStr = fullString.replace(subString, "");
        return ((fullString.length() - newStr.length())/subString.length());
    }

    public static int getPartitionCountOnBrokerForTopic(String topicData,  String brokerID ){
        return getSubStringOccurrenceCount(topicData, "["+brokerID+"]");
    }
    public static int getAnyPartitionNumberForBrokerAndTopic(String topicZNodeData, String brokerId) {
        List<Integer> partitionNumbers = new ArrayList<Integer>();
        Pattern p = Pattern.compile("\\[("+brokerId+")\\]");  // Regex
        Matcher m = p.matcher(topicZNodeData);
        while (m.find()) {
            int endPosition = m.start()-2;
            int startPosition = topicZNodeData.substring(0, endPosition).lastIndexOf('"')+1;
            partitionNumbers.add(Integer.parseInt(topicZNodeData.substring(startPosition,endPosition)));
        }
        return partitionNumbers.get(ThreadLocalRandom.current().nextInt(0, partitionNumbers.size()));
    }

    public static String getReplicasOfTopicPartition(CuratorFramework client, String topic, int partition) throws Exception {
        String topicData = getDataFromTopicZNode(client, topic);
        Pattern pattern = Pattern.compile(String.format("\"%d\":\\[(.*?)\\]", partition));
        Matcher matcher = pattern.matcher(topicData);
        if(matcher.find()) {
            return matcher.group(1);
        }

        return "";
    }

    public static String getDataFromTopicPartitionStateZNode(CuratorFramework client, String topic, int partition) throws Exception {
        String partitionStatePath = ZookeeperBackedAdoptionLogicImpl.BROKERS_ROOT_ZNODE + "/topics/" + topic + "/partitions/" + partition + "/state";
        logger.debug("Getting data of {}", partitionStatePath);
        String nodeData = new String(client.getData().forPath(partitionStatePath));
        logger.debug("Found data of znode {} as {}", partitionStatePath, nodeData);
        return nodeData;
    }

    public static int getLeaderOfTopicPartition(CuratorFramework client, String topic, int partition) throws Exception {
        String stateData = getDataFromTopicPartitionStateZNode(client, topic, partition);
        Pattern pattern = Pattern.compile("\"leader\":(.*?),");
        Matcher matcher = pattern.matcher(stateData);
        matcher.find();
        return Integer.parseInt(matcher.group(1));
    }

    public static void restartKafkaController(CuratorFramework client) throws Exception {
        client.delete().forPath("/controller");
    }
}
