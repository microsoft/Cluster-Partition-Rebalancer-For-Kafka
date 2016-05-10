//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

/**
 * Created by Soumyajit Sahu on 4/5/2016.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerLoadWatcher {
    public static void main(String args[]) throws Exception {
        Logger logger = LoggerFactory.getLogger(BrokerLoadWatcher.class);
        logger.info("Application has started");
        ZookeeperBackedAdoptionLogicImpl adoptionLogicImpl = new ZookeeperBackedAdoptionLogicImpl();
        adoptionLogicImpl.run(); // not using a thread for now
        logger.info("Application has stopped");
    }
}
