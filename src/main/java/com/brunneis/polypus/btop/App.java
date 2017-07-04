/*
    Polypus: a Big Data Self-Deployable Architecture for Microblogging 
    Text Extraction and Real-Time Sentiment Analysis

    Copyright (C) 2017 Rodrigo Martínez (brunneis) <dev@brunneis.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.brunneis.polypus.btop;

import com.brunneis.polypus.btop.conf.AerospikeConf;
import com.brunneis.polypus.btop.vo.BasicDigitalPostSentiment;
import com.brunneis.polypus.btop.dao.BufferSingletonFactoryDAO;
import com.brunneis.polypus.btop.dao.PersistenceSingletonFactoryDAO;
import com.brunneis.polypus.btop.conf.Conf;
import com.brunneis.polypus.btop.conf.ConfLoadException;
import com.brunneis.polypus.btop.conf.HBaseConf;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brunneis
 */
public class App {

    public static void main(String[] args) throws ConfLoadException {

        Logger logger = Logger.getLogger(App.class.getName());
        logger.setLevel(Conf.LOGGER_LEVEL.value());

        Conf.loadConf();

        logger.log(Level.INFO, "AS_NAME = {0}",
                ((AerospikeConf) Conf.BUFFER_DB.value()).NAME.value());
        logger.log(Level.INFO, "AS_SET = {0}",
                ((AerospikeConf) Conf.BUFFER_DB.value()).SET.value());
        logger.log(Level.INFO, "AS_HOST = {0}",
                ((AerospikeConf) Conf.BUFFER_DB.value()).HOST.value());
        logger.log(Level.INFO, "AS_PORT = {0}",
                ((AerospikeConf) Conf.BUFFER_DB.value()).PORT.value());
        logger.log(Level.INFO, "HB_NAME = {0}",
                ((HBaseConf) Conf.PERSISTENCE_DB.value()).NAME.value());
        logger.log(Level.INFO, "HB_COLUMN_FAMILY = {0}",
                ((HBaseConf) Conf.PERSISTENCE_DB.value()).COLUMN_FAMILY.value());
        logger.log(Level.INFO, "HB_ZOOKEEPER_PORT = {0}",
                ((HBaseConf) Conf.PERSISTENCE_DB.value()).ZOOKEEPER_PORT.value());
        logger.log(Level.INFO, "HB_ZOOKEEPER_QUORUM = {0}",
                ((HBaseConf) Conf.PERSISTENCE_DB.value()).ZOOKEEPER_QUORUM.value());

        int counter = 1;

        boolean running = true;
        while (running) {
            logger.log(Level.INFO,
                    "lap {0} | getting buffered processed posts.",
                    counter
            );

            // Read output buffer
            ArrayList<BasicDigitalPostSentiment> posts = new ArrayList<>();
            posts.addAll(BufferSingletonFactoryDAO
                    .getDigitalPostDAOinstance(Conf.AEROSPIKE)
                    .readBufferedProcessedPosts());
            logger.log(Level.INFO,
                    "lap {0} | {1} posts collected.",
                    new Object[]{counter, posts.size()}
            );

            // If there are new processed posts
            ArrayList<BasicDigitalPostSentiment> aux = new ArrayList<>();
            if (posts.size() > 0) {
                int i = 0;
                while (i < posts.size()) {
                    boolean newLap = true;

                    // Write 50K records max per iteration
                    if (posts.size() - i - 1 <= 50000) {
                        for (; i < posts.size(); i++) {
                            aux.add(posts.get(i));
                        }
                    } else {
                        for (; newLap || i % 50000 != 0; i++) {
                            aux.add(posts.get(i));
                        }
                        if (i % 50000 == 0) {
                            newLap = false;
                        }
                    }

                    // Write post polarities to HBase
                    PersistenceSingletonFactoryDAO.getDigitalPostDAOinstance(Conf.HBASE).
                            updateSentiment(aux);

                    // Delete persisted posts from the output buffer
                    BufferSingletonFactoryDAO
                            .getDigitalPostDAOinstance(Conf.AEROSPIKE)
                            .deleteBufferedPosts(aux);

                    logger.log(Level.INFO,
                            "lap {0} | {1} posts updated in HBase.",
                            new Object[]{counter, aux.size()}
                    );

                    aux = new ArrayList<>();

                    // Sleep for a second before starting the next batch
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(App.class.getName())
                                .log(Level.SEVERE, null, ex);
                    }
                }
            }

            // Sleep for 30 seconds after persisting all new posts
            try {
                Thread.sleep(30000);
            } catch (InterruptedException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            counter++;

        }
    }

}
