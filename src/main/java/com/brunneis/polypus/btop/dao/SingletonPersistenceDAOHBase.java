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
package com.brunneis.polypus.btop.dao;

import com.brunneis.polypus.btop.conf.Conf;
import com.brunneis.polypus.btop.vo.BasicDigitalPostSentiment;
import static com.brunneis.polypus.btop.conf.Conf.PERSISTENCE_DB;
import com.brunneis.polypus.btop.conf.HBaseConf;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author brunneis
 */
public class SingletonPersistenceDAOHBase implements PersistenceDAO {

    private static final SingletonPersistenceDAOHBase INSTANCE
            = new SingletonPersistenceDAOHBase();

    private Logger logger;

    private Table table;
    private final Configuration conf;

    private SingletonPersistenceDAOHBase() {
        logger = Logger.getLogger(SingletonPersistenceDAOHBase.class.getName());
        logger.setLevel(Conf.LOGGER_LEVEL.value());

        this.conf = HBaseConfiguration.create();
        this.conf.set(
                "hbase.zookeeper.quorum",
                ((HBaseConf) PERSISTENCE_DB.value()).ZOOKEEPER_QUORUM.value()
        );
        this.conf.set(
                "hbase.zookeeper.property.clientPort",
                ((HBaseConf) PERSISTENCE_DB.value()).ZOOKEEPER_PORT.value()
        );

        connect();
    }

    public static SingletonPersistenceDAOHBase getInstance() {
        return INSTANCE;
    }

    private void reconnect() {
        this.disconnect();
        this.connect();
    }

    @Override
    public void connect() {
        // Instantiation of the HBase table
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            this.table = connection.getTable(TableName.valueOf(
                    ((HBaseConf) PERSISTENCE_DB.value()).NAME.value())
            );
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        if (table == null) {
            logger.log(Level.SEVERE, "Error accessing HBase table.");
            System.exit(1);
        }
    }

    @Override
    public void disconnect() {
        try {
            this.table.close();
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void updateSentiment(List<BasicDigitalPostSentiment> updatedPosts) {
        for (BasicDigitalPostSentiment bdp : updatedPosts) {
            if (bdp == null) {
                continue;
            }
            Put put = new Put(Bytes.toBytes(bdp.getRowkey()));
            put.addColumn(
                    Bytes.toBytes(((HBaseConf) PERSISTENCE_DB.value()).COLUMN_FAMILY.value()),
                    Bytes.toBytes("sentiment"),
                    Bytes.toBytes(bdp.getSentiment()));

            try {
                this.table.put(put);
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
                reconnect();
            }
        }

    }

}
