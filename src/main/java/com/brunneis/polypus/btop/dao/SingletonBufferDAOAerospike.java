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

import com.brunneis.polypus.btop.vo.BasicDigitalPostSentiment;
import java.util.ArrayList;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Priority;
import com.brunneis.polypus.btop.conf.AerospikeConf;
import com.brunneis.polypus.btop.conf.Conf;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brunneis
 */
public final class SingletonBufferDAOAerospike implements BufferDAO, ScanCallback {

    private static final SingletonBufferDAOAerospike INSTANCE = new SingletonBufferDAOAerospike();

    private Logger logger;

    private AerospikeClient client;
    private final ClientPolicy policy = new ClientPolicy();
    private ArrayList<BasicDigitalPostSentiment> posts;

    private final String inputNamespace
            = ((AerospikeConf) Conf.BUFFER_DB.value()).NAME.value();
    private final String inputSet
            = ((AerospikeConf) Conf.BUFFER_DB.value()).SET.value();

    private SingletonBufferDAOAerospike() {
        logger = Logger.getLogger(SingletonPersistenceDAOHBase.class.getName());
        logger.setLevel(Conf.LOGGER_LEVEL.value());

        policy.readPolicyDefault.timeout = 50;
        // A post will be eventually read
        policy.readPolicyDefault.maxRetries = 1;
        policy.readPolicyDefault.sleepBetweenRetries = 10;

        policy.writePolicyDefault.timeout = 200;
        // Ensure erasure
        policy.writePolicyDefault.maxRetries = 5;
        policy.writePolicyDefault.sleepBetweenRetries = 50;
        // The record does not expire by itself
        policy.writePolicyDefault.expiration = -1;

        policy.scanPolicyDefault.includeBinData = true;
        policy.scanPolicyDefault.priority = Priority.HIGH;
        policy.scanPolicyDefault.concurrentNodes = true;

        connect();
    }

    public static SingletonBufferDAOAerospike getInstance() {
        return INSTANCE;
    }

    @Override
    public void connect() {
        String host = ((AerospikeConf) Conf.BUFFER_DB.value()).HOST.value();
        Integer port = ((AerospikeConf) Conf.BUFFER_DB.value()).PORT.value();
        // Single seed node
        this.client = new AerospikeClient(policy, host, port);
    }

    @Override
    public void disconnect() {
        this.client.close();
    }

    private void reconnect() {
        this.disconnect();
        this.connect();
    }

    @Override
    public void scanCallback(Key key, Record record) {
        String rowkey = record.getString("rowkey");
        Integer sentiment = record.getInt("sentiment");

        if (rowkey != null && sentiment != null) {
            synchronized (this.posts) {
                this.posts.add(new BasicDigitalPostSentiment(rowkey, sentiment));
            }
        }
    }

    @Override
    public void deleteBufferedPosts(List<BasicDigitalPostSentiment> posts) {
        try {
            for (BasicDigitalPostSentiment post : posts) {
                if (post == null) {
                    continue;
                }
                Key key = new Key(inputNamespace, inputSet, post.getRowkey());
                this.client.delete(null, key);
            }
        } catch (AerospikeException ex) {
            logger.log(Level.SEVERE, null, ex);
            reconnect();
        }
    }

    @Override
    public synchronized List<BasicDigitalPostSentiment> readBufferedProcessedPosts() {
        // Clean buffer and load new posts
        this.posts = new ArrayList<>();
        try {
            this.client.scanAll(null, inputNamespace, inputSet, this);
        } catch (AerospikeException ex) {
            logger.log(Level.SEVERE, null, ex);
            reconnect();
        }
        return this.posts;
    }

}
