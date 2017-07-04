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
package com.brunneis.polypus.btop.conf;

import com.brunneis.locker.Locker;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author brunneis
 */
public class Conf {

    public final static int HBASE = 002;
    public final static int AEROSPIKE = 003;

    public final static Locker<String> CONF_FILE = new Locker<>();
    public final static Locker<Level> LOGGER_LEVEL = new Locker<>();
    public final static Locker<DBConf> PERSISTENCE_DB = new Locker<>();
    public final static Locker<DBConf> BUFFER_DB = new Locker<>();

    public static void loadConf() throws ConfLoadException {
        if (!CONF_FILE.isLocked()) {
            CONF_FILE.set("polypus-btop.conf");
        }

        if (!LOGGER_LEVEL.isLocked()) {
            LOGGER_LEVEL.set(Level.INFO);
        }

        File file = new File(CONF_FILE.value());
        if (!file.exists()) {
            throw new ConfLoadException();
        }

        Properties properties = new Properties();
        InputStream input = null;

        try {
            input = new FileInputStream(CONF_FILE.value());
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Conf.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            properties.load(input);
        } catch (IOException ex) {
            Logger.getLogger(Conf.class.getName()).log(Level.SEVERE, null, ex);
        }

        AerospikeConf asc = new AerospikeConf();
        asc.CURRENT.set(AEROSPIKE);
        asc.NAME.set(properties.getProperty("AS_NAME"));
        asc.HOST.set(properties.getProperty("AS_HOST"));
        asc.PORT.set(Integer.parseInt(properties.getProperty("AS_PORT")));
        asc.SET.set(properties.getProperty("AS_SET"));

        HBaseConf hbc = new HBaseConf();
        hbc.CURRENT.set(HBASE);
        hbc.NAME.set(properties.getProperty("HB_NAME"));
        hbc.COLUMN_FAMILY.set(properties.getProperty("HB_COLUMN_FAMILY"));
        hbc.ZOOKEEPER_PORT.set(properties.getProperty("HB_ZOOKEEPER_PORT"));
        hbc.ZOOKEEPER_QUORUM.set(properties.getProperty("HB_ZOOKEEPER_QUORUM"));

        PERSISTENCE_DB.set(hbc);
        BUFFER_DB.set(asc);

    }

}
