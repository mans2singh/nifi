/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.influxdb;

import static org.junit.Assert.assertEquals;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.influxdb.InfluxDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPutInfluxDB {
    private TestRunner runner;
    private PutInfluxDB mockPutInfluxDB;

    @Before
    public void setUp() throws Exception {
        mockPutInfluxDB = new PutInfluxDB() {
            @Override
            protected InfluxDB makeConnection() {
                return null;
            }

            @Override
            protected void writeToInfluxDB(String consistencyLevel, String database, String retentionPolicy,
                    String records) {
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDB);
        runner.setProperty(PutInfluxDB.DB_NAME, "test");
        runner.setProperty(PutInfluxDB.USERNAME, "user");
        runner.setProperty(PutInfluxDB.PASSWORD, "password");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDB.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL,PutInfluxDB.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDB.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testDefaultValid() {
        runner.assertValid();
    }

    @Test
    public void testBlankDBUrl() {
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyDBName() {
        runner.setProperty(PutInfluxDB.DB_NAME, "");
        runner.assertNotValid();
    }

    @Test
    public void testEmptyUsername() {
        runner.setProperty(PutInfluxDB.USERNAME, "");
        runner.assertValid();
    }

    @Test
    public void testEmptyPassword() {
        runner.setProperty(PutInfluxDB.PASSWORD, "");
        runner.assertValid();
    }

    @Test
    public void testCharsetUTF8() {
        runner.setProperty(PutInfluxDB.CHARSET, "UTF-8");
        runner.assertValid();
    }

    @Test
    public void testEmptyConsistencyLevel() {
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL, "");
        runner.assertNotValid();
    }

    @Test
    public void testCharsetBlank() {
        runner.setProperty(PutInfluxDB.CHARSET, "");
        runner.assertNotValid();
    }
    @Test
    public void testZeroMaxDocumentSize() {
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "0");
        runner.assertNotValid();
    }

    @Test
    public void testSizeGreaterThanThreshold() {
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 B");
        runner.assertValid();
        byte [] bytes = new byte[2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = 'a';
        }
        runner.enqueue(bytes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_FAILURE, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_FAILURE);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE),"Max records size exceeded " + bytes.length);
    }

    @Test
    public void testValidSingleMeasurement() {
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 MB");
        runner.assertValid();
        byte [] bytes = "test".getBytes();

        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), null);
    }

    @Test
    public void testWriteThrowsException() {
        mockPutInfluxDB = new PutInfluxDB() {
            @Override
            protected InfluxDB makeConnection() {
                return null;
            }

            @Override
            protected void writeToInfluxDB(String consistencyLevel, String database, String retentionPolicy,
                    String records) {
                throw new RuntimeException("WriteException");
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDB);
        runner.setProperty(PutInfluxDB.DB_NAME, "test");
        runner.setProperty(PutInfluxDB.USERNAME, "u1");
        runner.setProperty(PutInfluxDB.PASSWORD, "p1");
        runner.setProperty(PutInfluxDB.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL,PutInfluxDB.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDB.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();

        byte [] bytes = "test".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_FAILURE);

        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE),"WriteException");
    }

    @Test(expected=AssertionError.class)
    public void testMakeConnectionThrowsException() {
        mockPutInfluxDB = new PutInfluxDB() {
            @Override
            protected InfluxDB makeConnection() {
                throw new RuntimeException("testException");
            }
        };
        runner = TestRunners.newTestRunner(mockPutInfluxDB);
        runner.setProperty(PutInfluxDB.DB_NAME, "test");
        runner.setProperty(PutInfluxDB.USERNAME, "u1");
        runner.setProperty(PutInfluxDB.PASSWORD, "p1");
        runner.setProperty(PutInfluxDB.CHARSET, "UTF-8");
        runner.setProperty(PutInfluxDB.INFLUX_DB_URL, "http://dbUrl");
        runner.setProperty(PutInfluxDB.CONSISTENCY_LEVEL,PutInfluxDB.CONSISTENCY_LEVEL_ONE.getValue());
        runner.setProperty(PutInfluxDB.RETENTION_POLICY,"autogen");
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 KB");
        runner.assertValid();

        byte [] bytes = "test".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
    }

    @Test
    public void testValidArrayMeasurement() {
        runner.setProperty(PutInfluxDB.MAX_RECORDS_SIZE, "1 MB");
        runner.assertValid();

        runner.enqueue("test rain=2\ntest rain=3".getBytes());
        runner.run(1,true,true);

        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_SUCCESS);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), null);
    }

    @Test
    public void testInvalidEmptySingleMeasurement() {
        byte [] bytes = "".getBytes();
        runner.enqueue(bytes);
        runner.run(1,true,true);
        runner.assertAllFlowFilesTransferred(PutInfluxDB.REL_FAILURE, 1);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(PutInfluxDB.REL_FAILURE);
        assertEquals(flowFiles.get(0).getAttribute(PutInfluxDB.INFLUX_DB_ERROR_MESSAGE), "Empty measurement size 0");
    }

}