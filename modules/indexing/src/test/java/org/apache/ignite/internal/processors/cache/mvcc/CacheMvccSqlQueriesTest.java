/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.mvcc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.lang.GridInClosure3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * TODO IGNITE-3478: text/spatial indexes with mvcc.
 * TODO IGNITE-3478: indexingSpi with mvcc.
 * TODO IGNITE-3478: dynamic index create.
 */
@SuppressWarnings("unchecked")
public class CacheMvccSqlQueriesTest extends CacheMvccAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, new InitIndexing(Integer.class, MvccTestAccount.class), true, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSumSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_SUM);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new InitIndexing(Integer.class, MvccTestAccount.class), true, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_ClientServer_Backups2() throws Exception {
        accountsTxReadAll(4, 2, 2, 64, new InitIndexing(Integer.class, MvccTestAccount.class), false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateSingleValue_SingleNode() throws Exception {
        updateSingleValue(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdateSingleValue_ClientServer() throws Exception {
        updateSingleValue(false);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @throws Exception If failed.
     */
    private void updateSingleValue(boolean singleNode) throws Exception {
        final int VALS = 100;

        final int writers = 4;

        final int readers = 4;

        final int INC_BY = 110;

        final IgniteInClosure<IgniteCache<Object, Object>> init = new IgniteInClosure<IgniteCache<Object, Object>>() {
            @Override public void apply(IgniteCache<Object, Object> cache) {
                Map<Integer, MvccTestSqlIndexValue> vals = new HashMap<>();

                for (int i = 0; i < VALS; i++)
                    vals.put(i, new MvccTestSqlIndexValue(i));

                cache.putAll(vals);
            }
        };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int cnt = 0;

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            Integer key = rnd.nextInt(VALS);

                            cache.cache.invoke(key, new CacheEntryProcessor<Integer, MvccTestSqlIndexValue, Object>() {
                                @Override public Object process(MutableEntry<Integer, MvccTestSqlIndexValue> e, Object... args) {
                                    Integer key = e.getKey();

                                    MvccTestSqlIndexValue val = e.getValue();

                                    int newIdxVal;

                                    if (val.idxVal1 < INC_BY) {
                                        assertEquals(key.intValue(), val.idxVal1);

                                        newIdxVal = val.idxVal1 + INC_BY;
                                    }
                                    else {
                                        assertEquals(INC_BY + key, val.idxVal1);

                                        newIdxVal = key;
                                    }

                                    e.setValue(new MvccTestSqlIndexValue(newIdxVal));

                                    return null;
                                }
                            });
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    SqlFieldsQuery[] qrys = new SqlFieldsQuery[3];

                    qrys[0] = new SqlFieldsQuery(
                            "select _key, idxVal1 from MvccTestSqlIndexValue where idxVal1=?");

                    qrys[1] = new SqlFieldsQuery(
                            "select _key, idxVal1 from MvccTestSqlIndexValue where idxVal1=? or idxVal1=?");

                    qrys[2] = new SqlFieldsQuery(
                            "select _key, idxVal1 from MvccTestSqlIndexValue where _key=?");

                    while (!stop.get()) {
                        Integer key = rnd.nextInt(VALS);

                        int qryIdx = rnd.nextInt(3);

                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        List<List<?>> res;

                        try {
                            SqlFieldsQuery qry = qrys[qryIdx];

                            if (qryIdx == 1)
                                qry.setArgs(key, key + INC_BY);
                            else
                                qry.setArgs(key);

                            res = cache.cache.query(qry).getAll();
                        }
                        finally {
                            cache.readUnlock();
                        }

                        assertTrue(qryIdx == 0 || !res.isEmpty());

                        if (!res.isEmpty()) {
                            assertEquals(1, res.size());

                            List<?> resVals = res.get(0);

                            Integer key0 = (Integer)resVals.get(0);
                            Integer val0 = (Integer)resVals.get(1);

                            assertEquals(key, key0);
                            assertTrue(val0.equals(key) || val0.equals(key + INC_BY));
                        }
                    }

                    if (idx == 0) {
                        SqlFieldsQuery qry = new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue");

                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        List<List<?>> res;

                        try {
                            res = cache.cache.query(qry).getAll();
                        }
                        finally {
                            cache.readUnlock();
                        }

                        assertEquals(VALS, res.size());

                        for (List<?> vals : res)
                            info("Value: " + vals);
                    }
                }
            };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(Integer.class, MvccTestSqlIndexValue.class),
            init,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountTransactional_SingleNode() throws Exception {
      countTransactional(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCountTransactional_ClientServer() throws Exception {
        countTransactional(false);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @throws Exception If failed.
     */
    private void countTransactional(boolean singleNode) throws Exception {
        final int writers = 4;

        final int readers = 4;

        final int THREAD_KEY_RANGE = 100;

        final int VAL_RANGE = 10;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    int min = idx * THREAD_KEY_RANGE;
                    int max = min + THREAD_KEY_RANGE;

                    info("Thread range [min=" + min + ", max=" + max + ']');

                    int cnt = 0;

                    Set<Integer> keys = new LinkedHashSet<>();

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            // Add or remove 10 keys.
                            if (!keys.isEmpty() && (keys.size() == THREAD_KEY_RANGE || rnd.nextInt(3) == 0 )) {
                                Set<Integer> rmvKeys = new HashSet<>();

                                for (Integer key : keys) {
                                    rmvKeys.add(key);

                                    if (rmvKeys.size() == 10)
                                        break;
                                }

                                assertEquals(10, rmvKeys.size());

                                cache.cache.removeAll(rmvKeys);

                                keys.removeAll(rmvKeys);
                            }
                            else {
                                TreeMap<Integer, MvccTestSqlIndexValue> map = new TreeMap<>();

                                while (map.size() != 10) {
                                    Integer key = rnd.nextInt(min, max);

                                    if (keys.add(key))
                                        map.put(key, new MvccTestSqlIndexValue(rnd.nextInt(VAL_RANGE)));
                                }

                                assertEquals(10, map.size());

                                cache.cache.putAll(map);
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }

                    info("Writer finished, updates: " + cnt);
                }
            };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
            new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    List<SqlFieldsQuery> qrys = new ArrayList<>();

                    qrys.add(new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue"));

                    qrys.add(new SqlFieldsQuery(
                        "select count(*) from MvccTestSqlIndexValue where idxVal1 >= 0 and idxVal1 <= " + VAL_RANGE));

                    while (!stop.get()) {
                        TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                        try {
                            for (SqlFieldsQuery qry : qrys) {
                                List<List<?>> res = cache.cache.query(qry).getAll();

                                assertEquals(1, res.size());

                                Long cnt = (Long)res.get(0).get(0);

                                assertTrue(cnt % 10 == 0);
                            }
                        }
                        finally {
                            cache.readUnlock();
                        }
                    }
                }
            };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(Integer.class, MvccTestSqlIndexValue.class),
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxTransactional_SingleNode() throws Exception {
        maxMinTransactional(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMaxTransactional_ClientServer() throws Exception {
        maxMinTransactional(false);
    }

    /**
     * @param singleNode {@code True} for test with single node.
     * @throws Exception If failed.
     */
    private void maxMinTransactional(boolean singleNode) throws Exception {
        final int writers = 1;

        final int readers = 1;

        final int THREAD_OPS = 10;

        final int OP_RANGE = 10;

        final int THREAD_KEY_RANGE = OP_RANGE * THREAD_OPS;

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> writer =
                new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                    @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        int min = idx * THREAD_KEY_RANGE;

                        info("Thread range [start=" + min + ']');

                        int cnt = 0;

                        boolean add = true;

                        int op = 0;

                        while (!stop.get()) {
                            TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                            try {
                                int startKey = min + op * OP_RANGE;

                                if (add) {
                                    Map<Integer, MvccTestSqlIndexValue> vals = new HashMap<>();

                                    for (int i = 0; i < 10; i++) {
                                        Integer key = startKey + i + 1;

                                        vals.put(key, new MvccTestSqlIndexValue(key));
                                    }

                                    cache.cache.putAll(vals);

                                    info("put " + vals.keySet());
                                }
                                else {
                                    Set<Integer> rmvKeys = new HashSet<>();

                                    for (int i = 0; i < 10; i++)
                                        rmvKeys.add(startKey + i + 1);

                                    cache.cache.removeAll(rmvKeys);

                                    info("remove " + rmvKeys);
                                }

                                if (++op == THREAD_OPS) {
                                    add = !add;

                                    op = 0;
                                }
                            }
                            finally {
                                cache.readUnlock();
                            }
                        }

                        info("Writer finished, updates: " + cnt);
                    }
                };

        GridInClosure3<Integer, List<TestCache>, AtomicBoolean> reader =
                new GridInClosure3<Integer, List<TestCache>, AtomicBoolean>() {
                    @Override public void apply(Integer idx, List<TestCache> caches, AtomicBoolean stop) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        List<SqlFieldsQuery> qrys = new ArrayList<>();

                        qrys.add(new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue"));

                        qrys.add(new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue"));

                        while (!stop.get()) {
                            TestCache<Integer, MvccTestSqlIndexValue> cache = randomCache(caches, rnd);

                            try {
                                for (SqlFieldsQuery qry : qrys) {
                                    List<List<?>> res = cache.cache.query(qry).getAll();

                                    assertEquals(1, res.size());

                                    Integer m = (Integer)res.get(0).get(0);

                                    assertTrue(m == null || m % 10 == 0);
                                }
                            }
                            finally {
                                cache.readUnlock();
                            }
                        }
                    }
                };

        int srvs;
        int clients;

        if (singleNode) {
            srvs = 1;
            clients = 0;
        }
        else {
            srvs = 4;
            clients = 2;
        }

        readWriteTest(
            null,
            srvs,
            clients,
            0,
            DFLT_PARTITION_COUNT,
            writers,
            readers,
            DFLT_TEST_TIME,
            new InitIndexing(Integer.class, MvccTestSqlIndexValue.class),
            null,
            writer,
            reader);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlQueriesWithMvcc() throws Exception {
        Ignite srv0 = startGrid(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache =  (IgniteCache)srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class));

        for (int i = 0; i < 10; i++)
            cache.put(i, new MvccTestSqlIndexValue(i));

        {
            SqlFieldsQuery qry = new SqlFieldsQuery("select max(idxVal1) from MvccTestSqlIndexValue");

            cache.query(qry).getAll();
        }

        {
            SqlFieldsQuery qry = new SqlFieldsQuery("select min(idxVal1) from MvccTestSqlIndexValue");

            cache.query(qry).getAll();
        }

        {

            SqlFieldsQuery qry = new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue");

            cache.query(qry).getAll();
        }

        {

            SqlFieldsQuery qry = new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1=5");

            cache.query(qry).getAll();
        }

        {

            SqlFieldsQuery qry = new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue where idxVal1 >= 0 and idxVal1 < 5");

            cache.query(qry).getAll();
        }

        {

            SqlFieldsQuery qry = new SqlFieldsQuery("select sum(idxVal1) from MvccTestSqlIndexValue");

            cache.query(qry).getAll();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlSimple() throws Exception {
        startGrid(0);

        for (int i = 0; i < 4; i++)
            sqlSimple(i * 512);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 5; i++)
            sqlSimple(rnd.nextInt(2048));
    }

    /**
     * @param inlineSize Inline size.
     * @throws Exception If failed.
     */
    private void sqlSimple(int inlineSize) throws Exception {
        Ignite srv0 = ignite(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache =  (IgniteCache)srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class).
                setSqlIndexMaxInlineSize(inlineSize));

        Map<Integer, Integer> expVals = new HashMap<>();

        checkValues(expVals, cache);

        cache.put(1, new MvccTestSqlIndexValue(1));
        expVals.put(1, 1);

        checkValues(expVals, cache);

        cache.put(1, new MvccTestSqlIndexValue(2));
        expVals.put(1, 2);

        checkValues(expVals, cache);

        cache.put(2, new MvccTestSqlIndexValue(1));
        expVals.put(2, 1);
        cache.put(3, new MvccTestSqlIndexValue(1));
        expVals.put(3, 1);
        cache.put(4, new MvccTestSqlIndexValue(1));
        expVals.put(4, 1);

        checkValues(expVals, cache);

        cache.remove(1);
        expVals.remove(1);

        checkValues(expVals, cache);

        checkNoValue(1, cache);

        cache.put(1, new MvccTestSqlIndexValue(10));
        expVals.put(1, 10);

        checkValues(expVals, cache);

        checkActiveQueriesCleanup(srv0);

        srv0.destroyCache(cache.getName());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSqlSimplePutRemoveRandom() throws Exception {
        startGrid(0);

        testSqlSimplePutRemoveRandom(0);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 3; i++)
            testSqlSimplePutRemoveRandom(rnd.nextInt(2048));
    }

    /**
     * @param inlineSize Inline size.
     * @throws Exception If failed.
     */
    private void testSqlSimplePutRemoveRandom(int inlineSize) throws Exception {
        Ignite srv0 = grid(0);

        IgniteCache<Integer, MvccTestSqlIndexValue> cache = (IgniteCache) srv0.createCache(
            cacheConfiguration(PARTITIONED, FULL_SYNC, 0, DFLT_PARTITION_COUNT).
                setIndexedTypes(Integer.class, MvccTestSqlIndexValue.class).
                setSqlIndexMaxInlineSize(inlineSize));

        Map<Integer, Integer> expVals = new HashMap<>();

        final int KEYS = 100;
        final int VALS = 10;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        long stopTime = System.currentTimeMillis() + 5_000;

        for (int i = 0; i < 100_000; i++) {
            Integer key = rnd.nextInt(KEYS);

            if (rnd.nextInt(5) == 0) {
                cache.remove(key);

                expVals.remove(key);
            }
            else {
                Integer val = rnd.nextInt(VALS);

                cache.put(key, new MvccTestSqlIndexValue(val));

                expVals.put(key, val);
            }

            checkValues(expVals, cache);

            if (System.currentTimeMillis() > stopTime) {
                info("Stop test, iteration: " + i);

                break;
            }
        }

        for (int i = 0; i < KEYS; i++) {
            if (!expVals.containsKey(i))
                checkNoValue(i, cache);
        }

        checkActiveQueriesCleanup(srv0);

        srv0.destroyCache(cache.getName());
    }

    /**
     * @param key Key.
     * @param cache Cache.
     */
    private void checkNoValue(Object key, IgniteCache cache) {
        SqlQuery<Integer, MvccTestSqlIndexValue> qry;

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "_key = ?");

        qry.setArgs(key);

        List<IgniteCache.Entry<Integer, MvccTestSqlIndexValue>> res = cache.query(qry).getAll();

        assertTrue(res.isEmpty());
    }

    /**
     * @param expVals Expected values.
     * @param cache Cache.
     */
    private void checkValues(Map<Integer, Integer> expVals, IgniteCache<Integer, MvccTestSqlIndexValue> cache) {
        SqlFieldsQuery cntQry = new SqlFieldsQuery("select count(*) from MvccTestSqlIndexValue");

        Long cnt = (Long)cache.query(cntQry).getAll().get(0).get(0);

        assertEquals((long)expVals.size(), (Object)cnt);

        SqlQuery<Integer, MvccTestSqlIndexValue> qry;

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "true");

        Map<Integer, Integer> vals = new HashMap<>();

        for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll())
            assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

        assertEquals(expVals, vals);

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "_key >= 0");

        vals = new HashMap<>();

        for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll())
            assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

        assertEquals(expVals, vals);

        qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "idxVal1 >= 0");

        vals = new HashMap<>();

        for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll())
            assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

        assertEquals(expVals, vals);

        Map<Integer, Set<Integer>> expIdxVals = new HashMap<>();

        for (Map.Entry<Integer, Integer> e : expVals.entrySet()) {
            qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "_key = ?");

            qry.setArgs(e.getKey());

            List<IgniteCache.Entry<Integer, MvccTestSqlIndexValue>> res = cache.query(qry).getAll();

            assertEquals(1, res.size());
            assertEquals(e.getKey(), res.get(0).getKey());
            assertEquals(e.getValue(), (Integer)res.get(0).getValue().idxVal1);

            SqlFieldsQuery fieldsQry = new SqlFieldsQuery("select _key, idxVal1 from MvccTestSqlIndexValue where _key=?");
            fieldsQry.setArgs(e.getKey());

            List<List<?>> fieldsRes = cache.query(fieldsQry).getAll();

            assertEquals(1, fieldsRes.size());
            assertEquals(e.getKey(), fieldsRes.get(0).get(0));
            assertEquals(e.getValue(), fieldsRes.get(0).get(1));

            Integer val = e.getValue();

            Set<Integer> keys = expIdxVals.get(val);

            if (keys == null)
                expIdxVals.put(val, keys = new HashSet<>());

            assertTrue(keys.add(e.getKey()));
        }

        for (Map.Entry<Integer, Set<Integer>> expE : expIdxVals.entrySet()) {
            qry = new SqlQuery<>(MvccTestSqlIndexValue.class, "idxVal1 = ?");
            qry.setArgs(expE.getKey());

            vals = new HashMap<>();

            for (IgniteCache.Entry<Integer, MvccTestSqlIndexValue> e : cache.query(qry).getAll()) {
                assertNull(vals.put(e.getKey(), e.getValue().idxVal1));

                assertEquals(expE.getKey(), (Integer)e.getValue().idxVal1);

                assertTrue(expE.getValue().contains(e.getKey()));
            }

            assertEquals(expE.getValue().size(), vals.size());
        }
    }

    /**
     *
     */
    static class MvccTestSqlIndexValue implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int idxVal1;

        /**
         * @param idxVal1 Indexed value 1.
         */
        MvccTestSqlIndexValue(int idxVal1) {
            this.idxVal1 = idxVal1;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MvccTestSqlIndexValue.class, this);
        }
    }

    /**
     *
     */
    static class InitIndexing implements IgniteInClosure<CacheConfiguration> {
        /** */
        private final Class[] idxTypes;

        /**
         * @param idxTypes Indexed types.
         */
        InitIndexing(Class<?>... idxTypes) {
            this.idxTypes = idxTypes;
        }

        /** {@inheritDoc} */
        @Override public void apply(CacheConfiguration cfg) {
            cfg.setIndexedTypes(idxTypes);
        }
    }
}
