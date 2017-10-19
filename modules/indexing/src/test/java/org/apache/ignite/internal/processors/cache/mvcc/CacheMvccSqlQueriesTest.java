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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
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
        accountsTxReadAll(1, 0, 0, 1, new IgniteInClosure<CacheConfiguration>() {
            @Override public void apply(CacheConfiguration ccfg) {
                ccfg.setIndexedTypes(Integer.class, MvccTestAccount.class);
            }
        }, false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode_SinglePartition() throws Exception {
        accountsTxReadAll(1, 0, 0, 1, new IgniteInClosure<CacheConfiguration>() {
            @Override public void apply(CacheConfiguration ccfg) {
                ccfg.setIndexedTypes(Integer.class, MvccTestAccount.class);
            }
        }, true, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new IgniteInClosure<CacheConfiguration>() {
            @Override public void apply(CacheConfiguration ccfg) {
                ccfg.setIndexedTypes(Integer.class, MvccTestAccount.class);
            }
        }, false, ReadMode.SQL_ALL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAccountsTxSql_WithRemoves_SingleNode() throws Exception {
        accountsTxReadAll(1, 0, 0, 64, new IgniteInClosure<CacheConfiguration>() {
            @Override public void apply(CacheConfiguration ccfg) {
                ccfg.setIndexedTypes(Integer.class, MvccTestAccount.class);
            }
        }, true, ReadMode.SQL_ALL);
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
}
