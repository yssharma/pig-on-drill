/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.sys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

public class PStoreTestUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PStoreTestUtil.class);

  public static void test(PStoreProvider provider) throws Exception{
    PStore<String> store = provider.getPStore(PStoreConfig.newJacksonBuilder(new ObjectMapper(), String.class).name("sys.test").build());
    String[] keys = {"first", "second"};
    String[] values = {"value1", "value2"};
    Map<String, String> expectedMap = Maps.newHashMap();

    for(int i =0; i < keys.length; i++){
      expectedMap.put(keys[i], values[i]);
      store.put(keys[i], values[i]);
    }

    {
      Iterator<Map.Entry<String, String>> iter = store.iterator();
      for(int i =0; i < keys.length; i++){
        Entry<String, String> e = iter.next();
        assertTrue(expectedMap.containsKey(e.getKey()));
        assertEquals(expectedMap.get(e.getKey()), e.getValue());
      }

      assertFalse(iter.hasNext());
    }

    {
      Iterator<Map.Entry<String, String>> iter = store.iterator();
      while(iter.hasNext()){
        iter.next();
        iter.remove();
      }
    }

    assertFalse(store.iterator().hasNext());
  }
}
