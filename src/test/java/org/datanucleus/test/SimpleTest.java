/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.test;

import org.junit.*;
import javax.jdo.*;

import static org.junit.Assert.*;

import org.datanucleus.samples.Sample;
import org.datanucleus.util.NucleusLogger;

public class SimpleTest
{
    @Test
    public void testSimple()
    {
        NucleusLogger.GENERAL.info(">> test START");
        PersistenceManagerFactory pmf = null;
        try
        {
            pmf = JDOHelper.getPersistenceManagerFactory("MyTest");

            PersistenceManager pm = pmf.getPersistenceManager();
            Transaction tx = pm.currentTransaction();
            Object id = null;
            String longString = "A very long string that will be serialised using Kryo";
            try
            {
                tx.begin();

                Sample s1 = new Sample(1, longString);
                pm.makePersistent(s1);

                tx.commit();
                id = pm.getObjectId(s1);
            }
            catch (Throwable thr)
            {
                NucleusLogger.GENERAL.error(">> Exception thrown persisting data", thr);
                fail("Failed to persist data : " + thr.getMessage());
            }
            finally 
            {
                if (tx.isActive())
                {
                    tx.rollback();
                }
                pm.close();
            }
            pmf.getDataStoreCache().evictAll();

            pm = pmf.getPersistenceManager();
            tx = pm.currentTransaction();
            try
            {
                tx.begin();

                Sample s = (Sample)pm.getObjectById(id);
                NucleusLogger.GENERAL.info(">> getObjectById => " + s);
                assertEquals(longString, s.getLongString());

                tx.commit();
            }
            catch (Throwable thr)
            {
                NucleusLogger.GENERAL.error(">> Exception thrown querying data", thr);
                fail("Failed to query data : " + thr.getMessage());
            }
            finally 
            {
                if (tx.isActive())
                {
                    tx.rollback();
                }
                pm.close();
            }

        }
        catch (Exception e)
        {
            NucleusLogger.GENERAL.info(">> exception thrown by test", e);
            fail("Exception thrown running test : " + e.getMessage());
        }
        finally
        {
            if (pmf != null)
            {
                pmf.close();
            }
            NucleusLogger.GENERAL.info(">> test END");
        }
    }
}
