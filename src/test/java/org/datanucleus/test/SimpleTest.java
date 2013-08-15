package org.datanucleus.test;

import java.util.List;
import java.util.Set;

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
        try
        {
            tx.begin();

            Sample s1 = new Sample(1, "A very long string that will be serialised using Kryo");
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
        }
        finally
        {
        pmf.close();
        NucleusLogger.GENERAL.info(">> test END");
        }
    }
}
