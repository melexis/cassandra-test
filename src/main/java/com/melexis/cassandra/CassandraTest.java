package com.melexis.cassandra;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.model.MutatorImpl;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import java.util.Map;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Period;

public class CassandraTest {

    private static final String ONE_MEGABYTE = StringUtils.repeat("a", 1024 * 1024);

    public static void main(final String[] args) {
        final CassandraHostConfigurator conf = new CassandraHostConfigurator("esb-a-test:9160, esb-b-test:9160");
        conf.setAutoDiscoveryDataCenter("ieper");
        conf.setAutoDiscoverHosts(true);

        final Cluster cluster = HFactory.getOrCreateCluster("local", conf);

        final ConfigurableConsistencyLevel cl = new ConfigurableConsistencyLevel();
        final Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();

        clmap.put("TESTKS", HConsistencyLevel.LOCAL_QUORUM);

        cl.setReadCfConsistencyLevels(clmap);
        cl.setWriteCfConsistencyLevels(clmap);

        final Keyspace testks = HFactory.createKeyspace("TESTNS", cluster, cl);
        final ColumnFamilyTemplate<String, String> testcf =
            new ThriftColumnFamilyTemplate<String, String>(testks,
                                                           "TESTKS",
                                                           StringSerializer.get(),
                                                           StringSerializer.get());

        int written = 0;
        int failures = 0;
        final DateTime before = new DateTime();

        for (int r=0; r<50; r++) {
            final DateTime beforeRow = new DateTime();
            System.out.println("Start write of row " + r);
            //            final ColumnFamilyUpdater<String, String> updater = testcf.createUpdater("row" + r);
            for (int c=0; c<50; c++) {
                final Mutator mutator = new MutatorImpl(testks);
                mutator.insert("row" + r, "TESTKS", new HColumnImpl("column" + c, ONE_MEGABYTE, System.currentTimeMillis()));
                //                updater.setString("column" + c, ONE_MEGABYTE);
                written++;
                mutator.execute();
                //                testcf.update(updater);
            }
            final Period rowDelta = new Period(beforeRow, new DateTime());
            System.out.println("Stop write of row " + r + " after " + rowDelta);
        }

        final Period delta = new Period(before, new DateTime());

        System.out.println(String.format("Wrote %d MB in %s.  Got %d failures", 
                                         written,
                                         delta,
                                         failures));
    }
}
