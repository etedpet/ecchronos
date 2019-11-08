/*
 * Copyright 2019 Telefonaktiebolaget LM Ericsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ericsson.bss.cassandra.ecchronos.core.osgi.commands;

import com.datastax.driver.core.Host;
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.collect.ImmutableSet;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.karaf.shell.support.CommandException;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;

import static com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.TestRepairStatusCommand.mockRepairJob;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TestRepairTableStatusCommand
{
    private static final RepairJobView job1 = mockRepairJob("ks1", "tbl3", true, 42);
    private static final RepairJobView job2 = mockRepairJob("ks2", "tbl2", false, 43);

    private static final VnodeRepairState state1 = new VnodeRepairState(new LongTokenRange(5, 6), mockHosts("host1", "host2"), DateTime.parse("2019-12-24T14:57Z").getMillis());
    private static final VnodeRepairState state2 = new VnodeRepairState(new LongTokenRange(1, 2), mockHosts("host1", "host3"), DateTime.parse("2019-11-12T00:26:59Z").getMillis());
    private static final VnodeRepairState state3 = new VnodeRepairState(new LongTokenRange(3, 4), mockHosts("host2", "host3"), DateTime.parse("1970-01-01T00:00Z").getMillis());

    @BeforeClass
    public static void setup()
    {
        RepairStatusCommand.DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Test
    public void testThatGetRepairJobThrowsWhenTableDoesNotExist()
    {
        // Given
        List<RepairJobView> jobs = asList(job1, job2);
        // When looking for a non existing table - Then an exception should be thrown
        assertThatExceptionOfType(CommandException.class)
                .isThrownBy(() -> RepairTableStatusCommand.getRepairJob(jobs, "NonExisting.Table"))
                .withMessage("Table reference 'NonExisting.Table' was not found. Format must be <keyspace>.<table>");
    }

    @Test
    public void testThatGetRepairJobFindsMatchingJob() throws CommandException
    {
        // Given
        List<RepairJobView> jobs = asList(job1, job2);
        // When
        RepairJobView repairJob = RepairTableStatusCommand.getRepairJob(jobs, "ks2.tbl2");
        // Then
        assertThat(repairJob).isEqualTo(job2);
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(String sortBy, boolean reverse, List<VnodeRepairState> expected)
    {
        // Given
        List<VnodeRepairState> jobs = asList(state1, state2, state3);
        // When
        Comparator<VnodeRepairState> comparator = RepairTableStatusCommand.getRepairStateComparator(sortBy, reverse);
        jobs.sort(comparator);
        // Then
        assertThat(jobs).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {"RANGE", false, asList(state2, state3, state1)},
                {"RANGE", true, asList(state1, state3, state2)},
                {"LAST_REPAIRED", false, asList(state3, state2, state1)},
                {"LAST_REPAIRED", true, asList(state1, state2, state3)},
                {"DEFAULT", false, asList(state2, state3, state1)},
        };
    }

    @Test
    public void testThatTableStatusIsDisplayedCorrectly()
    {
        // Given
        VnodeRepairStates vnodeRepairStates = VnodeRepairStates.newBuilder(asList(state1, state2, state3)).build();
        Comparator<VnodeRepairState> comparator = RepairTableStatusCommand.getRepairStateComparator("RANGE", false);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        RepairTableStatusCommand.display(out, vnodeRepairStates, comparator, 2);
        // Then
        String expected =
                "Range │ Last repaired at    │ Replicas\n" +
                "──────┼─────────────────────┼───────────────\n" +
                "(1,2] │ 2019-11-12 00:26:59 │ [host1, host3]\n" +
                "(3,4] │ 1970-01-01 00:00:00 │ [host2, host3]\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    private static ImmutableSet<Host> mockHosts(String... hosts)
    {
        ImmutableSet.Builder<Host> builder = ImmutableSet.builder();
        Stream.of(hosts)
                .map(TestRepairTableStatusCommand::mockHost)
                .forEach(builder::add);
        return builder.build();
    }

    private static Host mockHost(String hostName)
    {
        Host host = mock(Host.class);
        when(host.toString()).thenReturn(hostName);
        return host;
    }
}
