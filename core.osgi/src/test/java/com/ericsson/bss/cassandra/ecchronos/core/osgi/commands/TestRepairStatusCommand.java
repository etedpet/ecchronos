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

import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairJobView;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import com.ericsson.bss.cassandra.ecchronos.core.utils.TableReference;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitParamsRunner.class)
public class TestRepairStatusCommand
{
    private static final RepairJobView job1 = mockRepairJob("ks1", "tbl3", true, DateTime.parse("2019-11-11T22:25Z").getMillis());
    private static final RepairJobView job2 = mockRepairJob("ks2", "tbl2", false, DateTime.parse("2019-12-24T14:57Z").getMillis());
    private static final RepairJobView job3 = mockRepairJob("ks1", "tbl1", false, DateTime.parse("1970-01-01T00:00Z").getMillis());

    @BeforeClass
    public static void setup()
    {
        RepairStatusCommand.DATE_FORMATTER.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Test
    @Parameters(method = "comparatorParameters")
    public void testThatComparatorSortsCorrectly(String sortBy, boolean reverse, List<RepairJobView> expected)
    {
        // Given
        List<RepairJobView> jobs = asList(job1, job2, job3);
        // When
        Comparator<RepairJobView> comparator = RepairStatusCommand.getJobComparator(sortBy, reverse);
        jobs.sort(comparator);
        // Then
        assertThat(jobs).isEqualTo(expected);
    }

    public Object[][] comparatorParameters()
    {
        return new Object[][]{
                {"TABLE", false, asList(job3, job1, job2)},
                {"TABLE", true, asList(job2, job1, job3)},
                {"CAN_REPAIR", false, asList(job2, job3, job1)},
                {"CAN_REPAIR", true, asList(job1, job2, job3)},
                {"LAST_REPAIRED", false, asList(job3, job1, job2)},
                {"LAST_REPAIRED", true, asList(job2, job1, job3)},
                {"NON_EXISTING", false, asList(job3, job1, job2)},
        };
    }

    @Test
    public void testThatRepairStatusIsDisplayedCorrectly()
    {
        // Given
        List<RepairJobView> jobs = asList(job1, job2, job3);
        Comparator<RepairJobView> comparator = RepairStatusCommand.getJobComparator("TABLE", false);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(os);
        // When
        RepairStatusCommand.display(out, jobs, comparator, 2);
        // Then
        String expected =
                "Table    │ Want to repair │ Last repaired at\n" +
                "─────────┼────────────────┼────────────────────\n" +
                "ks1.tbl1 │ false          │ 1970-01-01 00:00:00\n" +
                "ks1.tbl3 │ true           │ 2019-11-11 22:25:00\n";
        assertThat(os.toString()).isEqualTo(expected);
    }

    static RepairJobView mockRepairJob(String keyspace, String table, boolean canRepair, long lastRepaired)
    {
        TableReference tableReference = new TableReference(keyspace, table);
        RepairStateSnapshot state = mock(RepairStateSnapshot.class);
        when(state.canRepair()).thenReturn(canRepair);
        when(state.lastRepairedAt()).thenReturn(lastRepaired);

        return new RepairJobView(tableReference, null, state);
    }
}
