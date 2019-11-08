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
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.RepairStateSnapshot;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Completion;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.completers.StringsCompleter;
import org.apache.karaf.shell.support.table.ShellTable;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

@Service
@Command(scope = "repair", name = "status", description = "Give the current repair status")
public class RepairStatusCommand implements Action
{
    static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    private static final String SORT_TABLE = "TABLE";
    private static final String SORT_CAN_REPAIR = "CAN_REPAIR";
    private static final String SORT_LAST_REPAIRED = "LAST_REPAIRED";

    @Option(name = "-n", aliases = {}, description = "Number of entries to display", required = false, multiValued = false)
    int limit = Integer.MAX_VALUE;

    @Option(name = "-s", aliases = {"--sort"}, description = "Sort output based on TABLE/CAN_REPAIR/LAST_REPAIRED", required = false, multiValued = false)
    @Completion(value = StringsCompleter.class, values = {SORT_TABLE, SORT_CAN_REPAIR, SORT_LAST_REPAIRED})
    String sortBy = SORT_TABLE;

    @Option(name = "-r", aliases = "--reverse", description = "Reverse the sort order", required = false, multiValued = false)
    boolean reverse = false;

    @Reference
    private RepairScheduler myRepairSchedulerService;

    @Override
    public Object execute() throws Exception
    {
        Comparator<RepairJobView> comparator = getJobComparator(sortBy, reverse);
        display(System.out, myRepairSchedulerService.getCurrentRepairJobs(), comparator, limit);
        return null;
    }

    protected static final Comparator<RepairJobView> getJobComparator(String sortBy, boolean reverse)
    {
        Comparator<RepairJobView> comparator;
        switch (sortBy)
        {
            case SORT_CAN_REPAIR:
                comparator = Comparator.comparing(job -> job.getRepairStateSnapshot().canRepair());
                break;
            case SORT_LAST_REPAIRED:
                comparator = Comparator.comparing(job -> job.getRepairStateSnapshot().lastRepairedAt());
                break;
            case SORT_TABLE:
            default:
                comparator = Comparator.comparing(job -> job.getTableReference().toString());
                break;
        }

        return reverse
                ? comparator.reversed()
                : comparator;
    }

    protected static final void display(PrintStream out, List<RepairJobView> repairJobs, Comparator<RepairJobView> comparator, int limit)
    {
        ShellTable table = new ShellTable();
        table.column("Table");
        table.column("Want to repair");
        table.column("Last repaired at");

        repairJobs.stream()
                .sorted(comparator)
                .limit(limit)
                .forEach(addJobToTable(table));

        table.print(out);
    }

    private static Consumer<RepairJobView> addJobToTable(ShellTable table)
    {
        return repairJobView -> table.addRow().addContent(getRowData(repairJobView));
    }

    private static List<Object> getRowData(RepairJobView job)
    {
        RepairStateSnapshot state = job.getRepairStateSnapshot();
        return Arrays.asList(job.getTableReference(), state.canRepair(), epochToHumanReadable(state.lastRepairedAt()));
    }

    static String epochToHumanReadable(long time)
    {
        return DATE_FORMATTER.format(new Date(time));
    }
}
