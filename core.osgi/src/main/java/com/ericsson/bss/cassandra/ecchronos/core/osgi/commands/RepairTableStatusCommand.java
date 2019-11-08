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
import com.ericsson.bss.cassandra.ecchronos.core.repair.RepairScheduler;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairState;
import com.ericsson.bss.cassandra.ecchronos.core.repair.state.VnodeRepairStates;
import com.ericsson.bss.cassandra.ecchronos.core.utils.LongTokenRange;
import com.google.common.collect.ImmutableSet;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.Completion;
import org.apache.karaf.shell.api.action.Option;
import org.apache.karaf.shell.api.action.lifecycle.Reference;
import org.apache.karaf.shell.api.action.lifecycle.Service;
import org.apache.karaf.shell.support.CommandException;
import org.apache.karaf.shell.support.completers.StringsCompleter;
import org.apache.karaf.shell.support.table.ShellTable;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import static com.ericsson.bss.cassandra.ecchronos.core.osgi.commands.RepairStatusCommand.epochToHumanReadable;

@Service
@Command(scope = "repair", name = "table-status", description = "Give the current repair status for the given table")
public class RepairTableStatusCommand implements Action
{
    private static final String SORT_RANGE = "RANGE";
    private static final String SORT_LAST_REPAIRED = "LAST_REPAIRED";

    @Option(name = "-t", aliases = "table_reference", description = "The table reference in format <keyspace>.<table>", required = true, multiValued = false)
    @Completion(TableReferenceCompleter.class)
    String tableRef;

    @Option(name = "-n", aliases = {}, description = "Number of entries to display", required = false, multiValued = false)
    int limit = Integer.MAX_VALUE;

    @Option(name = "-s", aliases = {"--sort"}, description = "Sort output based on RANGE/LAST_REPAIRED", required = false, multiValued = false)
    @Completion(value = StringsCompleter.class, values = {SORT_RANGE, SORT_LAST_REPAIRED})
    String sortBy = SORT_RANGE;

    @Option(name = "-r", aliases = "--reverse", description = "Reverse the sort order", required = false, multiValued = false)
    boolean reverse = false;

    @Reference
    private RepairScheduler myRepairSchedulerService;

    @Override
    public Object execute() throws Exception
    {
        RepairJobView repairJobView = getRepairJob(myRepairSchedulerService.getCurrentRepairJobs(), tableRef);
        Comparator<VnodeRepairState> comparator = getRepairStateComparator(sortBy, reverse);
        display(System.out, repairJobView.getRepairStateSnapshot().getVnodeRepairStates(), comparator, limit);
        return null;
    }

    protected static final RepairJobView getRepairJob(List<RepairJobView> currentRepairJobs, String tableRef) throws CommandException
    {
        return currentRepairJobs
                .stream()
                .filter(repairJobView -> repairJobView.getTableReference().toString().equalsIgnoreCase(tableRef))
                .findFirst()
                .orElseThrow(() -> new CommandException("Table reference '" + tableRef + "' was not found. Format must be <keyspace>.<table>"));
    }

    protected static final Comparator<VnodeRepairState> getRepairStateComparator(String sortBy, boolean reverse)
    {
        Comparator<VnodeRepairState> comparator;
        switch (sortBy)
        {
            case SORT_LAST_REPAIRED:
                comparator = Comparator.comparing(vnodeRepairState -> vnodeRepairState.lastRepairedAt());
                break;
            case SORT_RANGE:
            default:
                comparator = Comparator.comparing(vnodeRepairState -> vnodeRepairState.getTokenRange().start);
                break;
        }

        return reverse
                ? comparator.reversed()
                : comparator;
    }

    protected static final void display(PrintStream out, VnodeRepairStates repairStates, Comparator<VnodeRepairState> comparator, int limit)
    {
        ShellTable table = new ShellTable();
        table.column("Range");
        table.column("Last repaired at");
        table.column("Replicas");

        repairStates.getVnodeRepairStates()
                .stream()
                .sorted(comparator)
                .limit(limit)
                .forEach(addVnodeStateToTable(table));

        table.print(out);
    }

    private static Consumer<VnodeRepairState> addVnodeStateToTable(ShellTable table)
    {
        return vnodeRepairState -> table.addRow().addContent(getRowData(vnodeRepairState));
    }

    private static List<Object> getRowData(VnodeRepairState state)
    {
        LongTokenRange tokenRange = state.getTokenRange();
        String lastRepairedAt = epochToHumanReadable(state.lastRepairedAt());
        ImmutableSet<Host> replicas = state.getReplicas();
        return Arrays.asList(tokenRange, lastRepairedAt, replicas);
    }
}
