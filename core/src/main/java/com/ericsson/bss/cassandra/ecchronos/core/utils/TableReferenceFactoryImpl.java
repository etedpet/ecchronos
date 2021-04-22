/*
 * Copyright 2020 Telefonaktiebolaget LM Ericsson
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
package com.ericsson.bss.cassandra.ecchronos.core.utils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A table reference factory using tables existing in Cassandra.
 * Each unique table contains one specific table reference to avoid creating a lot of copies of table references.
 */
public class TableReferenceFactoryImpl implements TableReferenceFactory
{
    private final ConcurrentMap<UUID, UuidTableReference> tableReferences = new ConcurrentHashMap<>();

    private final Metadata metadata;

    public TableReferenceFactoryImpl(Metadata metadata)
    {
        this.metadata = Preconditions.checkNotNull(metadata, "Metadata must be set");
        logMetadata(metadata);
    }

    private static final Logger LOG = LoggerFactory.getLogger(TableReferenceFactoryImpl.class);

    private static void logMetadata(Metadata m)
    {
        LOG.debug("XXX metadata: {}, clusterName:{}, partitioner:{}", m, m.getClusterName(), m.getPartitioner());
        LOG.debug("XXXXX metadata.getKeyspaces(): {}", m.getKeyspaces());
        LOG.debug("XXXXX metadata.getAllHosts(): {}", m.getAllHosts());
    }

    @Override
    public TableReference forTable(String keyspace, String table)
    {
        LOG.debug("XXX forTable. keyspace:{}, table:{}", keyspace, table);
        logMetadata(metadata);
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);  // FÅR NULL HÄR!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        LOG.debug("XXX forTable. keyspaceMetadata:{}", keyspaceMetadata);
        if (keyspaceMetadata == null)
        {
            return null;
        }
        TableMetadata tableMetadata = keyspaceMetadata.getTable(table);
        LOG.debug("XXX forTable. tableMetadata:{}", tableMetadata);
        if (tableMetadata == null)
        {
            return null;
        }
        UUID tableId = tableMetadata.getId();

        TableReference tableReference = tableReferences.get(tableId);
        LOG.debug("XXX forTable. tableReference:{}", tableReference);
        if (tableReference == null)
        {
            tableReference = tableReferences.computeIfAbsent(tableId, k -> new UuidTableReference(tableMetadata));
        }

        return tableReference;
    }

    class UuidTableReference implements TableReference
    {
        private final UUID uuid;
        private final String keyspace;
        private final String table;

        UuidTableReference(TableMetadata tableMetadata)
        {
            uuid = tableMetadata.getId();
            keyspace = tableMetadata.getKeyspace().getName();
            table = tableMetadata.getName();
        }

        @Override
        public UUID getId()
        {
            return uuid;
        }

        @Override
        public String getKeyspace()
        {
            return keyspace;
        }

        @Override
        public String getTable()
        {
            return table;
        }

        @Override
        public String toString()
        {
            return keyspace + "." + table;
        }
    }
}
