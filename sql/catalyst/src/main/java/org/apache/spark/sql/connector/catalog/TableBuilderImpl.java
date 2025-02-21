/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog;

import com.google.common.collect.Maps;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.expressions.Transform;

import java.util.Map;

/**
 * Default implementation of {@link TableCatalog.TableBuilder}.
 */
public class TableBuilderImpl implements TableCatalog.TableBuilder {
  /** Catalog where table needs to be created. */
  private final TableCatalog catalog;
  /** Table identifier. */
  private final Identifier identifier;
  /** Columns of the new table. */
  private final Column[] columns;
  /** Table properties. */
  private final Map<String, String> properties = Maps.newHashMap();
  /** Transforms to use for partitioning data in the table. */
  private Transform[] partitions = new Transform[0];

  /**
   * Constructor for TableBuilderImpl.
   *
   * @param catalog catalog where table needs to be created.
   * @param identifier identifier for the table.
   * @param columns the columns of the new table.
   */
  public TableBuilderImpl(TableCatalog catalog,
                          Identifier identifier,
                          Column[] columns) {
    this.catalog = catalog;
    this.identifier = identifier;
    this.columns = columns;
  }

  @Override
  public TableCatalog.TableBuilder withPartitions(Transform[] partitions) {
    this.partitions = partitions;
    return this;
  }

  @Override
  public TableCatalog.TableBuilder withProperties(Map<String, String> properties) {
    this.properties.putAll(properties);
    return this;
  }

  @Override
  public Table create() throws TableAlreadyExistsException, NoSuchNamespaceException {
    return catalog.createTable(identifier, columns, partitions, properties);
  }
}
