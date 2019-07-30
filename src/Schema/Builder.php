<?php

namespace AHAbid\EloquentCassandra\Schema;

use Closure;
use Illuminate\Database\Schema\Builder as BaseBuilder;

class Builder extends BaseBuilder
{
    /** @var \AHAbid\EloquentCassandra\Connection */
    protected $connection;

    /**
     * Determine if the given table exists.
     *
     * @param  string  $table
     * @return bool
     */
    public function hasTable($table)
    {
        $table = $this->connection->getTablePrefix().$table;
        $keyspace = $this->connection->getKeyspace();

        $args = ['table_name' => $table, 'keyspace_name' => $keyspace];

        $result = $this->connection->selectFromWriteConnection(
            $this->grammar->compileTableExists(), $args
        );

        return $result->count() > 0;
    }

    /**
     * Create a new command set with a Closure.
     *
     * @param  string  $table
     * @param  \Closure|null  $callback
     * @return \Illuminate\Database\Schema\Blueprint
     */
    protected function createBlueprint($table, Closure $callback = null)
    {
        $prefix = $this->connection->getConfig('prefix_indexes')
                    ? $this->connection->getConfig('prefix')
                    : '';

        if (isset($this->resolver)) {
            return call_user_func($this->resolver, $table, $callback, $prefix);
        }

        return new Blueprint($table, $callback, $prefix);
    }

    /**
     * Drop all tables from the database.
     *
     * @return void
     *
     * @throws \LogicException
     */
    public function dropAllTables()
    {
        $tables = [];

        foreach ($this->getAllTables() as $row) {
            $row = (array) $row;

            $tables[] = reset($row);
        }

        if (empty($tables)) {
            return;
        }

        foreach ($tables as $table) {
            $this->connection->statement(
                $this->grammar->compileDropTableIfExists($table)
            );
        }
    }

    /**
     * Get all of the table names for the database.
     *
     * @return array
     */
    protected function getAllTables()
    {
        return $this->connection->select(
            $this->grammar->compileGetAllTables(),
            ['keyspace_name' => $this->connection->getKeyspace()]
        );
    }
}
