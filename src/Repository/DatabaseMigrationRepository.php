<?php

namespace AHAbid\EloquentCassandra\Repository;

use Illuminate\Database\Migrations\DatabaseMigrationRepository as BaseDatabaseMigrationRepository;

class DatabaseMigrationRepository extends BaseDatabaseMigrationRepository
{
    /**
     * Get the completed migrations.
     *
     * @return array
     */
    public function getRan()
    {
        return $this->table()
            ->get()
            ->sortBy('batch')
            ->pluck('migration')
            ->all();
    }

    /**
     * Get list of migrations.
     *
     * @param  int  $steps
     * @return array
     */
    public function getMigrations($steps)
    {
        return $this->table()
            ->get()
            ->where('batch', '>=', '1')
            ->sortByDesc('batch')
            ->take($steps)
            ->all();
    }

    /**
     * Get the last migration batch.
     *
     * @return array
     */
    public function getLast()
    {
        return $this->table()
            ->get()
            ->where('batch', '=', $this->getLastBatchNumber())
            ->all();
    }

    /**
     * Create the migration repository data store.
     *
     * @return void
     */
    public function createRepository()
    {
        $schema = $this->getConnection()->getSchemaBuilder();
        $driver = $this->getConnection()->getDriverName();

        $schema->create($this->table, function ($table) use ($driver) {
            // The migrations table is responsible for keeping track of which of the
            // migrations have actually run for the application. We'll create the
            // table to hold the migration file's path as well as the batch ID.
            $table->uuid('id');
            $table->string('migration');
            $table->integer('batch');

            if ($driver == 'cassandra') {
                $table->primary([['id'], 'batch', 'migration']);

                $table->withOptions(function($option) {
                    $option->orderBy('batch', 'DESC');
                    $option->orderBy('migration', 'DESC');
                });
            } else {
                $table->primary('id');
            }
        });
    }

    /**
     * Log that a migration was run.
     *
     * @param  string  $file
     * @param  int  $batch
     * @return void
     */
    public function log($file, $batch)
    {
        $record = [
            'id' => $this->getConnection()->raw('uuid()'),
            'migration' => $file,
            'batch' => $batch
        ];

        $this->table()->insert($record);
    }

    /**
     * Remove a migration from the log.
     *
     * @param  object  $migration
     * @return void
     */
    public function delete($migration)
    {
        $row = $this->table()
            ->get()
            ->where('migration', '=', $migration->migration)
            ->first();

        if ($row) {
            $this->table()->where('id', '=', $row['id'])->delete();
        }
    }
}