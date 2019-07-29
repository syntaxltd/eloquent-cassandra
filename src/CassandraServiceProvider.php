<?php

namespace AHAbid\EloquentCassandra;

use AHAbid\EloquentCassandra\Repository\DatabaseMigrationRepository;
use Illuminate\Support\ServiceProvider;

class CassandraServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     */
    public function register()
    {
        // Add database driver.
        $this->app->resolving('db', function ($db) {
            $db->extend('cassandra', function ($config, $name) {
                $config['name'] = $name;

                return new Connection((new CassandraConnector)->connect($config), $config);
            });
        });

    }

    /**
     * Boot the service provider
     */
    public function boot()
    {
        $this->app->singleton('migration.repository', function ($app) {
            $table = $app['config']['database.migrations'];

            return new DatabaseMigrationRepository($app['db'], $table);
        });
    }
}
