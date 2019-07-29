<?php

namespace AHAbid\EloquentCassandra;

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
}
