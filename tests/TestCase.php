<?php

namespace AHAbid\EloquentCassandra\Tests;

use AHAbid\EloquentCassandra\CassandraServiceProvider;

class TestCase extends Orchestra\Testbench\TestCase
{
    /**
     * Get package providers.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return array
     */
    protected function getPackageProviders($app)
    {
        return [
            CassandraServiceProvider::class,
        ];
    }

    /**
     * Define environment setup.
     *
     * @param  Illuminate\Foundation\Application    $app
     * @return void
     */
    protected function getEnvironmentSetUp($app)
    {
        $config = require(__DIR__ . '/config/database.php');

        $app['config']->set('app.key', 'gi0BMtzVEdluo98rjx9aiFWjYtETsj8V');

        $app['config']->set('database.default', 'cassandra');
        $app['config']->set('database.connections.cassandra', $config['connections']['cassandra']);

        $app['config']->set('auth.model', 'User');
        $app['config']->set('auth.providers.users.model', 'User');
        $app['config']->set('cache.driver', 'array');

        $app['db']->connection('cassandra')->select('TRUNCATE testtable');
        $app['db']->connection('cassandra')->select('TRUNCATE testtable_popularity');
        for ($i = 1; $i <= 10; $i++) {
            $app['db']->connection('cassandra')->select('INSERT INTO testtable (id, name) VALUES (?, ?)', [$i, "value$i"]);
        }
    }
}
