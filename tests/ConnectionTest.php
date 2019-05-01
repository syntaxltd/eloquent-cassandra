<?php

class ConnectionTest extends TestCase
{
    public function testConnection()
    {
        $connection = DB::connection('cassandra');
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Connection::class, $connection);
    }

    public function testReconnect()
    {
        $c1 = DB::connection('cassandra');
        $c2 = DB::connection('cassandra');
        $this->assertEquals(spl_object_hash($c1), spl_object_hash($c2));

        $c1 = DB::connection('cassandra');
        DB::purge('cassandra');
        $c2 = DB::connection('cassandra');
        $this->assertNotEquals(spl_object_hash($c1), spl_object_hash($c2));
    }

    public function testQueryLog()
    {
        DB::enableQueryLog();

        $this->assertEquals(0, count(DB::getQueryLog()));

        DB::table('testtable')->get();
        $this->assertEquals(1, count(DB::getQueryLog()));

        DB::table('testtable')->insert(['id' => 99, 'name' => 'test']);
        $this->assertEquals(2, count(DB::getQueryLog()));

        DB::table('testtable')->count();
        $this->assertEquals(3, count(DB::getQueryLog()));

        DB::table('testtable')->where('id', 99)->update(['name' => 'test']);
        $this->assertEquals(4, count(DB::getQueryLog()));

        DB::table('testtable')->where('id', 99)->delete();
        $this->assertEquals(5, count(DB::getQueryLog()));
    }

    public function testDriverName()
    {
        $driver = DB::connection('cassandra')->getDriverName();
        $this->assertEquals('cassandra', $driver);
    }

    public function testConnectionGetCassandraCluster()
    {
        $cluster = \Illuminate\Support\Facades\DB::connection('cassandra')->getCassandraCluster();

        $this->assertInstanceOf(\Cassandra\Cluster::class, $cluster);
    }

    public function testConnectionGetCassandraSession()
    {
        $session = \Illuminate\Support\Facades\DB::connection('cassandra')->getCassandraSession();

        $this->assertInstanceOf(\Cassandra\Session::class, $session);
    }

    public function testConnectionGetKeyspace()
    {
        $keyspace = \Illuminate\Support\Facades\DB::connection('cassandra')->getKeyspace();

        $this->assertEquals($keyspace, $this->app['config']->get('database.connections.cassandra.keyspace'));
    }

    public function testConnectionDisconnect()
    {
        $connection = \Illuminate\Support\Facades\DB::connection('cassandra');
        $connection->disconnect();

        $this->assertNull($connection->getCassandraSession());
    }

    public function testConnectionReconnectIfMissingConnection()
    {
        DB::enableQueryLog();

        $connection = \Illuminate\Support\Facades\DB::connection('cassandra');
        $connection->disconnect();

        $this->assertNull($connection->getCassandraSession());

        $connection->table('testtable')->first();

        $this->assertEquals(1, count(DB::getQueryLog()));

        $this->assertInstanceOf(\Cassandra\Session::class, $connection->getCassandraSession());
    }
}
