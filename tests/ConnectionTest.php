<?php

use \Illuminate\Support\Facades\DB;
use \lroman242\LaravelCassandra\Connection;
use \lroman242\LaravelCassandra\Exceptions\CassandraNotSupportedException;

class ConnectionTest extends TestCase
{
    /**
     * Test if correct connection instance was created
     */
    public function testConnection()
    {
        $connection = DB::connection('cassandra');
        $this->assertInstanceOf(Connection::class, $connection);
    }

    /**
     * Test reconnect function
     */
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

    /**
     * Check if all queries were logged
     */
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

    /**
     * Check driver name is 'cassandra'
     */
    public function testDriverName()
    {
        $driver = DB::connection('cassandra')->getDriverName();
        $this->assertEquals('cassandra', $driver);
    }

    /**
     * Test cluster getter function returns correct result
     */
    public function testConnectionGetCassandraCluster()
    {
        $cluster = DB::connection('cassandra')->getCassandraCluster();

        $this->assertInstanceOf(\Cassandra\Cluster::class, $cluster);
    }

    /**
     * Test session getter returns correct results
     */
    public function testConnectionGetCassandraSession()
    {
        $session = DB::connection('cassandra')->getCassandraSession();

        $this->assertInstanceOf(\Cassandra\Session::class, $session);
    }

    /**
     * Check if correct keyspace was storred inside of the
     * connection instance
     */
    public function testConnectionGetKeyspace()
    {
        $keyspace = DB::connection('cassandra')->getKeyspace();

        $this->assertEquals($keyspace, $this->app['config']->get('database.connections.cassandra.keyspace'));
    }

    /**
     * Test disconnect function.
     * Check if connection was closed
     */
    public function testConnectionDisconnect()
    {
        $connection = DB::connection('cassandra');
        $connection->disconnect();

        $this->assertNull($connection->getCassandraSession());
    }

    /**
     * Check reconnect function
     */
    public function testConnectionReconnectIfMissingConnection()
    {
        DB::enableQueryLog();

        $connection = DB::connection('cassandra');
        $connection->disconnect();

        $this->assertNull($connection->getCassandraSession());

        $connection->table('testtable')->first();

        $this->assertEquals(1, count(DB::getQueryLog()));

        $this->assertInstanceOf(\Cassandra\Session::class, $connection->getCassandraSession());
    }

    /**
     * Check if correct table was set to the query
     */
    public function testConnectionTableIsCorrect()
    {
        $table = 'testtable';

        $builder = DB::connection('cassandra')->table($table);
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Query\Builder::class, $builder);
        $this->assertEquals($builder->from, $table);
    }

    /**
     * Test select function with only query param
     */
    public function testConnectionCommonSelect()
    {
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable');
        $this->assertInstanceOf(\Cassandra\Rows::class, $rows);
    }

    /**
     * Test select function with query and bindings
     */
    public function testConnectionSelectWithBinding()
    {
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable WHERE id = ?', [3]);
        $this->assertInstanceOf(\Cassandra\Rows::class, $rows);

        $this->assertEquals(1, $rows->count());

        foreach ($rows as $row) {
            $this->assertEquals('value' . $row['id'], $row['name']);
        }
    }

    /**
     * Test if inserts processed with select function.
     * Select should process raw queries without issue
     */
    public function testConnectionSelectWithBindingProcessInsert()
    {
        DB::enableQueryLog();

        $query = "INSERT INTO testtable (id, name) VALUES (?, ?)";

        for ($i = 11; $i <= 15; $i++) {
            DB::connection('cassandra')->select($query, [$i, "newValue$i"]);
        }

        $this->assertEquals(5, count(DB::getQueryLog()));

        $rows = DB::connection('cassandra')->table('testtable')->get();
        $this->assertEquals(15, $rows->count());
    }

    /**
     * Check if custom options were processed correctly
     *
     * List of available custom options:
     * array[‘arguments’] array An array or positional or named arguments
     * array[‘consistency’] int One of Cassandra::CONSISTENCY_*
     * array[‘timeout’] int|null A number of seconds or null
     * array[‘page_size’] int A number of rows to include in result for paging
     * array[‘paging_state_token’] string A string token use to resume from the state of a previous result set
     * array[‘retry_policy’] Cassandra\RetryPolicy A retry policy that is used to handle server-side failures for this request
     * array[‘serial_consistency’] int Either Cassandra::CONSISTENCY_SERIAL or Cassandra::CONSISTENCY_LOCAL_SERIAL
     * array[‘timestamp’] int|string Either an integer or integer string timestamp that represents the number of microseconds since the epoch.
     */
    public function testConnectionSelectWithCustomOptions()
    {
        /** @var \Cassandra\Rows $rows */
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable', [], true, ['page_size' => 5]);

        $this->assertEquals(5, $rows->count());

        $this->assertFalse($rows->isLastPage());

        /** @var \Cassandra\Rows $rowsNext */
        $rowsNext = DB::connection('cassandra')->select('SELECT * FROM testtable', [], true, ['paging_state_token' => $rows->pagingStateToken()]);
        $this->assertEquals(5, $rowsNext->count());
        $this->assertTrue($rowsNext->isLastPage());
    }

    /**
     * Check bulk statements
     */
    public function testConnectionInsertBulk()
    {
        $queries = [];
        $bindings = [];
        for ($i = 11; $i <= 15; $i++) {
            $queries[] = "INSERT INTO testtable (id, name) VALUES (:id, :name)";
            $bindings[] = ['id' => $i, 'name' => 'value' . $i];
        }

        /** @var \Cassandra\Rows $result */
        $result = DB::connection('cassandra')->insertBulk($queries, $bindings);
        $this->assertTrue(!$result[0]['[applied]']);
        $rows = DB::connection('cassandra')->table('testtable')->get();
        $this->assertEquals(15, $rows->count());
    }

    /**
     * Check bulk statements
     */
    public function testConnectionBatchStatementWithSelect()
    {
        $queries = [];
        $bindings = [];
        for ($i = 11; $i <= 15; $i++) {
            $queries[] = "INSERT INTO testtable (id, name) VALUES (:id, :name)";
            $bindings[] = ['id' => $i, 'name' => 'value' . $i];
        }
        $queries[] = "SELECT * FROM testtable WHERE id = ?";
        $bindings[] = [11];

        // only INSERT, UPDATE, DELETE
        $this->expectException(\Exception::class);

        /** @var \Cassandra\Rows $result */
        DB::connection('cassandra')->batchStatement($queries, $bindings);
    }

    /**
     * Check processing batch statement with wrong batch type
     */
    public function testConnectionBatchStatementWithWrongType()
    {
        $queries = [];
        $bindings = [];
        for ($i = 11; $i <= 15; $i++) {
            $queries[] = "INSERT INTO testtable (id, name) VALUES (:id, :name)";
            $bindings[] = ['id' => $i, 'name' => 'value' . $i];
        }

        // only UPDATE for counters
        $this->expectException(\Exception::class);

        /** @var \Cassandra\Rows $result */
        DB::connection('cassandra')->insertBulk($queries, $bindings, \Cassandra::BATCH_COUNTER);
    }

    //TODO: test batch counter
    //TODO: test batch unlogged
    //TODO: test batch with custom options

    /**
     * Test update function with query and bindings
     * expect to get number of affected rows equal 1
     */
    public function testConnectionUpdateWithBinding()
    {
        $newName = "updatedName";
        $affectedRows = DB::connection('cassandra')->update('UPDATE testtable SET name = ? WHERE id = ?', [$newName, 3]);

        $this->assertEquals(1, $affectedRows);

        /** @var \Cassandra\Rows $rows */
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable WHERE id = ?', [3]);
        $this->assertEquals(1, $rows->count());
        $this->assertEquals($newName, $rows->first()['name']);
    }

    /**
     * Test update function with query and bindings
     * expect to get number of affected rows
     */
    public function testConnectionMultipleUpdate()
    {
        $newName = "updatedName";
        $affectedRows = DB::connection('cassandra')->update('UPDATE testtable SET name = ? WHERE id IN (?, ?, ?)', [$newName, 1, 2, 3]);

        //Still expect 1 as result because Cassandra is an eventually consistent database,
        // it's not possible to obtain the affected count for statements
        $this->assertEquals(1, $affectedRows);

        /** @var \Cassandra\Rows $rows */
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable WHERE id IN (?, ?, ?)', [1, 2, 3]);
        $this->assertEquals(3, $rows->count());

        foreach ($rows as $row) {
            $this->assertEquals($row['name'], $newName);
        }
    }

    /**
     * Test update function with query and bindings
     * expect to get number of affected rows
     */
    public function testConnectionDelete()
    {
        $affectedRows = DB::connection('cassandra')->update('DELETE FROM testtable WHERE id = ?', [3]);

        $this->assertEquals(1, $affectedRows);

        /** @var \Cassandra\Rows $rows */
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable WHERE id = ?', [3]);
        $this->assertEquals(0, $rows->count());
    }


    /**
     * Test update function with query and bindings
     * expect to get number of affected rows
     */
    public function testConnectionMultipleDelete()
    {
        $affectedRows = DB::connection('cassandra')->update('DELETE FROM testtable WHERE id IN (?, ?, ?)', [1, 2, 3]);

        //Still expect 1 as result because Cassandra is an eventually consistent database,
        // it's not possible to obtain the affected count for statements
        $this->assertEquals(1, $affectedRows);

        /** @var \Cassandra\Rows $rows */
        $rows = DB::connection('cassandra')->select('SELECT * FROM testtable WHERE id IN (?, ?, ?)', [1, 2, 3]);
        $this->assertEquals(0, $rows->count());
    }

    /**
     * Cassandra doesn't support transactions
     */
    public function testConnectionTransaction()
    {
        $this->expectException(CassandraNotSupportedException::class);

        DB::connection('cassandra')->transaction(function ($query) {
            $query->get();
        }, 5);
    }

    /**
     * Cassandra doesn't support transactions
     */
    public function testConnectionBeginTransaction()
    {
        $this->expectException(CassandraNotSupportedException::class);

        DB::connection('cassandra')->beginTransaction();
    }

    /**
     * Cassandra doesn't support transactions
     */
    public function testConnectionRollback()
    {
        $this->expectException(CassandraNotSupportedException::class);

        DB::connection('cassandra')->rollback();
    }

    /**
     * Cassandra doesn't support transactions
     */
    public function testConnectionCommit()
    {
        $this->expectException(CassandraNotSupportedException::class);

        DB::connection('cassandra')->commit();
    }

    /**
     * Cassandra doesn't support transactions
     */
    public function testConnectionTransactionLevel()
    {
        $this->expectException(CassandraNotSupportedException::class);

        DB::connection('cassandra')->transactionLevel();
    }

    /**
     * Test Connection magic method __call
     * should redirect request to \Cassandra\Session
     */
    public function testConnectionMagicCall()
    {
        $rows = DB::connection('cassandra')->execute('SELECT * FROM testtable LIMIT 3');
        $this->assertEquals(3, $rows->count());
    }

    /**
     * Check if common statement returns correct
     * failed value on pretending state
     *
     * @throws ReflectionException
     */
    public function testPretendingStatement()
    {
        $connection = DB::connection('cassandra');

        $reflector = new ReflectionProperty($connection, 'pretending');
        $reflector->setAccessible(true);
        $reflector->setValue($connection, true);

        $this->assertTrue($connection->pretending());

        $result = $connection->select('SELECT * FROM testtable LIMIT :limit', ['limit' => 3]);

        $this->assertTrue(is_array($result));
        $this->assertEquals([], $result);
    }

    /**
     * Check if affecting statement returns correct
     * failed value on pretending state
     *
     * @throws ReflectionException
     */
    public function testPretendingAffectingStatement()
    {
        $connection = DB::connection('cassandra');

        $reflector = new ReflectionProperty($connection, 'pretending');
        $reflector->setAccessible(true);
        $reflector->setValue($connection, true);

        $this->assertTrue($connection->pretending());

        $result = $connection->update('UPDATE testtable SET name = :name WHERE id = :id ', ['name' => __FUNCTION__, 'id' => 1]);

        $this->assertEquals(0, $result);
    }

    /**
     * Check if batch statement returns correct
     * failed value on pretending state
     *
     * @throws ReflectionException
     */
    public function testPretendingBatchStatement()
    {
        $connection = DB::connection('cassandra');

        $reflector = new ReflectionProperty($connection, 'pretending');
        $reflector->setAccessible(true);
        $reflector->setValue($connection, true);

        $this->assertTrue($connection->pretending());

        $queries = [];
        $bindings = [];
        for ($i = 11; $i <= 15; $i++) {
            $queries[] = "INSERT INTO testtable (id, name) VALUES (:id, :name)";
            $bindings[] = ['id' => $i, 'name' => 'value' . $i];
        }

        $result = $connection->insertBulk($queries, $bindings);
        $this->assertTrue($result === []);
    }

}
