<?php

namespace AHAbid\EloquentCassandra;

use Cassandra;
use Cassandra\BatchStatement;
use AHAbid\EloquentCassandra\Exceptions\CassandraNotSupportedException;
use Illuminate\Database\Connection as BaseConnection;

class Connection extends BaseConnection
{
    /**
     * The Cassandra keyspace
     *
     * @var string
     */
    protected $keyspace;

    /**
     * The Cassandra connection handler.
     *
     * @var \Cassandra\Session
     */
    protected $session;

    /**
     * The config
     *
     * @var array
     */
    protected $config;

    /**
     * The Cassandra cluster
     *
     * @var \Cassandra\Cluster
     */
    protected $cluster;

    /**
     * Connection constructor.
     * @param Cassandra\Cluster $cluster
     * @param array $config
     */
    public function __construct(\Cassandra\Cluster $cluster, array $config)
    {
        $this->config = $config;

        $this->cluster = $cluster;

        $this->keyspace = $this->getDatabase($config);

        $this->session = $this->cluster->connect($this->keyspace);

        $this->useDefaultPostProcessor();

        $this->useDefaultSchemaGrammar();

        $this->setQueryGrammar($this->getDefaultQueryGrammar());
    }

    /**
     * Get keyspace name from config
     *
     * @param array $config
     *
     * @return string|null
     */
    protected function getDatabase(array $config)
    {
        $keyspaceName = null;

        if (isset($config['keyspace'])) {
            $keyspaceName = $config['keyspace'];
        }

        return $keyspaceName;
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param string $table
     * @return Query\Builder
     */
    public function table($table)
    {
        $processor = $this->getPostProcessor();

        $query = new Query\Builder($this, null, $processor);

        return $query->from($table);
    }

    /**
     * return Cassandra cluster.
     *
     * @return \Cassandra\Cluster
     */
    public function getCassandraCluster()
    {
        return $this->cluster;
    }

    /**
     * return Cassandra Session.
     *
     * @return \Cassandra\Session
     */
    public function getCassandraSession()
    {
        return $this->session;
    }

    /**
     * Return the Cassandra keyspace
     *
     * @return string
     */
    public function getKeyspace()
    {
        return $this->keyspace;
    }

    /**
     * Disconnect from the underlying Cassandra connection.
     */
    public function disconnect()
    {
        $this->session->close();
        $this->session = null;
    }

    /**
     * Get the PDO driver name.
     *
     * @return string
     */
    public function getDriverName()
    {
        return 'cassandra';
    }

    /**
     * Run a select statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @param  bool  $useReadPdo
     * @param  array  $customOptions
     *
     * @return mixed
     */
    public function select($query, $bindings = [], $useReadPdo = true, array $customOptions = [])
    {
        return $this->statement($query, $bindings, $customOptions);
    }

    /**
     * Run an bulk insert statement against the database.
     *
     * @param  array  $queries
     * @param  array  $bindings
     * @param  int  $type
     * @param  array  $customOptions
     *
     * @return bool
     */
    public function insertBulk($queries = [], $bindings = [], $type = Cassandra::BATCH_LOGGED, array $customOptions = [])
    {
        return $this->batchStatement($queries, $bindings, $type, $customOptions);
    }

    /**
     * Execute a group of queries inside a batch statement against the database.
     *
     * @param  array  $queries
     * @param  array  $bindings
     * @param  int  $type
     * @param  array  $customOptions
     *
     * @return bool
     */
    public function batchStatement($queries = [], $bindings = [], $type = Cassandra::BATCH_LOGGED, array $customOptions = [])
    {
        return $this->run($queries, $bindings, function ($queries, $bindings) use ($type, $customOptions) {
            if ($this->pretending()) {
                return [];
            }

            $batch = new BatchStatement($type);

            foreach ($queries as $k => $query) {
                $preparedStatement = $this->session->prepare($query);
                $batch->add($preparedStatement, $bindings[$k]);
            }

            return $this->session->execute($batch, $customOptions);
        });
    }

    /**
     * Execute an CQL statement and return the boolean result.
     *
     * @param  string  $query
     * @param  array   $bindings
     * @param  array  $customOptions
     *
     * @return mixed
     */
    public function statement($query, $bindings = [], array $customOptions = [])
    {
        return $this->runStatement($query, $bindings, $customOptions);
    }

    /**
     * Because Cassandra is an eventually consistent database, it's not possible to obtain
     * the affected count for statements so we're just going to return 0, based on the idea
     * that if the query fails somehow, an exception will be thrown
     *
     * @param  string  $query
     * @param  array   $bindings
     * @param  array  $customOptions
     *
     * @return int
     */
    public function affectingStatement($query, $bindings = [], array $customOptions = [])
    {
        return $this->runStatement($query, $bindings, $customOptions, 0, 1);
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultPostProcessor()
    {
        return new Query\Processor();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultQueryGrammar()
    {
        return new Query\Grammar();
    }

    /**
     * @inheritdoc
     */
    protected function getDefaultSchemaGrammar()
    {
        //return new Schema\Grammar();
    }

    /**
     * Reconnect to the database if connection is missing.
     *
     * @return void
     */
    protected function reconnectIfMissingConnection()
    {
        if (is_null($this->session)) {
            $this->session = $this->cluster->connect($this->keyspace);
        }
    }

    /**
     * Dynamically pass methods to the connection.
     *
     * @param  string  $method
     * @param  array   $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return call_user_func_array([$this->session, $method], $parameters);
    }

    /**
     * Execute an CQL statement and return the boolean result.
     *
     * @param string $query
     * @param array $bindings
     * @param array $customOptions
     * @param mixed $defaultFailed
     * @param mixed $defaultSuccess
     *
     * @return mixed
     */
    protected function runStatement($query, $bindings = [], array $customOptions = [], $defaultFailed = [], $defaultSuccess = null)
    {
        return $this->run($query, $bindings, function ($query, $bindings) use ($customOptions, $defaultFailed, $defaultSuccess) {
            if ($this->pretending()) {
                return $defaultFailed;
            }

            $preparedStatement = $this->session->prepare($query);

            //Add bindings
            $customOptions['arguments'] = $bindings;

            $result = $this->session->execute($preparedStatement, $customOptions);

            return $defaultSuccess === null ? $result : $defaultSuccess;
        });
    }

    /**
     * @inheritDoc
     */
    public function transaction(\Closure $callback, $attempts = 1)
    {
        throw new CassandraNotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function beginTransaction()
    {
        throw new CassandraNotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function commit()
    {
        throw new CassandraNotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function rollBack($toLevel = null)
    {
        throw new CassandraNotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function transactionLevel()
    {
        throw new CassandraNotSupportedException("Transactions is not supported by Cassandra database");
    }

    //TODO: override isDoctrineAvailable method
    //TODO: override getDoctrineColumn method
    //TODO: override getDoctrineSchemaManager method
    //TODO: override getDoctrineConnection method
    //TODO: override getPdo method
    //TODO: override getReadPdo method
    //TODO: override setPdo method
    //TODO: override setReadPdo method
    //TODO: override setReconnector method
    //TODO: override reconnect method
    //TODO: override query method

    //TODO: override bindValues method
    //TODO: override cursor method
    //TODO: override unprepared method

    //TODO: check prepareBindings method

    //TODO: provide interface for $this->session->executeAsync
    //TODO: provide interface for $this->session->prepareAsync
    //TODO: provide interface for $this->session->closeAsync
    //TODO: provide interface for $this->session->schema
}
