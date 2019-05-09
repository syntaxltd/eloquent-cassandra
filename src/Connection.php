<?php

namespace lroman242\LaravelCassandra;

use Cassandra;
use Cassandra\BatchStatement;
use Closure;
use lroman242\LaravelCassandra\Exceptions\NotSupportedException;

class Connection extends \Illuminate\Database\Connection
{
    const DEFAULT_PAGE_SIZE = 5000;
    
    /**
     * The Cassandra keyspace
     *
     * @var string
     */
    protected $keyspace;

    /**
     * The Cassandra cluster
     *
     * @var \Cassandra\Cluster
     */
    protected $cluster;

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
     * Create a new database connection instance.
     *
     * @param  array   $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        if (empty($this->config['page_size'])) {
            $this->config['page_size'] = self::DEFAULT_PAGE_SIZE;
        }

        // Create the connection
        $this->cluster = $this->createCluster($config);

        if (isset($config['keyspace'])) {
            $keyspaceName = $config['keyspace'];

            $this->keyspace = $keyspaceName;
            $this->session = $this->cluster->connect($keyspaceName);
        }

        $this->useDefaultPostProcessor();

        $this->useDefaultSchemaGrammar();

        $this->setQueryGrammar($this->getDefaultQueryGrammar());
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param  string  $table
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
     * Create a new Cassandra cluster object.
     *
     * @param  array   $config
     *
     * @return \Cassandra\Cluster
     */
    protected function createCluster(array $config)
    {
        $cluster = Cassandra::cluster();

        // Authentication
        if (isset($config['username']) && isset($config['password'])) {
            $cluster->withCredentials($config['username'], $config['password']);
        }

        // Contact Points/Host
        if (!empty($config['host'])) {
            $contactPoints = $config['host'];

            if (is_string($contactPoints)) {
                $contactPoints = explode(',', $contactPoints);
            }

            call_user_func_array([$cluster, 'withContactPoints'], (array) $contactPoints);
        }

        if (!empty($config['port'])) {
            $cluster->withPort((int) $config['port']);
        }

        $cluster->withDefaultPageSize(intval(!empty($config['page_size']) ? $config['page_size'] : self::DEFAULT_PAGE_SIZE));

        if (isset($config['consistency']) && in_array($config['consistency'], [
                Cassandra::CONSISTENCY_ANY, Cassandra::CONSISTENCY_ONE, Cassandra::CONSISTENCY_TWO,
                Cassandra::CONSISTENCY_THREE, Cassandra::CONSISTENCY_QUORUM, Cassandra::CONSISTENCY_ALL,
                Cassandra::CONSISTENCY_SERIAL, Cassandra::CONSISTENCY_QUORUM, Cassandra::CONSISTENCY_LOCAL_QUORUM,
                Cassandra::CONSISTENCY_EACH_QUORUM, Cassandra::CONSISTENCY_LOCAL_SERIAL, Cassandra::CONSISTENCY_LOCAL_ONE,
            ])) {

            $cluster->withDefaultConsistency($config['consistency']);
        }

        if (!empty($config['timeout'])) {
            $cluster->withDefaultTimeout(intval($config['timeout']));
        }

        if (!empty($config['connect_timeout'])) {
            $cluster->withConnectTimeout(floatval($config['connect_timeout']));
        }

        if (!empty($config['request_timeout'])) {
            $cluster->withRequestTimeout(floatval($config['request_timeout']));
        }

        return $cluster->build();
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
            $this->session = $this->createCluster($this->config)->connect($this->keyspace);
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
        return call_user_func_array([$this->cluster, $method], $parameters);
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
    public function transaction()
    {
        throw new NotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function beginTransaction()
    {
        throw new NotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function commit()
    {
        throw new NotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function rollBack()
    {
        throw new NotSupportedException("Transactions is not supported by Cassandra database");
    }

    /**
     * @inheritDoc
     */
    public function transactionLevel()
    {
        throw new NotSupportedException("Transactions is not supported by Cassandra database");
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
