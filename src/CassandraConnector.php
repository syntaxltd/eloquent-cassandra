<?php


namespace lroman242\LaravelCassandra;


use Illuminate\Database\Connectors\Connector;
use Illuminate\Queue\Connectors\ConnectorInterface;

class CassandraConnector extends Connector implements ConnectorInterface
{
    const DEFAULT_PAGE_SIZE = 5000;

    /**
     * Cassandra Cluster Builder instance
     *
     * @var \Cassandra\Cluster\Builder
     */
    protected $builder;

    /**
     * Establish a database connection.
     *
     * @param  array  $config
     *
     * @return \Cassandra\Cluster
     */
    public function connect(array $config)
    {
        $this->builder = \Cassandra::cluster();

        $this->setConnectionOptions($config);

        $this->setTimeouts($config);

        $this->setDefaultQueryOptions($config);

        return $this->builder->build();
    }

    /**
     * Set username and password to cluster connection.
     * Set cluster contact points (IP addresses)
     * Set connection communication port
     *
     * @param array $config
     */
    protected function setConnectionOptions(array $config)
    {
        // Authentication
        list($username, $password) = [
            $config['username'] ?? null, $config['password'] ?? null,
        ];

        $this->builder->withCredentials($username, $password);

        call_user_func_array([$this->builder, 'withContactPoints'], $this->getContactPoints($config));

        $this->builder->withPort($this->getPort($config));
    }

    /**
     * Parse contact points list from config
     *
     * @param array $config
     *
     * @return array
     */
    protected function getContactPoints(array $config)
    {
        $contactPoints = [];

        if (!empty($config['host'])) {
            $contactPoints = $config['host'];

            if (is_string($contactPoints)) {
                $contactPoints = explode(',', $contactPoints);
            }
        }

        return (array) $contactPoints;
    }

    /**
     * Parse communication port or set default
     *
     * @param array $config
     *
     * @return int
     */
    protected function getPort(array $config)
    {
        return !empty($config['port']) ? (int)$config['port'] : 9042;
    }

    /**
     * Set default consistency level
     * Set default response size to queries
     *
     * @param array $config
     */
    protected function setDefaultQueryOptions(array $config)
    {
        if (isset($config['consistency']) && in_array($config['consistency'], [
                \Cassandra::CONSISTENCY_ANY, \Cassandra::CONSISTENCY_ONE, \Cassandra::CONSISTENCY_TWO,
                \Cassandra::CONSISTENCY_THREE, \Cassandra::CONSISTENCY_QUORUM, \Cassandra::CONSISTENCY_ALL,
                \Cassandra::CONSISTENCY_SERIAL, \Cassandra::CONSISTENCY_QUORUM, \Cassandra::CONSISTENCY_LOCAL_QUORUM,
                \Cassandra::CONSISTENCY_EACH_QUORUM, \Cassandra::CONSISTENCY_LOCAL_SERIAL, \Cassandra::CONSISTENCY_LOCAL_ONE,
            ])) {

            $this->builder->withDefaultConsistency($config['consistency']);
        }

        $this->builder->withDefaultPageSize(intval(!empty($config['page_size']) ? $config['page_size'] : self::DEFAULT_PAGE_SIZE));
    }

    /**
     * Set timeouts for query execution
     * and cluster connection
     *
     * @param array $config
     */
    protected function setTimeouts(array $config)
    {
        if (!empty($config['timeout'])) {
            $this->builder->withDefaultTimeout(intval($config['timeout']));
        }

        if (!empty($config['connect_timeout'])) {
            $this->builder->withConnectTimeout(floatval($config['connect_timeout']));
        }

        if (!empty($config['request_timeout'])) {
            $this->builder->withRequestTimeout(floatval($config['request_timeout']));
        }
    }
}