<?php

return [

    'connections' => [

        'cassandra' => [
            'name' => 'cassandra',
            'driver' => 'cassandra',
            'host' => '127.0.0.1,cassandra',
            'keyspace' => 'unittest',
            'port' => 9042,
            'username' => 'root',
            'password' => '',
            'consistency' => Cassandra::CONSISTENCY_LOCAL_ONE,
            'timeout' => 5.0,
            'connect_timeout' => 5.0,
            'request_timeout' => 12.0,
        ],
    ],

];
