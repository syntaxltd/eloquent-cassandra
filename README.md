Laravel Cassandra
===============

A Cassandra based Eloquent model and Query builder for Laravel (Casloquent)

[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/lroman242/laravel-cassandra/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/lroman242/laravel-cassandra/?branch=master)[![Code Coverage](https://scrutinizer-ci.com/g/lroman242/laravel-cassandra/badges/coverage.png?b=master)](https://scrutinizer-ci.com/g/lroman242/laravel-cassandra/?branch=master)[![Build Status](https://scrutinizer-ci.com/g/lroman242/laravel-cassandra/badges/build.png?b=master)](https://scrutinizer-ci.com/g/lroman242/laravel-cassandra/build-status/master)

**Real test coverage is much lower**

Installation
------------

### Laravel version Compatibility

 Laravel  | Package
:---------|:----------
 5.4.x - 5.5.x   | v0.1.2

Make sure you have the Cassandra PHP driver installed (version 1.2+). You can find more information at http://datastax.github.io/php-driver/.

Installation using composer:

```
composer require lroman242/laravel-cassandra
```
#### Laravel
And add the service provider in `config/app.php`:

```php
lroman242\LaravelCassandra\CassandraServiceProvider::class,
```

The service provider will register a cassandra database extension with the original database manager. There is no need to register additional facades or objects. When using cassandra connections, Laravel will automatically provide you with the corresponding cassandra objects.

For usage outside Laravel, check out the [Capsule manager](https://github.com/illuminate/database/blob/master/README.md) and add:

```php
$capsule->getDatabaseManager()->extend('cassandra', function($config)
{
    return new lroman242\LaravelCassandra\Connection($config);
});
```
#### Lumen

Add next lines to your `bootstrap.php`

```php
    $app->configure('database');
```

```php
    $app->register(lroman242\LaravelCassandra\CassandraServiceProvider::class);
```

Configuration
-------------

Change your default database connection name in `config/database.php`:

```php
'default' => env('DB_CONNECTION', 'cassandra'),
```

And add a new cassandra connection:

```php
'cassandra' => [
    'driver'          => 'cassandra',
    'host'            => env('DB_HOST', 'localhost'),
    'port'            => env('DB_PORT', 9042),
    'keyspace'        => env('DB_DATABASE'),
    'username'        => env('DB_USERNAME'),
    'password'        => env('DB_PASSWORD'),
    'page_size'       => env('DB_PAGE_SIZE', 5000),
    'consistency'     => Cassandra::CONSISTENCY_LOCAL_ONE,
    'timeout'         => null,
    'connect_timeout' => 5.0,
    'request_timeout' => 12.0,
],
```

You can connect to multiple servers with the following configuration:

```php
'cassandra' => [
    'driver'          => 'cassandra',
    'host'            => ['192.168.0.1', '192.168.0.2'], //or '192.168.0.1,192.168.0.2'
    'port'            => env('DB_PORT', 9042),
    'keyspace'        => env('DB_DATABASE'),
    'username'        => env('DB_USERNAME'),
    'password'        => env('DB_PASSWORD'),
    'page_size'       => env('DB_PAGE_SIZE', 5000),
    'consistency'     => Cassandra::CONSISTENCY_LOCAL_ONE,
    'timeout'         => null,
    'connect_timeout' => 5.0,
    'request_timeout' => 12.0,
],
```
Note: you can enter all of your nodes in .env like :


    # .env
    DB_HOST=192.168.0.1,192.168.0.2,192.168.0.3

Note: list of available consistency levels (php constants):

    Cassandra::CONSISTENCY_ANY
    Cassandra::CONSISTENCY_ONE
    Cassandra::CONSISTENCY_TWO
    Cassandra::CONSISTENCY_THREE
    Cassandra::CONSISTENCY_QUORUM
    Cassandra::CONSISTENCY_ALL
    Cassandra::CONSISTENCY_SERIAL
    Cassandra::CONSISTENCY_QUORUM
    Cassandra::CONSISTENCY_LOCAL_QUORUM
    Cassandra::CONSISTENCY_EACH_QUORUM
    Cassandra::CONSISTENCY_LOCAL_SERIAL
    Cassandra::CONSISTENCY_LOCAL_ONE
    
Note: you can set specific consistency level according to the query using options

Eloquent
--------

#### Model Usage
Supported most of eloquent query build features, events, fields access.

```php
    $users = User::all();
    
    $user = User::where('email', 'tester@test.com')->first();
    
    $user = User::find(new \Cassandra\Uuid("7e4c27e2-1991-11e8-accf-0ed5f89f718b"))
```

Relations - NOT SUPPORTED

#### Attributes casting

There is ability to use UUID as model primary key

```
class Item 
{
    ...
    
    protected $keyType = 'uuid';
    
    public $incrementing = true; // will automatically cast your primary key to keyType
    
    // OR
    
    protected $keyType = 'uuid';
    
    public $incrementing = false;
    
    protected $casts = [
        'id' => 'uuid',
    ];    
    ...
}
```


Query Builder
-------------

The database driver plugs right into the original query builder. When using cassandra connections, you will be able to build fluent queries to perform database operations.

```php
$users = DB::table('users')->get();

$user = DB::table('users')->where('name', 'John')->first();
```

If you did not change your default database connection, you will need to specify it when querying.

```php
$user = DB::connection('cassandra')->table('users')->get();
```

Default use of `get` method of query builder will call chunked fetch from database.
Chunk size can be configured on config file (` 'page_size' => env('DB_PAGE_SIZE', 5000)`) or with additional query builder\`s method `setPageSize`.

```php
$comments = Comments::setPageSize(500)->get(); // will return all comments, not 500
```

**WARNING**: Not recomended to use `get` if there are a lot of data in table. Use `getPage` instead.

Get single page of resuts
```php
$comments = Comments::setPageSize(500)->getPage(); // will return collection with 500 results
```

There is an ability to set next page token what allows to get next chunk of results
```php
$comments = Comments::setPaginationStateToken($token)->getPage();
```

Get next page:
```php
$comments = $comments->nextPage();
```

Get next page token:
```php
$comments = $comments->getNextPageToken();
```

Append collection with next page`s result:
```php
$comments->appendNextPage();
```

Check if it is last page:
```php
$comments->isLastPage();
```

Get raw cassandra response for current page (\Cassandra\Rows):
```php
$rows = $commants->getRows();
```

Read more about the query builder on http://laravel.com/docs/queries

Examples
---------

- store users data to csv

```php
$users = User::setPageSize(1000)->getPage();
while(!$users->isLastPage()) {
    foreach($users as $user) {
        // here you can write a lines to csv file
    }
    
    $users = $users->nextPage();
}
```

- Simple api to make `Load more` as paggination on page

```php
public function getComments(Request $request) {
    ...
    
    $comments = Comment::setPageSize(50)
        ->setPaginationStateToken($request->get('nextPageToken', null)
        ->getPage();
    
    ...
    
    return response()->json([
        ...
        'comments' => $comments,
        'nextPageToken' => !$comments->isLastPage() ? $comments->getNextPageToken() : null,
        ...
    ]);
}
```

- If you use cassandra materialized views you can easily use it with eloquent models
```php
$users = User::from('users_by_country_view')->where('country', 'USA')->get();
```


TODO:
-----
- full support of composite primary key
- full test coverage
- fix diff between \Cassandra\Date with Carbon
- add schema queries support
- add ability to use async queries

Docker:
------
There is docker-compose setup stored in package root. It can be used for local development and test running.
Works well with PHPStorm testing tools + coverage.

Run command below inside of the "main" container to run tests and generate coverage file:

```sh
vendor/bin/phpunit --coverage-clover clover.xml
```
