<?php

namespace lroman242\LaravelCassandra\Eloquent;

use Carbon\Carbon;
use Cassandra\Rows;
use Cassandra\Timestamp;
use lroman242\LaravelCassandra\CassandraTypesTrait;
use lroman242\LaravelCassandra\Collection;
use lroman242\LaravelCassandra\Query\Builder as QueryBuilder;
use Illuminate\Database\Eloquent\Model as BaseModel;

abstract class Model extends BaseModel
{
    use CassandraTypesTrait;

    /**
     * The connection name for the model.
     *
     * @var string
     */
    protected $connection = 'cassandra';

    /**
     * Indicates if the IDs are auto-incrementing.
     * This is not possible in cassandra so we override this
     *
     * @var bool
     */
    public $incrementing = false;

    /**
     * @inheritdoc
     */
    public function newEloquentBuilder($query)
    {
        return new Builder($query);
    }

    /**
     * @inheritdoc
     */
    protected function newBaseQueryBuilder()
    {
        $connection = $this->getConnection();

        return new QueryBuilder($connection, null, $connection->getPostProcessor());
    }

    /**
     * @inheritdoc
     */
    public function freshTimestamp()
    {
        return new Timestamp();
    }

    /**
     * @inheritdoc
     */
    public function fromDateTime($value)
    {
        // If the value is already a Timestamp instance, we don't need to parse it.
        if ($value instanceof Timestamp) {
            return $value;
        }

        // Let Eloquent convert the value to a DateTime instance.
        if (!$value instanceof \DateTime) {
            $value = parent::asDateTime($value);
        }

        return new Timestamp($value->getTimestamp() * 1000);
    }

    /**
     * @inheritdoc
     */
    protected function asDateTime($value)
    {
        // Convert UTCDateTime instances.
        if ($value instanceof Timestamp) {
            return Carbon::instance($value->toDateTime());
        }

        return parent::asDateTime($value);
    }

    /**
     * Get the table qualified key name.
     * Cassandra does not support the table.column annotation so
     * we override this
     *
     * @return string
     */
    public function getQualifiedKeyName()
    {
        return $this->getKeyName();
    }

    /**
     * Qualify the given column name by the model's table.
     *
     * @param  string  $column
     * @return string
     */
    public function qualifyColumn($column)
    {
        return $column;
    }

    /**
     * @inheritdoc
     */
    public function __call($method, $parameters)
    {
        // Unset method
        if ($method == 'unset') {
            return call_user_func_array([$this, 'drop'], $parameters);
        }

        return parent::__call($method, $parameters);
    }

    /**
     * Create a new Eloquent Collection instance.
     *
     * @param  Rows|array  $rows
     *
     * @return Collection
     *
     * @throws \Exception
     */
    public function newCassandraCollection($rows)
    {
        if (!is_array($rows) && !$rows instanceof Rows) {
            throw new \Exception('Wrong type to create collection');//TODO: customize error
        }

        $items = [];
        foreach ($rows as $row) {
            $items[] = $this->newFromBuilder($row);
        }

        $collection = new Collection($items);

        if ($rows instanceof Rows) {
            $collection->setRowsInstance($rows);
        }

        return $collection;
    }

    /**
     * Determine if the new and old values for a given key are equivalent.
     *
     * @param  string $key
     * @param  mixed $current
     * @return bool
     */
    public function originalIsEquivalent($key, $current)
    {
        if (!array_key_exists($key, $this->original)) {
            return false;
        }

        $original = $this->getOriginal($key);

        if ($current === $original) {
            return true;
        } elseif (is_null($current)) {
            return false;
        } elseif ($this->isDateAttribute($key)) {
            return $this->fromDateTime($current) ===
                $this->fromDateTime($original);
        } elseif ($this->hasCast($key)) {
            return $this->castAttribute($key, $current) ===
                $this->castAttribute($key, $original);
        } elseif ($this->isCassandraValueObject($current)) {
            return $this->valueFromCassandraObject($current) ===
                $this->valueFromCassandraObject($original);
        }

        return is_numeric($current) && is_numeric($original)
            && strcmp((string) $current, (string) $original) === 0;
    }

    /**
     * Get the value of the model's primary key.
     *
     * @return mixed
     */
    public function getKey()
    {
        $value = $this->getAttribute($this->getKeyName());

        if ($this->isCassandraValueObject($value)) {
            return $this->valueFromCassandraObject($this->getAttribute($this->getKeyName()));
        }

        return $value;
    }

}
