<?php

namespace AHAbid\EloquentCassandra\Schema;

class WithOption
{
    /** @var array */
    protected $orders = [];

    /** @var array */
    protected $attributes = [];

    /**
     * Add Order By Field with Direction
     *
     * @param string $field
     * @param string $dir
     * @return void
     */
    public function orderBy($field, $dir = 'asc')
    {
        $this->orders[] = "\"$field\" $dir";
    }

    /**
     * Add attribute
     *
     * @param string $key
     * @param string $value
     * @return void
     */
    public function attribute($key, $value)
    {
        $this->attributes[] = "$key=$value";
    }

    /**
     * Compile to CQL
     *
     * @return string
     */
    public function compile()
    {
        if (empty($this->attributes) && empty($this->orders)) {
            return '';
        }

        $cql = 'with ';

        if (!empty($this->orders)) {
            $cql .= sprintf('clustering order by (%s) ', implode(',', $this->orders));
        }

        if (!empty($this->attributes)) {
            $cql .= implode(' AND ', $this->attributes);
        }

        return $cql;
    }
}