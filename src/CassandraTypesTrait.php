<?php


namespace lroman242\LaravelCassandra;

use Cassandra\Value;

trait CassandraTypesTrait
{
    /**
     * Check if object is instance of any cassandra object types
     *
     * @param $obj
     * @return bool
     */
    public function isCassandraValueObject($obj)
    {
        return $obj instanceof Value;
    }

    /**
     * Returns comparable value from cassandra object type
     *
     * @param $obj
     * @return mixed
     */
    public function valueFromCassandraObject($obj)
    {
        if (is_array($obj)) {
            return array_map(function ($item) {
                return $this->valueFromCassandraObject($item);
            }, $obj);
        }

        if (!is_object($obj)) {
            return $obj;
        }

        $class = get_class($obj);

        $value = $obj;
        switch ($class) {
            case 'Cassandra\Date':
                $value = $obj->seconds();
                break;
            case 'Cassandra\Time':
                $value = $obj->__toString();
                break;
            case 'Cassandra\Timestamp':
                $value = $obj->time();
                break;
            case 'Cassandra\Float':
                $value = $obj->value();
                break;
            case 'Cassandra\Decimal':
                $value = $obj->value();
                break;
            case 'Cassandra\Inet':
                $value = $obj->address();
                break;
            case 'Cassandra\Uuid':
                $value = $obj->uuid();
                break;
            case 'Cassandra\Bigint':
                $value = $obj->value();
                break;
            case 'Cassandra\Blob':
                $value = $obj->toBinaryString();
                break;
            case 'Cassandra\Smallint':
                $value = $obj->value();
                break;
            case 'Cassandra\Timeuuid':
                $value = $obj->uuid();
                break;
            case 'Cassandra\Tinyint':
                $value = $obj->value();
                break;
            case 'Cassandra\Varint':
                $value = $obj->value();
                break;
            case 'Cassandra\Collection':
                $value = [];
                foreach ($obj->values() as $item) {
                    $value[] = $this->valueFromCassandraObject($item);
                }
                break;
//            //TODO: convert to \DateInterval
//            case 'Cassandra\Duration':
//                $value = $obj->nanos();
//                break;
            case 'Cassandra\Map':
                $values = array_map(function ($item) {
                    return $this->valueFromCassandraObject($item);
                }, $obj->values());

                $value = array_combine($obj->keys(), $values);
                break;
            case 'Cassandra\Set':
                $value = array_map(function ($item) {
                    return $this->valueFromCassandraObject($item);
                }, $obj->values());
                break;
            case 'Cassandra\Tuple':
                $value = array_map(function ($item) {
                    return $this->valueFromCassandraObject($item);
                }, $obj->values());
                break;
            case 'Cassandra\UserTypeValue':
                $value = $this->valueFromCassandraObject($obj->values());
                break;
        }

        return $value;
    }

}