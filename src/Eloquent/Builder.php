<?php

namespace fuitad\LaravelCassandra\Eloquent;

use Illuminate\Database\Eloquent\Builder as EloquentBuilder;

class Builder extends EloquentBuilder
{
    /**
     * Is not needed cause data are already sorted in cassandra storage
     *
     * @return void
     */
    protected function enforceOrderBy()
    {
        //
    }

    /**
     * Add a where clause on the primary key(s) to the query.
     *
     * @param  mixed  $id
     * @return $this
     */
    public function whereKey($id)
    {
        if (is_array($this->model->getQualifiedKeyName())) {

            foreach ($this->model->getQualifiedKeyName() as $index => $key) {
                if (is_array($id) || $id instanceof Arrayable) {
                    if (array_key_exists($key, $id)) {
                        if (is_array($id[$key])) {
                            $this->query->whereIn($key, $id[$key]);
                        } else {
                            $this->query->where($key, $id[$key]);
                        }
                    } elseif (array_key_exists($index, $id)) {
                        if (is_array($id[$index])) {
                            $this->query->whereIn($key, $id[$index]);
                        } else {
                            $this->query->where($key, $id[$index]);
                        }
                    } else {
                        //error
                        throw new \Exception("Can't map values to array of keys in whereKeyNot. The WHERE clause must specify a value for every component of the primary key.");
                    }
                } else {
                    //error
                    throw new \Exception("Can't map single value to array of keys in whereKey. Expect array or instance of Arrayable");
                }
            }

        } else {

            if (is_array($id) || $id instanceof Arrayable) {
                $this->query->whereIn($this->model->getQualifiedKeyName(), $id);

                return $this;
            }

            return $this->where($this->model->getQualifiedKeyName(), '=', $id);

        }
    }

    /**
     * Add a where clause on the primary key to the query.
     *
     * @param  mixed  $id
     * @return $this
     */
    public function whereKeyNot($id)
    {
        if (is_array($this->model->getQualifiedKeyName())) {

            foreach ($this->model->getQualifiedKeyName() as $index => $key) {
                if (is_array($id) || $id instanceof Arrayable) {
                    if (array_key_exists($key, $id)) {
                        if (is_array($id[$key])) {
                            $this->query->whereNotIn($key, $id[$key]);
                        } else {
                            $this->query->where($key, '!=', $id[$key]);
                        }
                    } elseif (array_key_exists($index, $id)) {
                        if (is_array($id[$index])) {
                            $this->query->whereNotIn($key, $id[$index]);
                        } else {
                            $this->query->where($key, '!=', $id[$index]);
                        }
                    } else {
                        //error
                        throw new \Exception("Can't map values to array of keys in whereKeyNot. The WHERE clause must specify a value for every component of the primary key.");
                    }
                } else {
                    //error
                    throw new \Exception("Can't map single value to array of keys in whereKeyNot. Expect array or instance of Arrayable");
                }
            }

        } else {

            if (is_array($id) || $id instanceof Arrayable) {
                $this->query->whereNotIn($this->model->getQualifiedKeyName(), $id);

                return $this;
            }

            return $this->where($this->model->getQualifiedKeyName(), '!=', $id);

        }
    }
}
