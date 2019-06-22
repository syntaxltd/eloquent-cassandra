<?php

use lroman242\LaravelCassandra\Eloquent\Model as Casloquent;

class TestableType extends Casloquent
{
    protected $keyType = 'uuid';

    public $incrementing = false;

    protected $casts = [
        'date' => 'date',
        'datetime' => 'datetime',
        'time' => 'string',
    ];
}