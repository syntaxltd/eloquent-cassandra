<?php

namespace AHAbid\EloquentCassandra\Fixtures\Models;

use AHAbid\EloquentCassandra\Eloquent\Model;

class TestableType extends Model
{
    protected $keyType = 'uuid';

    public $incrementing = false;

    protected $casts = [
        'date' => 'date',
        'datetime' => 'datetime',
        'time' => 'string',
    ];
}