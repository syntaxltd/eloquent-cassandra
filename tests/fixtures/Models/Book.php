<?php

namespace AHAbid\EloquentCassandra\Fixtures\Models;

use AHAbid\EloquentCassandra\Eloquent\Model;

class Book extends Model
{
    protected static $unguarded = true;
    protected $primaryKey = 'title';
}
