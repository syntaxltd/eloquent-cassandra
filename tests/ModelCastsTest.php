<?php


use Illuminate\Foundation\Application;

class ModelCastsTest extends TestCase
{
    protected $uuid = null;

    /**
     * Define environment setup.
     *
     * @param  Illuminate\Foundation\Application    $app
     *
     * @return void
     *
     * @throws Exception
     */
    protected function getEnvironmentSetUp($app)
    {
        parent::getEnvironmentSetUp($app);

        $this->uuid = new \Cassandra\Uuid();

        \Illuminate\Support\Facades\DB::connection('cassandra')->select('TRUNCATE TABLE unittest.testable_types');
        \Illuminate\Support\Facades\DB::connection('cassandra')->select('INSERT INTO testable_types (id, timeId, name, date, time, datetime, ip, asciiStr) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', [
            $this->uuid,
            new \Cassandra\Timeuuid(),
            "Some Name",
            \Cassandra\Date::fromDateTime(new \DateTime('2019-01-02 13:14:15')),
            \Cassandra\Time::fromDateTime(new \DateTime('2019-01-02 13:14:15')),
            new \Cassandra\Timestamp(),
            new \Cassandra\Inet('127.1.2.3'),
            "Some String for ASCII"
        ]);

    }

    /**
     * Check if casts work with cassandra date
     */
    public function testCastCassandraDateToDatetime()
    {
        $model = TestableType::where('id', $this->uuid)->first();

        $this->assertTrue($model->hasCast('date', 'date'));
        $this->assertInstanceOf(\DateTime::class, $model->date);
        $this->assertEquals('00:00:00', $model->date->format('H:i:s'));
    }

    /**
     * Check if casts work with cassandra timestamp
     */
    public function testCastCassandraTimestampToDatetime()
    {
        $model = TestableType::where('id', $this->uuid)->first();

        $this->assertTrue($model->hasCast('datetime', 'datetime'));
        $this->assertInstanceOf(\DateTime::class, $model->datetime);
    }

    /**
     * Check if casts work with cassandra timestamp
     */
    public function testCastCassandraTimeToString()
    {
        $model = TestableType::where('id', $this->uuid)->first();

        $this->assertTrue($model->hasCast('time', 'string'));
        $this->assertEquals('13:14:15', $model->time);
    }


}