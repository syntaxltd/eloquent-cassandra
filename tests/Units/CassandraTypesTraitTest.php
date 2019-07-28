<?php

namespace AHAbid\EloquentCassandra\Tests\Units;

use AHAbid\EloquentCassandra\Tests\TestCase;
use AHAbid\EloquentCassandra\Fixtures\Models\Item;

class CassandraTypesTraitTest extends TestCase
{
    /**
     * Check if cassandra Model class use CassandraTypesTrait
     */
    public function testModelUseTrait()
    {
        $class = Item::class;
        $traits = [];
        do {
            $traits = array_merge(class_uses($class), $traits);
        } while ($class = get_parent_class($class));

        foreach ($traits as $trait => $same) {
            $traits = array_merge(class_uses($trait), $traits);
        }
        $traits = array_unique($traits);

        $this->assertArrayHasKey(\AHAbid\LaravelCassandra\CassandraTypesTrait::class, $traits);
    }

    public function testIsCassandraValueObjectDate()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Date(time())));
    }

    public function testIsCassandraValueObjectTime()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Time(time())));
    }

    public function testIsCassandraValueObjectTimestamp()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Timestamp(time())));
    }

    public function testIsCassandraValueObjectDuration()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Duration(1, 2, 3)));
    }

    public function testIsCassandraValueObjectUUID()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Uuid("550e8400-e29b-41d4-a716-446655440000")));
    }

    public function testIsCassandraValueObjectTimeUUID()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Timeuuid(time())));
    }

    public function testIsCassandraValueObjectInet()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Inet("0.0.0.0")));
    }

    public function testIsCassandraValueObjectFloat()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Float(3.5)));
    }

    public function testIsCassandraValueObjectDecimal()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Decimal(microtime(true))));
    }

    public function testIsCassandraValueObjectBigint()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Bigint(100500)));
    }

    public function testIsCassandraValueObjectSmallint()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Smallint(1050)));
    }

    public function testIsCassandraValueObjectTinyint()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Tinyint(15)));
    }

    public function testIsCassandraValueObjectVariant()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Varint("157")));
    }

    public function testIsCassandraValueObjectBlob()
    {
        $model = new Item();
        $this->assertTrue($model->isCassandraValueObject(new \Cassandra\Blob("SOMETHING")));
    }

    public function testIsCassandraValueObjectMap()
    {
        $model = new Item();
        $map = new \Cassandra\Map(\Cassandra\Type::int(), \Cassandra\Type::float());
        $map->set(1, new \Cassandra\Float(1.5));
        $map->set(3, new \Cassandra\Float(3.5));
        $this->assertTrue($model->isCassandraValueObject($map));
    }

    public function testIsCassandraValueObjectSet()
    {
        $model = new Item();
        $set = new \Cassandra\Set(\Cassandra\Type::float());
        $set->add(new \Cassandra\Float(2.5));
        $set->add(new \Cassandra\Float(4.5));
        $this->assertTrue($model->isCassandraValueObject($set));
    }

    public function testIsCassandraValueObjectTuple()
    {
        $model = new Item();
        $tuple = new \Cassandra\Tuple([\Cassandra\Type::int(), \Cassandra\Type::uuid()]);
        $tuple->set(1, new \Cassandra\Uuid("550e8400-e29b-41d4-a716-446655440000"));
        $this->assertTrue($model->isCassandraValueObject($tuple));
    }

    public function testIsCassandraValueObjectUserType()
    {
        $model = new Item();
        $addressType = Cassandra\Type::userType(
            'street', Cassandra\Type::text(),
            'city', Cassandra\Type::text(),
            'zip', Cassandra\Type::int()
        );
        $addressesType = Cassandra\Type::userType(
            'home', $addressType,
            'work', $addressType
        );
        $userTypeValue = $addressesType->create(
            'home', $addressType->create(
            'city', 'New York',
            'street', '1000 Database Road',
            'zip', 10025),
            'work', $addressType->create(
            'city', 'New York',
            'street', '60  SSTable Drive',
            'zip', 10024)
        );
        $this->assertTrue($model->isCassandraValueObject($userTypeValue));
    }


    public function testGetCassandraObjectValueDate()
    {
        $model = new Item();

        $value = $model->valueFromCassandraObject(new \Cassandra\Date(time()));

        $this->assertEquals(strtotime(date('Y-m-d 00:00:00', time())), $value);
    }

    public function testGetCassandraObjectValueTime()
    {
        $model = new Item();
        $value = $model->valueFromCassandraObject(new \Cassandra\Time(time()));
        $this->assertEquals(time(), $value);
    }

    public function testGetCassandraObjectValueTimestamp()
    {
        $model = new Item();
        $value = $model->valueFromCassandraObject(new \Cassandra\Timestamp(time()));
        $this->assertEquals(time(), $value);
    }

//    public function testGetCassandraObjectValueDuration()
//    {
//        $model = new Item();
//        $value = $model->valueFromCassandraObject(new \Cassandra\Duration(1, 2, 3));
//    }

    public function testGetCassandraObjectValueUUID()
    {
        $model = new Item();
        $value = $model->valueFromCassandraObject(new \Cassandra\Uuid("550e8400-e29b-41d4-a716-446655440000"));
        $this->assertEquals("550e8400-e29b-41d4-a716-446655440000", $value);
    }

    public function testGetCassandraObjectValueTimeUUID()
    {
        $model = new Item();
        $uuid = new \Cassandra\Timeuuid(time());
        $value = $model->valueFromCassandraObject($uuid);
        $this->assertEquals($uuid->uuid(), $value);
    }

    public function testGetCassandraObjectValueInet()
    {
        $model = new Item();
        $this->assertEquals("127.1.2.3", $model->valueFromCassandraObject(new \Cassandra\Inet("127.1.2.3")));
    }

    public function testGetCassandraObjectValueFloat()
    {
        $model = new Item();
        $this->assertEquals(3.5, $model->valueFromCassandraObject(new \Cassandra\Float(3.5)));
    }

    public function testGetCassandraObjectValueDecimal()
    {
        $model = new Item();
        $mTime = microtime(true);
        $decimal = new \Cassandra\Decimal($mTime);
        $this->assertEquals($mTime * pow(10, $decimal->scale()), $model->valueFromCassandraObject($decimal));
    }

    public function testGetCassandraObjectValueBigint()
    {
        $model = new Item();
        $this->assertEquals(100500, $model->valueFromCassandraObject(new \Cassandra\Bigint(100500)));
    }

    public function testGetCassandraObjectValueSmallint()
    {
        $model = new Item();
        $this->assertEquals(1050, $model->valueFromCassandraObject(new \Cassandra\Smallint(1050)));
    }

    public function testGetCassandraObjectValueTinyint()
    {
        $model = new Item();
        $this->assertEquals(15, $model->valueFromCassandraObject(new \Cassandra\Tinyint(15)));
    }

    public function testGetCassandraObjectValueVariant()
    {
        $model = new Item();
        $this->assertEquals(157, $model->valueFromCassandraObject(new \Cassandra\Varint("157")));
    }

    public function testGetCassandraObjectValueBlob()
    {
        $model = new Item();
        $this->assertEquals("SOMETHING", $model->valueFromCassandraObject(new \Cassandra\Blob("SOMETHING")));
    }

    public function testGetCassandraObjectValueMap()
    {
        $model = new Item();
        $map = new \Cassandra\Map(\Cassandra\Type::int(), \Cassandra\Type::float());
        $map->set(1, new \Cassandra\Float(1.5));
        $map->set(3, new \Cassandra\Float(3.5));

        $this->assertEquals([1 => 1.5, 3 => 3.5], $model->valueFromCassandraObject($map));
    }

    public function testGetCassandraObjectValueSet()
    {
        $model = new Item();
        $set = new \Cassandra\Set(\Cassandra\Type::float());
        $set->add(new \Cassandra\Float(2.5));
        $set->add(new \Cassandra\Float(4.5));
        $this->assertEquals([2.5, 4.5], $model->valueFromCassandraObject($set));
    }

    public function testGetCassandraObjectValueTuple()
    {
        $model = new Item();
        $tuple = Cassandra\Type::tuple(\Cassandra\Type::int(), Cassandra\Type::uuid());
        $tuple = $tuple->create(3, new \Cassandra\Uuid("550e8400-e29b-41d4-a716-446655440000"));

        $this->assertEquals([3, "550e8400-e29b-41d4-a716-446655440000"], $model->valueFromCassandraObject($tuple));
    }

    public function testGetCassandraObjectValueCollection()
    {
        $model = new Item();
        $collection = new \Cassandra\Collection(\Cassandra\Type::int());
        $collection->add(1);
        $collection->add(3);
        $collection->add(5);
        $collection->add(7);
        $collection->add(9);

        $this->assertSame([1, 3, 5, 7, 9], $model->valueFromCassandraObject($collection));
    }

    public function testGetCassandraObjectValueUserType()
    {
        $model = new Item();
        $addressType = Cassandra\Type::userType(
            'street', Cassandra\Type::text(),
            'city', Cassandra\Type::text(),
            'zip', Cassandra\Type::int()
        );
        $addressesType = Cassandra\Type::userType(
            'home', $addressType,
            'work', $addressType
        );
        $userTypeValue = $addressesType->create(
            'home', $addressType->create(
            'city', 'New York',
            'street', '1000 Database Road',
            'zip', 10025),
            'work', $addressType->create(
            'city', 'New York',
            'street', '60  SSTable Drive',
            'zip', 10024)
        );

        $this->assertEquals([
            'home' => [
                'city' => 'New York',
                'street' => '1000 Database Road',
                'zip' => 10025
            ],
            'work' => [
                'city' => 'New York',
                'street' => '60  SSTable Drive',
                'zip' => 10024
            ],
        ], $model->valueFromCassandraObject($userTypeValue));
    }

    public function testGetCassandraObjectValueArray()
    {
        $model = new Item();

        $time = time();
        $arr = [
            new \Cassandra\Float(2.5),
            new \Cassandra\Inet("127.1.2.3"),
            new \Cassandra\Date($time)
        ];

        $this->assertEquals([
            2.5,
            "127.1.2.3",
            strtotime(date('Y-m-d 00:00:00', $time)),
        ], $model->valueFromCassandraObject($arr));
    }

    public function testGetCassandraObjectValueObject()
    {
        $model = new Item();

        $this->assertSame($model, $model->valueFromCassandraObject($model));
    }

    public function testGetCassandraObjectValueBaseType()
    {
        $model = new Item();

        $this->assertEquals(321, $model->valueFromCassandraObject(321));
        $this->assertEquals(2.23, $model->valueFromCassandraObject(2.23));
        $this->assertEquals("string", $model->valueFromCassandraObject("string"));
        $this->assertEquals(false, $model->valueFromCassandraObject(false));
        $this->assertEquals(null, $model->valueFromCassandraObject(null));
        $this->assertEquals(function () {}, $model->valueFromCassandraObject(function () {}));
    }

}
