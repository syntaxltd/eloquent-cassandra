<?php

namespace AHAbid\EloquentCassandra\Tests\Units;

use AHAbid\EloquentCassandra\Tests\TestCase;
use AHAbid\EloquentCassandra\Fixtures\Models\User;

class CollectionTest extends TestCase
{
    protected function getEnvironmentSetUp($app)
    {
        parent::getEnvironmentSetUp($app);

        \Illuminate\Support\Facades\DB::connection('cassandra')->select('TRUNCATE users');

        $faker = \Faker\Factory::create();
        for($i = 1; $i <= 20; $i++) {
            \Illuminate\Support\Facades\DB::connection('cassandra')
                ->table('users')
                ->insert([
                    'id' => $i,
                    'name' => $faker->name(),
                    'title' => $faker->title(),
                    'age' => rand(18, 40),
                    'note1' => $faker->sentence(),
                    'note2' => $faker->sentence(),
                    'birthday' => new \Cassandra\Timestamp(time()),
                    'created_at' => new \Cassandra\Timestamp(time()),
                    'updated_at' => new \Cassandra\Timestamp(time()),
                ]);
        }
    }

    public function testModelResponseIsCollection()
    {
        $result = User::get();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $result);
    }

    public function testCorrectResultsAmount()
    {
        $result = User::get();
        $this->assertEquals(20, $result->count());
    }

    public function testCorrectPageResultsAmount()
    {
        $result = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $result->count());
    }

    public function testCorrectResultsAmountWithSetPageSize()
    {
        $result = User::setPageSize(5)->get();
        $this->assertEquals(20, $result->count());
    }

    public function testCollectionIsLastPage()
    {
        $result = User::setPageSize(5)->getPage();
        $this->assertFalse($result->isLastPage());

        $result = User::setPageSize()->get();
        $this->assertTrue($result->isLastPage());

        $result = User::get();
        $this->assertTrue($result->isLastPage());

        $result = User::setPageSize(5)->get();
        $this->assertTrue($result->isLastPage());
    }

    public function testGetNextPage()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $result */
        $result = User::setPageSize(11)->getPage();
        $this->assertFalse($result->isLastPage());

        $nextPageResults = $result->nextPage();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $nextPageResults);
        $this->assertTrue($nextPageResults->isLastPage());

        $nextPageResults2 = $nextPageResults->nextPage();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $nextPageResults2);
        $this->assertTrue($nextPageResults2->isEmpty());
    }

    public function testGetNextPageToken()
    {
        $result = User::setPageSize(5)->getPage();
        $this->assertFalse($result->isLastPage());
        $token = $result->getNextPageToken();

        $this->assertNotEmpty($token);
    }

    public function testGetNextPageTokenValid()
    {
        $result = User::setPageSize(11)->getPage();
        $token = $result->getNextPageToken();

        $nextPageResults = User::setPaginationStateToken($token)->getPage();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $nextPageResults);
        $this->assertTrue($nextPageResults->isLastPage());
    }

    public function testPageCollectionItemsAreModels()
    {
        $results = User::setPageSize(5)->getPage();
        foreach ($results as $result) {
            $this->assertInstanceOf(User::class, $result);
        }
    }

    public function testCollectionItemsAreModels()
    {
        $results = User::get();
        foreach ($results as $result) {
            $this->assertInstanceOf(User::class, $result);
        }
    }

    public function testPageFindInCollectionByKeyValue()
    {
        $result = User::setPageSize(5)->getPage();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $result);

        $searchResults = $result->find(10);
        $this->assertTrue($searchResults->id == 10);
    }

    public function testFindInCollectionByKeyValue()
    {
        $result = User::get();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $result);

        $searchResults = $result->find(10);
        $this->assertTrue($searchResults->id == 10);

    }

    public function testFindInCollectionByModel()
    {
        $result = User::get();
        $user = User::first();

        $searchResults = $result->find($user);

        $this->assertEquals($searchResults, $user);
    }

    public function testFindInCollectionByEmptyArrayable()
    {
        $searchValues = collect([]);
        $result = User::get();
        $searchResults = $result->find($searchValues);

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $searchResults);
        $this->assertEquals(0, $searchResults->count());
    }

    public function testFindInCollectionByArrayable()
    {
        $searchValues = collect([1,2,3,4,5]);
        $result = User::get();
        $searchResults = $result->find($searchValues);

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $searchResults);
        $this->assertEquals(5, $searchResults->count());
        foreach ($searchResults as $item) {
            $this->assertTrue($searchValues->contains($item->id));
        }
    }

    public function testFindInEmptyCollectionByArrayable()
    {
        $searchValues = collect([1,2,3,4,5]);
        $result = User::where('id', '>', 40)->allowFiltering(true)->get();
        $searchResults = $result->find($searchValues);

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $searchResults);
        $this->assertEquals(0, $searchResults->count());
    }

    public function testPageGetRows()
    {
        $result = User::setPageSize(5)->getPage();
        $rows = $result->getRows();

        $this->assertInstanceOf(\Cassandra\Rows::class, $rows);
        $this->assertEquals($rows->count(), $result->count());
    }

    public function testAllPagesGetRows()
    {
        $result = User::get();
        $rows = $result->getRows();

        $this->assertNull($rows);
    }

    public function testCollectionGetDictionary()
    {
        $results = User::get();
        $dictionary = $results->getDictionary();

        $this->assertTrue(is_array($dictionary));
        $this->assertEquals(count($dictionary), $results->count());

        foreach ($dictionary as $key => $value) {
            $this->assertEquals($results->find($key), $value);
        }
    }

    public function testCollectionExcept()
    {
        $results = User::get();

        $exceptKeys = [1,2];
        $exceptResults = $results->except($exceptKeys);

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $exceptResults);
        $this->assertEquals($exceptResults->count(), ($results->count() - count($exceptKeys)));

        foreach ($exceptResults as $value) {
            $this->assertFalse(in_array($value->getKey(), $exceptKeys));
        }
    }

    public function testCollectionOnly()
    {
        $results = User::get();

        $onlyKeys = [1, 2];

        $onlyResults = $results->only($onlyKeys);
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $onlyResults);
        $this->assertEquals($onlyResults->count(), count($onlyKeys));

        foreach ($onlyResults as $value) {
            $this->assertTrue(in_array($value->getKey(), $onlyKeys));
        }
    }

    public function testCollectionOnlyNullArgument()
    {
        $results = User::get();

        $onlyResults = $results->only(null);

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $onlyResults);
        $this->assertEquals($onlyResults->count(), $results->count());

        foreach ($onlyResults as $value) {
            $this->assertTrue($results->contains($value));
        }
    }

    public function testUniqueDuplicate()
    {
        $results = User::get();
        $duplicate = User::first();

        $originCount = $results->count();
        $resultsWithDuplicate = $results->push($duplicate);

        $this->assertEquals($resultsWithDuplicate->count(), $originCount + 1);

        $uniqueResults = $resultsWithDuplicate->unique('name');

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $uniqueResults);
        $this->assertEquals($uniqueResults->count(), $originCount);
    }

    public function testCollectionUnique()
    {
        User::create([
            'id' => 101,
            'name' => 'Tester',
            'title' => 'Mr.',
            'age' => rand(18, 40),
            'note1' => '',
            'note2' => '',
            'birthday' => new \Cassandra\Timestamp(time()),
            'created_at' => new \Cassandra\Timestamp(time()),
            'updated_at' => new \Cassandra\Timestamp(time()),
        ]);
        User::create([
            'id' => 102,
            'name' => 'Tester',
            'title' => 'Mr.',
            'age' => rand(18, 40),
            'note1' => '',
            'note2' => '',
            'birthday' => new \Cassandra\Timestamp(time()),
            'created_at' => new \Cassandra\Timestamp(time()),
            'updated_at' => new \Cassandra\Timestamp(time()),
        ]);

        $results = User::where('id', '>', 15)->allowFiltering(true)->get();
        $this->assertEquals(7, $results->count());

        $uniqueResults = $results->unique('name');
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $uniqueResults);
        $this->assertEquals($uniqueResults->count(), $results->count() - 1);
    }

    public function testCollectionUniqueWithEmptyKey()
    {
        $results = User::where('id', '>', 15)
            ->allowFiltering(true)
            ->get();

        $this->assertEquals(5, $results->count());

        $first = $results->first();
        $results = $results->push($first);
        $results = $results->push($first);

        $this->assertEquals(7, $results->count());

        $uniqueResults = $results->unique();

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $uniqueResults);
        $this->assertEquals(5, $uniqueResults->count());
    }

    public function testCollectionMerge()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $results */
        $results = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $results->count());

        $additionalResults = $results->nextPage();
        $this->assertEquals(5, $additionalResults->count());

        $mergeResults = $results->merge($additionalResults);
        $this->assertEquals(10, $mergeResults->count());

        foreach ($mergeResults as $item) {
            $this->assertTrue($results->contains($item) || $additionalResults->contains($item));
        }
    }

    public function testCollectionMergeWithDuplicates()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $results */
        $results = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $results->count());

        /** @var \AHAbid\LaravelCassandra\Collection $additionalResults */
        $additionalResults = User::setPageSize(7)->getPage();
        $this->assertEquals(7, $additionalResults->count());

        $mergeResults = $results->merge($additionalResults);
        $this->assertEquals(7, $mergeResults->count());

        foreach ($mergeResults as $item) {
            $this->assertTrue($results->contains($item) || $additionalResults->contains($item));
        }
    }

    public function testCollectionIntersect()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $results */
        $results = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $results->count());

        /** @var \AHAbid\LaravelCassandra\Collection $additionalResults */
        $additionalResults = User::setPageSize(7)->getPage();
        $this->assertEquals(7, $additionalResults->count());

        $intersectResults = $results->intersect($additionalResults);
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $intersectResults);
        $this->assertEquals(5, $intersectResults->count());

        foreach ($intersectResults as $item) {
            $this->assertTrue($results->contains($item));
        }
    }

    public function testCollectionDiff()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $results */
        $results = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $results->count());

        /** @var \AHAbid\LaravelCassandra\Collection $additionalResults */
        $additionalResults = User::setPageSize(7)->getPage();
        $this->assertEquals(7, $additionalResults->count());

        $diffResults = $results->diff($additionalResults);
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $diffResults);
        $this->assertEquals(0, $diffResults->count());

        $diffResults = $additionalResults->diff($results);
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $diffResults);
        $this->assertEquals(2, $diffResults->count());

        foreach ($diffResults as $item) {
            $this->assertTrue($additionalResults->contains($item) && !$results->contains($item));
        }
    }

    public function testCollectionFresh()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $results */
        $results = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $results->count());

        $fresh = $results->fresh();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $fresh);
        $this->assertEquals(5, $fresh->count());

        foreach ($fresh as $item) {
            $this->assertInstanceOf(User::class, $item);
        }
    }

    public function testCollectionFreshWithoutDeleted()
    {
        /** @var \AHAbid\LaravelCassandra\Collection $results */
        $results = User::setPageSize(5)->getPage();
        $this->assertEquals(5, $results->count());

        $results->first()->delete();

        $fresh = $results->fresh();
//        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $fresh);
        $this->assertEquals(5, $fresh->count());

        $notEmptyResults = $fresh->filter(function ($item) {
            return $item !== null;
        });
        $this->assertEquals(4, $notEmptyResults->count());

        $emptyResults = $fresh->filter(function ($item) {
            return $item === null;
        });
        $this->assertEquals(1, $emptyResults->count());
    }

    public function testCollectionFreshForEmptyCollection()
    {
        $collection = new \AHAbid\LaravelCassandra\Collection();
        $this->assertEquals(0, $collection->count());

        $fresh = $collection->fresh();
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $fresh);
        $this->assertEquals(0, $fresh->count());
    }

    public function testCollectionFreshWithFakeModels()
    {
        $users = [];
        for ($i = 1; $i <= 3; $i++) {
            $user = new User();
            $user->id = $i;
            $users[] = $user;
        }

        $collection = new \AHAbid\LaravelCassandra\Collection($users, reset($users));
        $this->assertEquals(3, $collection->count());

        $fresh = $collection->fresh();
//        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $fresh);
        $this->assertEquals(3, $fresh->count());

        foreach ($fresh->all() as $user) {
            $this->assertNull($user);
        }
    }

    public function testCollectionGetRowFromCollectionWithoutRows()
    {
        $results = User::setPageSize(5)->getPage();
        $collection = new \AHAbid\LaravelCassandra\Collection($results->values()->all());

        $this->assertNull($collection->getRows());
    }

    public function testCollectionGetNextPageTokenFromCollectionWithoutRows()
    {
        $results = User::setPageSize(5)->getPage();
        $collection = new \AHAbid\LaravelCassandra\Collection($results->values()->all());

        $this->assertNull($collection->getNextPageToken());
    }

    public function testCollectionNextPageTokenFromCollectionWithoutRows()
    {
        $results = User::setPageSize(5)->getPage();
        $collection = new \AHAbid\LaravelCassandra\Collection($results->values()->all());

        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $collection->nextPage());
        $this->assertTrue($collection->nextPage()->isEmpty());
    }

    public function testCollectionAppendNextPageFromCollectionWithoutRows()
    {
        $results = User::setPageSize(5)->getPage();
        $collection = new \AHAbid\LaravelCassandra\Collection($results->values()->all());

        $this->assertEquals(5, $collection->count());
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $collection->appendNextPage());
        $this->assertEquals(5, $collection->count());
    }

    public function testCollectionAppendNextPage()
    {
        $results = User::setPageSize(5)->getPage();

        $this->assertEquals(5, $results->count());
        $this->assertInstanceOf(\AHAbid\LaravelCassandra\Collection::class, $results->appendNextPage());
        $this->assertEquals(10, $results->count());
    }
}