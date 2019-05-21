<?php


class CollectionTest extends TestCase
{
    protected function getEnvironmentSetUp($app)
    {
        parent::getEnvironmentSetUp($app);

        $faker = Faker\Factory::create();
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
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Collection::class, $result);
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
        /** @var \lroman242\LaravelCassandra\Collection $result */
        $result = User::setPageSize(11)->getPage();
        $this->assertFalse($result->isLastPage());

        $nextPageResults = $result->nextPage();
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Collection::class, $nextPageResults);
        $this->assertTrue($nextPageResults->isLastPage());

        $nextPageResults2 = $nextPageResults->nextPage();
        $this->assertNull($nextPageResults2);
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
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Collection::class, $nextPageResults);
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

    public function testPageFindInCollection()
    {
        $result = User::setPageSize(5)->getPage();
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Collection::class, $result);

        $searchResults = $result->find(10);
        $this->assertTrue($searchResults->id == 10);
    }

    public function testFindInCollection()
    {
        $result = User::get();
        $this->assertInstanceOf(\lroman242\LaravelCassandra\Collection::class, $result);

        $searchResults = $result->find(10);
        $this->assertTrue($searchResults->id == 10);

    }
}