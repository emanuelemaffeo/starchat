package com.getjenny.starchat.resources

import com.getjenny.starchat.services.BayesOperatorCacheService

class BayesOperatorCacheTest extends TestBase {

  private[this] val bayesOperatorCache = BayesOperatorCacheService

  "StarChat" should {

    "insert value in cache" in {
      bayesOperatorCache.put("test", 5d)

      val value = bayesOperatorCache.get("test")

      assert(value === Some(5d))
    }

    "update value in cache" in {
      bayesOperatorCache.put("test", 8d)

      val value = bayesOperatorCache.get("test")

      assert(value === Some(8d))
    }

    "remove from cache" in {
      bayesOperatorCache.put("test2", 5d)
      bayesOperatorCache.put("test3", 5d)
      bayesOperatorCache.refresh()
      bayesOperatorCache.clear()

      val value = bayesOperatorCache.get("test")
      val value2 = bayesOperatorCache.get("test2")
      val value3 = bayesOperatorCache.get("test3")

      assert(value.isEmpty)
      assert(value2.isEmpty)
      assert(value3.isEmpty)
    }
  }


}
