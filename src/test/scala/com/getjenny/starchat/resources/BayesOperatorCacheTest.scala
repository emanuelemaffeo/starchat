package com.getjenny.starchat.resources

import com.getjenny.starchat.services.BayesOperatorCacheService

class BayesOperatorCacheTest extends TestBase {

  private[this] val bayesOperatorCache = BayesOperatorCacheService

  "StarChat" should {

    "insert value in cache" in {
      bayesOperatorCache.put("test", 5d)

      val value = bayesOperatorCache.get("test")
      assert(value.isDefined)
      assert(value.get == 5d)
    }

    "update value in cache" in {
      bayesOperatorCache.put("test", 8d)

      val value = bayesOperatorCache.get("test")
      assert(value.isDefined)
      assert(value.get == 8d)
    }
  }


}
