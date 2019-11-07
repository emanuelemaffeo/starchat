package com.getjenny.starchat.resources

import com.getjenny.starchat.services.{InstanceRegistryDocument, InstanceRegistryService}

class InstanceRegistryTest extends TestBase {

  val instanceRegistry: InstanceRegistryService.type = InstanceRegistryService
  val indexName = "index_getjenny_english_test_0"
  val indexNameCommon = "index_getjenny_english_common_0"
  val invalidIndexName = "starchat_system.index_test"
  val timestamp = System.currentTimeMillis()

  "Instance Registry" should {

    "fail if index name is not valid" in {
       intercept[Exception]{
         instanceRegistry.addInstance(invalidIndexName)
      }
    }

    "add and enable an instance" in {
      instanceRegistry.addInstance(indexName)
      instanceRegistry.addInstance(indexNameCommon)

      val instance = instanceRegistry.getInstance(indexName)
      val instance2 = instanceRegistry.getInstance(indexNameCommon)

      assert(instance.enabled.isDefined)
      assert(instance.enabled.get)
      assert(instance2.enabled.isDefined)
      assert(instance2.enabled.get)
    }

    "fail if trying to search an invalid index name" in {
      intercept[Exception] {
        instanceRegistry.getInstance(invalidIndexName)
      }
    }

    "fail if trying to update instance with invalid index name" in {
      intercept[Exception] {
        instanceRegistry.disableInstance(invalidIndexName)
      }
    }

    "disable instance" in {
      instanceRegistry.disableInstance(indexName)

      val instance = instanceRegistry.getInstance(indexName)
      assert(instance.enabled.isDefined)
      assert(!instance.enabled.get)
    }

    "mark delete instance" in {
      instanceRegistry.markDeleteInstance(indexName)

      val instance = instanceRegistry.getInstance(indexName)
      assert(instance.delete.isDefined)
      assert(instance.delete.get)
    }

    "update timestamp" in {
      instanceRegistry.updateTimestamp(indexName, timestamp, 1)

      val instance = instanceRegistry.instanceTimestamp(indexName)

      assert(instance.timestamp.equals(timestamp))
    }

    "get all instances" in {

      val allInstances = instanceRegistry.getAll

      assert(allInstances.nonEmpty)
      assert(allInstances.size == 2)
    }

    "get all instances timestamp" in {

      val allInstances = instanceRegistry.allInstanceTimestamp()

      assert(allInstances.nonEmpty)
      assert(allInstances.size == 2)

      val instancesMap = allInstances.map(x => x.indexName -> x.timestamp).toMap

      assert(instancesMap(indexName) == timestamp)
      assert(instancesMap(indexNameCommon) == 0)
    }

    "delete entry" in {
      instanceRegistry.deleteEntry(List(indexName))

      val instance = instanceRegistry.getInstance(indexName)

      assert(instance.equals(InstanceRegistryDocument.empty))
    }
  }

}
