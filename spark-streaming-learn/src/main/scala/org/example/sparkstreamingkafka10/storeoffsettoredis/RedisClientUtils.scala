package org.example.sparkstreamingkafka10.storeoffsettoredis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool


/**
 * Created by atyongsi@163.com on 2020/8/19
 * Description:redis连接池
 */
object RedisClientUtils extends Serializable {

  @transient private var pool: JedisPool = null

  //初始化redis连接池
  def initRedisPool() = {
    // Redis configurations
    val maxTotal = 20
    val maxIdle = 10
    val minIdle = 1
    val redisHost = "127.0.0.1"
    val redisPort = 6379
    val redisTimeout = 30000
    RedisClientUtils.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
  }


  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int): Unit = {
    makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
    if (pool == null) {
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)
      pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout, "root_jinkun")

      val hook = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool: JedisPool = {
    assert(pool != null)
    pool
  }
}
