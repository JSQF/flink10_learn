package com.yyb.flink10.table.blink.batch

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
  * @Author yyb
  * @Description
  * @Date Create in 2020-04-20
  * @Time 13:33
  */
object BlinkHiveBatchDemo {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val bbTableEnv = TableEnvironment.create(settings)

    val name = "myhive"
    val defaultDatabase = "flink"
//    val hiveConfDir = "src/main/resources/" //hive-site.xml的本地目录 ，注意 当有 hive-site.xml 在 resources 下的时候 ，hiveConfDir 也需要设置,否则会提示
//Required table missing : "DBS" in Catalog "" Schema "". DataNucleus requires this table to perform its persistence operations. Either your MetaData is incorrect, or you need to enable "datanucleus.schema.autoCreateTables"
    val hiveConfDir = this.getClass.getResource("/").getFile  //可以通过这一种方式设置 hiveConfDir，这样的话，开发与测试和生产环境可以保持一致

    val version = "2.3.6"
    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)

    bbTableEnv.registerCatalog("myhive", hive)
    bbTableEnv.useCatalog("myhive")

    //注意 查询语句 myhive.flink.a myhive是你的Hcatalog的别称，flink是库名称，a是别名称
    bbTableEnv.sqlQuery("select * from myhive.flink.a").printSchema()


//    bbTableEnv.execute("BlinkHiveBatchDemo")

  }
}
