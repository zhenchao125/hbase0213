package com.atguigu.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
 * Author atguigu
 * Date 2020/6/24 9:22
 */
object HbaseDDL {
    // 1. 先获取到hbase的连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    val conn: Connection = ConnectionFactory.createConnection(conf)
    
    def main(args: Array[String]): Unit = {
//        println(tableExists("user"))
        
        createTable("hbase1", "cf1", "cf2")
        closeConnection()
    }
    
    /**
     * 创建指定的表
     *
     * @param name
     */
    def createTable(name: String, cfs: String*): Boolean = {
        val admin: Admin = conn.getAdmin
        val tableName = TableName.valueOf(name)
        
        if(tableExists(name)) return false
        
        
        val td: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
        
        cfs.foreach(cf => {
            val cfd = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(cf))
                .build()
            td.setColumnFamily(cfd)
        })
        
        admin.createTable(td.build())
        admin.close()
        true
    }
    
    /**
     * 判断表是否存在
     *
     * @param name
     * @return
     */
    def tableExists(name: String): Boolean = {
        
        // 2. 获取管理对象 Admin
        val admin: Admin = conn.getAdmin
        // 3. 利用Admin进行各种操作
        val tableName: TableName = TableName.valueOf(name)
        val b = admin.tableExists(tableName)
        // 4. 关闭Admin
        admin.close()
        b
    }
    
    
    def closeConnection() = conn.close()
}
