package com.atguigu.hbase

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, NamespaceDescriptor, TableName}

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
        
        //        createTable("hbase1", "cf1", "cf2")
        //        deleteTable("hbase1")
        //        createNS("abc")
        //        closeConnection()
        
        createTable("test1", "info")
    }
    
    def createNS(name: String) = {
        val admin: Admin = conn.getAdmin
        if (!nsExists(name)) {
            val nd: NamespaceDescriptor.Builder = NamespaceDescriptor.create(name)
            admin.createNamespace(nd.build())
        } else {
            println(s"你要创建的命名空间: ${name}已经存在")
        }
        admin.close()
    }
    
    def nsExists(name: String): Boolean = {
        val admin: Admin = conn.getAdmin
        val nss: Array[NamespaceDescriptor] = admin.listNamespaceDescriptors()
        val r = nss.map(_.getName).contains(name)
        admin.close()
        r
    }
    
    
    def deleteTable(name: String) = {
        val admin: Admin = conn.getAdmin
        if (tableExists(name)) {
            admin.disableTable(TableName.valueOf(name))
            admin.deleteTable(TableName.valueOf(name))
        }
        
        admin.close()
    }
    
    /*
     * 创建指定的表
     *
     * @param name
    */
    def createTable(name: String, cfs: String*): Boolean = {
        val admin: Admin = conn.getAdmin
        val tableName = TableName.valueOf(name)
        
        if (tableExists(name)) return false
        
        
        val td: TableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName)
        
        cfs.foreach(cf => {
            val cfd = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(cf))
                
                .build()
            td.setColumnFamily(cfd)
        })
        
        //        admin.createTable(td.build())
        val splites = Array(Bytes.toBytes("aaa"), Bytes.toBytes("bbb"), Bytes.toBytes("ccc"))
        admin.createTable(td.build(), splites)
        admin.close()
        true
    }
    
    /*
    *
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

