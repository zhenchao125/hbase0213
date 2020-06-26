package test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


/**
 * Author atguigu
 * Date 2020/6/26 10:47
 */
object PhoenixTest {
    def main(args: Array[String]): Unit = {
        // 本质: 就是通过jdbc访问phoenix
        // 1. 建立连接
        val url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181"
        println(url)
        val conn: Connection = DriverManager.getConnection(url)
        // 2. PrepareState
        val ps: PreparedStatement = conn.prepareStatement("select * from person")
        // 3. 执行
        val resultSet: ResultSet = ps.executeQuery()
        // 4. 解析结果
        while (resultSet.next()) {
            val id: String = s"id=${resultSet.getString(1)}, name=${resultSet.getString(2)}, age=${resultSet.getLong(3)}"
            println(id)
        }
        
        // 5. 关闭连接
        conn.close()
        
    }
}
