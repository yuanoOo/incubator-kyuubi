package cn.jxau

import org.apache.kyuubi.service.authentication.PasswdAuthenticationProvider

import java.sql.{Connection, DriverManager}
import javax.security.sasl.AuthenticationException

class SimpleAuthenticationProvider extends PasswdAuthenticationProvider {

  override def authenticate(user: String, password: String): Unit = {

    val pwd: String = ConnectionFactory().authById(user)

    if (pwd.equals(""))
      throw new AuthenticationException(s"auth fail, no user")
    else if (!pwd.equals(password))
      throw new AuthenticationException(s"auth fail, pwd wrong")
  }

}

case class ConnectionFactory() {

  val database = "test"
  val table = "tb_score"

  // 访问本地MySQL服务器，通过3306端口访问mysql数据库
  val url = s"jdbc:mysql://172.29.130.156:3306/$database?useUnicode=true&characterEncoding=utf-8&useSSL=false"
  //驱动名称
  val driver = "com.mysql.cj.jdbc.Driver"

  //用户名
  val username = "root"
  //密码
  val password = "1234"
  //初始化数据连接
  var connection: Connection = _

  def authById(id: String): String ={
    var pwd = ""

    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement

      //执行查询语句，并返回结果
      val rs = statement.executeQuery(s"SELECT subject FROM $table WHERE userid = $id")

      //打印返回结果
      while (rs.next) {
        pwd = rs.getString("subject")
      }

      pwd match {
        case "" => ""
        case _ => pwd
      }

    } catch {
      case exception: Exception => {
        exception.printStackTrace()
        throw exception
      }
    }finally {
      if (connection != null){
        connection.close()
      }
    }
  }

  def apply(): ConnectionFactory = ConnectionFactory()

}