import cn.jxau.SimpleAuthenticationProvider

import javax.security.sasl.AuthenticationException
import org.scalatest.funsuite.AnyFunSuite

class SimpleAuthenticationProviderTest extends AnyFunSuite {

  test("test my authn provider") {
    val provider = new SimpleAuthenticationProvider

    provider.authenticate("001", "英语")

  }
}
