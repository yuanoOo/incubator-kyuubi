/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.server

import java.util.EnumSet
import java.util.concurrent.{Future, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import javax.servlet.DispatcherType
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response.Status

import com.google.common.annotations.VisibleForTesting
import org.apache.hadoop.conf.Configuration
import org.eclipse.jetty.servlet.FilterHolder

import org.apache.kyuubi.{KyuubiException, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{FRONTEND_REST_BIND_HOST, FRONTEND_REST_BIND_PORT, FRONTEND_REST_MAX_WORKER_THREADS, METADATA_RECOVERY_THREADS}
import org.apache.kyuubi.server.api.v1.ApiRootResource
import org.apache.kyuubi.server.http.authentication.{AuthenticationFilter, KyuubiHttpAuthenticationFactory}
import org.apache.kyuubi.server.ui.JettyServer
import org.apache.kyuubi.service.{AbstractFrontendService, Serverable, Service, ServiceUtils}
import org.apache.kyuubi.service.authentication.KyuubiAuthenticationFactory
import org.apache.kyuubi.session.{KyuubiSessionManager, SessionHandle}
import org.apache.kyuubi.util.ThreadUtils

/**
 * A frontend service based on RESTful api via HTTP protocol.
 * Note: Currently, it only be used in the Kyuubi Server side.
 */
class KyuubiRestFrontendService(override val serverable: Serverable)
  extends AbstractFrontendService("KyuubiRestFrontendService") {

  private var server: JettyServer = _

  private val isStarted = new AtomicBoolean(false)

  private def hadoopConf: Configuration = KyuubiServer.getHadoopConf()

  private def sessionManager = be.sessionManager.asInstanceOf[KyuubiSessionManager]

  private val batchChecker = ThreadUtils.newDaemonSingleThreadScheduledExecutor("batch-checker")

  lazy val host: String = conf.get(FRONTEND_REST_BIND_HOST)
    .getOrElse {
      if (conf.get(KyuubiConf.FRONTEND_CONNECTION_URL_USE_HOSTNAME)) {
        Utils.findLocalInetAddress.getCanonicalHostName
      } else {
        Utils.findLocalInetAddress.getHostAddress
      }
    }

  override def initialize(conf: KyuubiConf): Unit = synchronized {
    this.conf = conf
    server = JettyServer(
      getName,
      host,
      conf.get(FRONTEND_REST_BIND_PORT),
      conf.get(FRONTEND_REST_MAX_WORKER_THREADS))
    super.initialize(conf)
  }

  override def connectionUrl: String = {
    checkInitialized()
    server.getServerUri
  }

  private def startInternal(): Unit = {
    val contextHandler = ApiRootResource.getServletHandler(this)
    val holder = new FilterHolder(new AuthenticationFilter(conf))
    contextHandler.addFilter(holder, "/v1/*", EnumSet.allOf(classOf[DispatcherType]))
    val authenticationFactory = new KyuubiHttpAuthenticationFactory(conf)
    server.addHandler(authenticationFactory.httpHandlerWrapperFactory.wrapHandler(contextHandler))

    server.addStaticHandler("org/apache/kyuubi/ui/static", "/static/")
    server.addRedirectHandler("/", "/static/")
    server.addRedirectHandler("/static", "/static/")
    server.addStaticHandler("META-INF/resources/webjars/swagger-ui/4.9.1/", "/swagger-static/")
    server.addStaticHandler("org/apache/kyuubi/ui/swagger", "/swagger/")
    server.addRedirectHandler("/docs", "/swagger/")
    server.addRedirectHandler("/docs/", "/swagger/")
    server.addRedirectHandler("/swagger", "/swagger/")
  }

  private def startBatchChecker(): Unit = {
    val interval = conf.get(KyuubiConf.BATCH_CHECK_INTERVAL)
    val task = new Runnable {
      override def run(): Unit = {
        try {
          sessionManager.getPeerInstanceClosedBatchSessions(connectionUrl).foreach { batch =>
            Utils.tryLogNonFatalError {
              val sessionHandle = SessionHandle.fromUUID(batch.identifier)
              Option(sessionManager.getBatchSessionImpl(sessionHandle)).foreach(_.close())
            }
          }
        } catch {
          case e: Throwable => error("Error checking batch sessions", e)
        }
      }
    }

    batchChecker.scheduleWithFixedDelay(task, interval, interval, TimeUnit.MILLISECONDS)
  }

  @VisibleForTesting
  private[kyuubi] def recoverBatchSessions(): Unit = {
    val recoveryNumThreads = conf.get(METADATA_RECOVERY_THREADS)
    val batchRecoveryExecutor =
      ThreadUtils.newDaemonFixedThreadPool(recoveryNumThreads, "batch-recovery-executor")
    try {
      val batchSessionsToRecover = sessionManager.getBatchSessionsToRecover(connectionUrl)
      val pendingRecoveryTasksCount = new AtomicInteger(0)
      val tasks = batchSessionsToRecover.flatMap { batchSession =>
        val batchId = batchSession.batchJobSubmissionOp.batchId
        try {
          val task: Future[Unit] = batchRecoveryExecutor.submit(() =>
            Utils.tryLogNonFatalError(sessionManager.openBatchSession(batchSession)))
          Some(task -> batchId)
        } catch {
          case e: Throwable =>
            error(s"Error while submitting batch[$batchId] for recovery", e)
            None
        }
      }

      pendingRecoveryTasksCount.addAndGet(tasks.size)

      tasks.foreach { case (task, batchId) =>
        try {
          task.get()
        } catch {
          case e: Throwable =>
            error(s"Error while recovering batch[$batchId]", e)
        } finally {
          val pendingTasks = pendingRecoveryTasksCount.decrementAndGet()
          info(s"Batch[$batchId] recovery task terminated, current pending tasks $pendingTasks")
        }
      }
    } finally {
      ThreadUtils.shutdown(batchRecoveryExecutor)
    }
  }

  override def start(): Unit = synchronized {
    if (!isStarted.get) {
      try {
        server.start()
        recoverBatchSessions()
        isStarted.set(true)
        info(s"$getName has started at ${server.getServerUri}")
        startBatchChecker()
        startInternal()
      } catch {
        case e: Exception => throw new KyuubiException(s"Cannot start $getName", e)
      }
    }
    super.start()
  }

  override def stop(): Unit = synchronized {
    ThreadUtils.shutdown(batchChecker)
    if (isStarted.getAndSet(false)) {
      server.stop()
    }
    super.stop()
  }

  def getRealUser(): String = {
    ServiceUtils.getShortName(
      Option(AuthenticationFilter.getUserName).filter(_.nonEmpty).getOrElse("anonymous"))
  }

  def getSessionUser(hs2ProxyUser: String): String = {
    val sessionConf = Option(hs2ProxyUser).filter(_.nonEmpty).map(proxyUser =>
      Map(KyuubiAuthenticationFactory.HS2_PROXY_USER -> proxyUser)).getOrElse(Map())
    getSessionUser(sessionConf)
  }

  def getSessionUser(sessionConf: Map[String, String]): String = {
    // using the remote ip address instead of that in proxy http header for authentication
    val ipAddress = AuthenticationFilter.getUserIpAddress
    val realUser: String = getRealUser()
    try {
      getProxyUser(sessionConf, ipAddress, realUser)
    } catch {
      case t: Throwable => throw new WebApplicationException(
          t.getMessage,
          Status.METHOD_NOT_ALLOWED)
    }
  }

  def getIpAddress: String = {
    Option(AuthenticationFilter.getUserProxyHeaderIpAddress).getOrElse(
      AuthenticationFilter.getUserIpAddress)
  }

  private def getProxyUser(
      sessionConf: Map[String, String],
      ipAddress: String,
      realUser: String): String = {
    if (sessionConf == null) {
      realUser
    } else {
      sessionConf.get(KyuubiAuthenticationFactory.HS2_PROXY_USER).map { proxyUser =>
        KyuubiAuthenticationFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hadoopConf)
        proxyUser
      }.getOrElse(realUser)
    }
  }

  override val discoveryService: Option[Service] = None
}
