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

package org.apache.livy.server.interactive

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.net.URI
import java.util
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Options.{CreateOpts, Rename}
import scala.util.control.NonFatal

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.Utils.usingResource
import org.apache.livy.sessions.SessionKindModule

class StatementStore(livyConf: LivyConf) extends Logging {
  protected val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .registerModule(new SessionKindModule())

  def serializeToBytes(value: Object): Array[Byte] = mapper.writeValueAsBytes(value)

  private val fsUri = {
    val fsPath = livyConf.get(LivyConf.STATEMENT_STORE)
    require(fsPath != null && !fsPath.isEmpty,
      s"Please config ${LivyConf.STATEMENT_STORE.key}.")
    new URI(fsPath)
  }
  //  private val fileContext: FileContext = mockFileContext.getOrElse {
  //    FileContext.getFileContext(fsUri)
  //  }

  //  {
  //    // Only Livy user should have access to state files.
  //    fileContext.setUMask(new FsPermission("077"))
  //
  //    // Create state store dir if it doesn't exist.
  //    val stateStorePath = absPath(".")
  //    try {
  //      fileContext.mkdir(stateStorePath, FsPermission.getDirDefault(), true)
  //    } catch {
  //      case _: FileAlreadyExistsException =>
  //        if (!fileContext.getFileStatus(stateStorePath).isDirectory()) {
  //          throw new IOException(s"$stateStorePath is not a directory.")
  //        }
  //    }
  //
  //    // Check permission of state store dir.
  //    val fileStatus = fileContext.getFileStatus(absPath("."))
  //    require(fileStatus.getPermission.getUserAction() == FsAction.ALL,
  //      s"Livy doesn't have permission to access state store: $fsUri.")
  //    if (fileStatus.getPermission.getGroupAction != FsAction.NONE) {
  //      warn(s"Group users have permission to access state store: $fsUri. This is insecure.")
  //    }
  //    if (fileStatus.getPermission.getOtherAction != FsAction.NONE) {
  //      warn(s"Other users have permission to access state store: $fsUri. This is in secure.")
  //    }
  //  }

  def set(key: String, value: Object): Unit = {
    val fileContext = FileContext.getFileContext(fsUri)
    // Write to a temp file then rename to avoid file corruption if livy-server crashes
    // in the middle of the write.
    val tmpPath = absPath(s"$key.tmp")
    val createFlag = util.EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)

    // usingResource(fileContext.create(tmpPath, createFlag, CreateOpts.createParent())) { tmpFile =>
    try {
      val tmpFile = fileContext.create(tmpPath, createFlag, CreateOpts.createParent())
      tmpFile.write(serializeToBytes(value))
      tmpFile.close()
      // Assume rename is atomic.
      fileContext.rename(tmpPath, absPath(fileName), Rename.OVERWRITE)
    } catch {
      case e: Exception => logger.error("Exception ", e)
    }


    // tmpFile.write(serializeToBytes(value))
    // tmpFile.close()
    // // Assume rename is atomic.
    // fileContext.rename(tmpPath, absPath(key), Rename.OVERWRITE)
    // }

    try {
      val crcPath = new Path(tmpPath.getParent, s".${tmpPath.getName}.crc")
      fileContext.delete(crcPath, false)
    } catch {
      case NonFatal(e) => // Swallow the exception.
    }
  }

  private def absPath(key: String): Path = new Path(fsUri.getPath(), key)
}
