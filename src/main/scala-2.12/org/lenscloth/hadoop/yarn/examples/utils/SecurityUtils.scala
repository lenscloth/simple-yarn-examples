package org.lenscloth.hadoop.yarn.examples.utils

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.yarn.conf.YarnConfiguration

object SecurityUtils {

  def loadHDFSCredential(fs: FileSystem, conf: Configuration, cred: Credentials): Unit = {
    val renewer = conf.get(YarnConfiguration.RM_PRINCIPAL)
    fs.addDelegationTokens(renewer, cred)
  }

  def wrapToByteBuffer(credentials: Credentials): ByteBuffer = {
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    ByteBuffer.wrap(dob.getData, 0, dob.getLength)
  }
}
