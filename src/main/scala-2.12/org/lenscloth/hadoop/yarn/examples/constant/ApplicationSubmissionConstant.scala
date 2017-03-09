package org.lenscloth.hadoop.yarn.examples.constant

object ApplicationSubmissionConstant {
  val defaultMaxAttempt: Int = 3
  val defaultAttemptFailureValidityInterval: Long = 1000 * 3600

  val defaultQueue: String = "default"

  val defaultMemory : Int = 1024
  val defaultCore: Int = 1

  val defaultKeepContainerAcrossApplicationAttempts: Boolean = true
}
