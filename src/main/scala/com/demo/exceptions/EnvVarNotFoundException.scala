package com.demo.exceptions

class EnvVarNotFoundException(message: String = null, cause: Throwable = null) extends
  RuntimeException(message, cause)