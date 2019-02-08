package com.github.jarlakxen.drunk

class NonOkHttpCodeException(code: Int, body: String) extends Exception(s"Received http code: $code with body: $body")