package com.github.jarlakxen.drunk

class NonOkHttpCodeException(val code: Int, val body: String) extends Exception(s"Received http code: $code with body: $body")