package com.recommendengine.compute.utils

import java.text.MessageFormat

object GeneratorIdBuild {

  def build(s: String*): String = {

    if (s.length == 3)
      return MessageFormat.format("{1}-{2}-{3}-{4}", s(0), s(1), s(2), System.currentTimeMillis() + "")

    if (s.length == 2)
      return MessageFormat.format("{1}-{2}-{3}", s(0), s(1), System.currentTimeMillis() + "")
      
    MessageFormat.format("{1}", System.currentTimeMillis() + "")
  }

}