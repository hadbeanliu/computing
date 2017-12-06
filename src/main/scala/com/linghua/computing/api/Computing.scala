package com.linghua.computing.api
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

object Computing {

  def main(args:Array[String]):Unit={
    SpringApplication.run(classOf[Config])

  }

}


@SpringBootApplication
class Config
