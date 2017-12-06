package com.test

import scala.util.Random


class KDTreeBuilder {

  def main(args:Array[String]):Unit={

    val r=new Random()
    r.nextInt(5)
    val data=Array.fill(100,5)(r.nextInt(100))
    val data1 = data.map(_(0)).sortWith((x,y)=> x<y)
    print(data1)

  }

}

class KDTree(left:KDTree,right: KDTree,pTree:KDTree)