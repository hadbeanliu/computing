package com.test

import com.recommendengine.compute.utils.TextSplit
import com.huaban.analysis.jieba.JiebaSegmenter
import com.chenlb.mmseg4j.example.Complex
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import com.huaban.analysis.jieba.SegToken


object WordSplit {
  
  //京,多,安,希望,健康,出战,新,赛季,曼,城,球迷,可能,是,今夏,转,会,市场,上,过得,最,开心,的,了,球队,已经,签,下了,b,席,尔,瓦,和,埃,德,松,两位,强,援,引,援,进展,神速,而,另,一边,还有,好消息,京,多,安,已经,开始,上,强度,的,训练,了,他,希望,为,新,赛季,开始,前,满,血,复活,帮助,球队,京,多,安,是在,去年,12,月,曼,城,面对,沃,特,福,德,的,比赛,中,受伤,的,此后,他,就,赛季,报销,了,如今,德国人,终于,开始,正常,训练,了,太阳报,晒,出了,不少,京,多,安,独自,训练,的,图片,看起来,他的,恢复,状况,还是,不错,的,曼,城,球迷,也,对此,非常,乐观,他们,认为,新,赛季,开始时,京,多,安可,以,以,完全,健康,的,姿态,出战,了,京,多,安,独自,训练,曼,城,花费,2000,万,镑,从,多,特,蒙,德,买下,了,京,多,安,瓜,迪,奥,拉,对,他,期待,非常,高,不过,伤病,让,京,多,安,的,英,超,处,子,季,过得,并不,愉快,在他,受伤,后,曼,城,的,中场,也,经历,了,一段时间,的,挣扎,新,赛季,京,多,安,如果,能够,保持,健康,对于,曼,城,而言,无疑,有着,非常重,要的,意义

  def main(args: Array[String]): Unit = {
    
    val txt="京多安希望健康出战新赛季曼城球迷可能是今夏转会市场上过得最开心的了，球队已经签下了 B- 席尔瓦和埃德松两位强援，引援进展神速。而另一边，还有好消息，京多安已经开始上强度的训练了，他希望为新赛季开始前满血复活帮助球队。京多安是在去年 12 月曼城面对沃特福德的比赛中受伤的，此后他就赛季报销了。如今德国人终于开始正常训练了。《太阳报》晒出了不少京多安独自训练的图片，看起来他的恢复状况还是不错的，曼城球迷也对此非常乐观。他们认为新赛季开始时，京多安可以以完全健康的姿态出战了。 京多安独自训练曼城花费 2000 万镑从多特蒙德买下了京多安，瓜迪奥拉对他期待非常高。不过伤病让京多安的英超处子季过得并不愉快。在他受伤后，曼城的中场也经历了一段时间的挣扎。新赛季，京多安如果能够保持健康，对于曼城而言无疑有着非常重要的意义。"
    //    val split =TextSplit
//    val result=split.process(text)
    
    val jieba=new JiebaSegmenter
    import collection.JavaConversions._
    println(System.currentTimeMillis())
//    for(i<- 0 until 100)
    jieba.process(txt, SegMode.INDEX)
     for( str<-jieba.process(txt, SegMode.SEARCH).toList){
       println(str.toString())
     }
    println(System.currentTimeMillis())
    
    val seg=new Complex
    println(System.currentTimeMillis())
//    for(i<- 0 until 100)
      println(seg.segWords(txt, ","));
    println(System.currentTimeMillis())
    
  }
}