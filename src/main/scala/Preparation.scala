import Preparation.{traitementEntete, traitementId}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Preparation {



  def structureInitiale(sc:SparkContext,input:String, separateur:String, entete:String, ID:String): RDD[(String,String)] ={

    var rdd = sc.textFile(input).filter(x=> (!x.contains("NULL")))

    /***Traitement du separateur s'il differe de l'espace*/
    rdd=rdd.map(x => x.replaceAll(separateur," ").trim.replaceAll("\t"," "))

    if(entete==1) {
      rdd = traitementEntete(rdd)
    }
    /***Traitement des identifiants d'instances s'ils éxistent*/
    if(ID==1) {
      rdd = rdd.map( x=> traitementId(x) )
    }



   val ensembleStructure = rdd
      .filter(x=> (!x.contains("NULL")))
      .zipWithIndex()
      .map (x => structurerEnsemble(x._2,x._1.split(" ")) )//.zipWithIndex()
      .flatMap(x=> x.split(","))
      .map(x=> (x.split("#")(1),x.split("#")(0)))
      .reduceByKey(_+","+_).cache()



    return ensembleStructure
  }

  def traitementEntete(rdd:RDD[String]): RDD[String] ={
    val result = rdd.mapPartitionsWithIndex(
      (i,iterator)=>
        if(i==0 && iterator.hasNext){
          iterator.next
          iterator}
        else iterator)
    return result
  }


  def traitementId(instance:String): String ={
    var resultat=""
    val inst:List[String] = instance.split(" ").toList
    for(i<-1 to inst.size-1){
      if(resultat.equals("")){
        resultat=inst(i)
      }else{
        resultat=resultat+" "+inst(i)
      }
    }
    return resultat
  }


  def structurerEnsemble(ligne:Long,array: Array[String]): String = {
    var result =""
    for(i<-0 to array.size-2){
      if(result.equals("")){
        result = ligne+"::"+array(i)+"_"+array(array.size-1)+"#"+i
      }else{
        result = result+","+ligne+"::"+array(i)+"_"+array(array.size-1)+"#"+i
      }
    }
    return result
  }



}
