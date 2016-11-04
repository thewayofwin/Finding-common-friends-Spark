//Output the following users with following pair.
//(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)


val input = sc.textFile("/FileStore/tables/dgte0fm01476997410700/test.txt")
val r = input.map(line => line.split("\\t")).filter(line=>(line.length == 2)).flatMap(line =>({
                                                                    val list = line(1).split(",");
                                                                    list.map(x=>{                                                                      
                                                                        if(line(0) < x) ((line(0),x),line(1))
                                                                        else ((x,line(0)),line(1))                                                                
                                                                    })
                                                                    }) )

val common = r.map(x=>((x._1._1.toInt, x._1._2.toInt), x._2.split(",").toSet)).reduceByKey((a,b)=>a.intersect(b)).sortByKey()
val list = common.filter(y=>((y._1._1 == 0 && y._1._2 == 1)||(y._1._1 == 20 && y._1._2 == 28193)||(y._1._1 == 1 && y._1._2 == 29826)||(y._1._1 == 6222 && y._1._2 == 19272)||(y._1._1 == 28041 && y._1._2 == 28056))).map(x=>(x._1 + "   "+x._2.mkString(",")))
list.collect().foreach(println)