//Output all users' common friends if they have common friends

val input = sc.textFile("/FileStore/tables/dgte0fm01476997410700/test.txt")
val r = input.map(line => line.split("\\t")).filter(line=>(line.length == 2)).flatMap(line =>({
                                                                    val list = line(1).split(",");
                                                                    list.map(x=>{                                                                      
                                                                        if(line(0) < x) ((line(0),x),line(1))
                                                                        else ((x,line(0)),line(1))                                                                
                                                                    })
                                                                    }) )

val common = r.map(x=>((x._1._1.toInt, x._1._2.toInt), x._2.split(",").toSet)).reduceByKey((a,b)=>a.intersect(b)).sortByKey().filter(y=>(y._2.size > 0)).map(x=>(x._1 + "   "+x._2.mkString(",")))
common.collect().foreach(println)