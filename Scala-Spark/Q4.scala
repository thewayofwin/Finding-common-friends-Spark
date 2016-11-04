import java.util.Calendar

val now = Calendar.getInstance()

def cal_age(DOB: String) : Float ={
    val date = DOB.split("/")
    val Year = now.get(Calendar.YEAR)
    val Month = now.get(Calendar.MONTH)
    val Day = now.get(Calendar.DATE);
    var age = Year - date(2).toInt -1
    if(date(0).toInt < Month){
      age += 1
    }
    if(date(0).toInt == Month){
       if(date(1).toInt <= Day){
         age += 1 
       }
    }
    return age.toFloat
}


val input = sc.textFile("/FileStore/tables/dgte0fm01476997410700/test.txt")
val r = input.map(line => line.split("\\t")).filter(line=>(line.length == 2)).flatMap(line =>(line(1).split(",").map(x=> (x, line(0).toInt)  )))
val userdata = sc.textFile("/FileStore/tables/6gkudkd11477086406965/userdata.txt")
val age = userdata.map(li=>li.split(",")).map(line => (  line(0),  line(9) )).map(x=>(x._1,  cal_age(x._2)   )  )

val maxAge = r.join(age).map(line=>(line._2._1,line._2._2)).reduceByKey((a,b)=>Math.max(a,b)).sortByKey()
val sortByFriendAge = maxAge.sortBy(-_._2)

val address = userData.map(line => line.split(",")).map(line => (line(0).toInt,(line(3),line(4),line(5))))
val Top10 = sc.parallelize(sortByFriendAge.take(10))
val joinTop10 = Top10.join(address).sortByKey().map(line=>(line._1,(line._2._2._1,line._2._2._2,line._2._2._3), line._2._1))

joinTop10.collect().foreach(println)

