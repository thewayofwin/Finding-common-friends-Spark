val userA = "0"
val userB = "1"
val input = sc.textFile("/FileStore/tables/dgte0fm01476997410700/test.txt")
val A_f = input.map(li=>li.split("\\t")).filter(li => (li.size == 2)).filter(li =>(userB == li(0))).flatMap(li=>li(1).split(",")).map(_.toInt)
val B_f = input.map(li=>li.split("\\t")).filter(li => (li.size == 2)).filter(li =>(userA == li(0))).flatMap(li=>li(1).split(",")).map(_.toInt)
val common = A_f.intersection(B_f).collect().sorted.mkString(",")
//println(common)

val userdata = sc.textFile("/FileStore/tables/6gkudkd11477086406965/userdata.txt").cache()
val Detail = userdata.map(li=>li.split(",")).filter(li=>common.split(",").contains(li(0))).map(line => ((line(1)+ ":" + line(9))))
val result = userA+" "+userB+"\t["+Detail.collect.mkString(", ")+"]"
println(result)