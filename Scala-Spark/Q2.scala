val userA = "0"
val userB = "1"
val input = sc.textFile("/FileStore/tables/dgte0fm01476997410700/test.txt")
val A_f = input.map(li=>li.split("\\t")).filter(li => (li.size == 2)).filter(li =>(userB == li(0))).flatMap(li=>li(1).split(",").toSet).map(_.toInt).collect()
val B_f = input.map(li=>li.split("\\t")).filter(li => (li.size == 2)).filter(li =>(userA == li(0))).flatMap(li=>li(1).split(",").toSet).map(_.toInt).collect()
val common = userA + ", " + userB + "\t" + "(" + A_f.intersect(B_f).sorted.mkString(",") + ")"
println(common)
