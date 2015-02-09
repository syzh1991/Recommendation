/**
 * Created by zhang on 2015/1/31.
 */
object Test {
  def main(args: Array[String]) {
    val str = "a,a b,b c,c d,d".split(" ")
    println(str.map(s=>s.split(",")(0)).mkString)
  }
}
