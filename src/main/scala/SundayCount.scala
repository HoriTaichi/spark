import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat
import org.apache.spark.{SparkConf, SparkContext}

object SundayCount {

  def main(args: Array[String]) {
    if(args.length < 1){
      throw new IllegalArgumentException(
        "コマンドの引数に日付が記録されたファイルへのパスを入力してください")

    }

    val filePath = args(0)

    val conf = new SparkConf
    val sc = new SparkContext

    try{
      // テキストファイルをロードする
      val textRDD = sc.textFile(filePath)

      // 文字列で表現された日付からDateTimeのインスタンスを生成する
      val dateTimeRDD = textRDD.map { dateStr =>
        val pattern =
          DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }

      // 日曜日だけを抽出する
      val sundayRDD = dateTimeRDD.filter{ dateTime =>
        dateTime.getDayOfWeek == DateTimeConstants.SUNDAY
      }

      // sundayRDDに含まれる日曜日の数を数える
      val numOfSunday = sundayRDD.count
      println(s"与えられたデータの中に日曜日は${numOfSunday}個含まれていました")
    }finally{
      sc.stop()
    }

  }

}
