
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
object AnalysisUsingDataFrames extends App{
    case class Team1Data(match_id:Int,innings_id:Int,batting_team_1:String,total_runs:Int,dismissal:String)

    case class Team2Data(match_id:Int,innings_id:Int,batting_team_2:String,total_runs:Int,dismissal:String)

    val sparkConf = new SparkConf().setMaster("local").setAppName("Ipl-Data-Analysis-Dataframe")
    val sc =  new SparkContext(sparkConf);
    val sQLContext= new SQLContext(sc)
    val iplDataRDD = sc.textFile("file:///D:/kaggle_ipl_Data/deliveries.csv")
    val firstline = iplDataRDD.first()
    val iplDataWithoutHeaderRDD = iplDataRDD.filter(line=>line!=firstline)
    import sQLContext.implicits._
    val iplTeam1DF = iplDataWithoutHeaderRDD.filter(line=>line.split(",")(1).toInt==1).map(line=>{
      val temp = line.split(",",-1)
      if(temp(19)==""){
        temp(19)="0"
        }else{
        temp(19)="1"
      }
      Team1Data(temp(0).toInt,temp(1).toInt,temp(2),temp(17).toInt,temp(19))
    }).toDF()
    val iplTeam2DF = iplDataWithoutHeaderRDD.filter(line=>line.split(",")(1).toInt==2).map(line=>{
      val temp = line.split(",",-1)
      if(temp(19)==""){
        temp(19)="0"
      }else{
        temp(19)="1"
      }
      Team2Data(temp(0).toInt,temp(1).toInt,temp(2),temp(17).toInt,temp(19))
    }).toDF()
    iplTeam1DF.registerTempTable("team1")
    iplTeam2DF.registerTempTable("team2")
    /*
         +--------+----------+-------------------+----------+---------+
         |match_id|innings_id|     batting_team_1|total_runs|dismissal|
         +--------+----------+-------------------+----------+---------+
         |       1|         1|Sunrisers Hyderabad|         0|        0|
         |       1|         1|Sunrisers Hyderabad|         0|        0|
         |       1|         1|Sunrisers Hyderabad|         4|        0|
         |       1|         1|Sunrisers Hyderabad|         0|        0|
         |       1|         1|Sunrisers Hyderabad|         2|        0|
         |       1|         1|Sunrisers Hyderabad|         0|        0|
         |       1|         1|Sunrisers Hyderabad|         1|        0|
         |       1|         1|Sunrisers Hyderabad|         1|        0|
         |       1|         1|Sunrisers Hyderabad|         4|        0|
         |       1|         1|Sunrisers Hyderabad|         1|        0|
         +--------+----------+-------------------+----------+---------+
         only showing top 10 rows

    */
    sQLContext.setConf("spark.sql.shuffle.partitions","3")
    val team1grouped = sQLContext.sql("select match_id, innings_id, batting_team_1, sum(total_runs) as total_runs_team_1, cast(sum(dismissal) as Int) as wickets_team_1 from team1 group by match_id,innings_id,batting_team_1")
    /*
+--------+----------+--------------------+-----------------+--------------+
|match_id|innings_id|      batting_team_1|total_runs_team_1|wickets_team_1|
+--------+----------+--------------------+-----------------+--------------+
|       3|         1|       Gujarat Lions|              183|             4|
|       5|         1|Royal Challengers...|              157|             8|
|       6|         1|       Gujarat Lions|              135|             7|
|       7|         1|Kolkata Knight Ri...|              178|             7|
|       8|         1|Royal Challengers...|              148|             4|
+--------+----------+--------------------+-----------------+--------------+
     */
    val team2grouped = sQLContext.sql("select match_id, innings_id, batting_team_2, sum(total_runs) as total_runs_team_2, cast(sum(dismissal) as Int) as wickets_team_2 from team2 group by match_id,innings_id,batting_team_2")
    /*
+--------+----------+--------------------+-----------------+--------------+
|match_id|innings_id|      batting_team_2|total_runs_team_2|wickets_team_2|
+--------+----------+--------------------+-----------------+--------------+
|       1|         2|Royal Challengers...|              172|            10|
|       3|         2|Kolkata Knight Ri...|              184|             0|
|       4|         2|     Kings XI Punjab|              164|             4|
|      12|         2|      Mumbai Indians|              145|             6|
|      18|         2|Kolkata Knight Ri...|              169|             6|
+--------+----------+--------------------+-----------------+--------------+
     */
    val iplTeamJoinedData = team1grouped.join(team2grouped,team1grouped("match_id")===team2grouped("match_id")).select(team1grouped("match_id"),team1grouped("batting_team_1"),team1grouped("total_runs_team_1"),team1grouped("wickets_team_1"),team2grouped("batting_team_2"),team2grouped("total_runs_team_2"),team2grouped("wickets_team_2"))
    val result = iplTeamJoinedData.rdd
    val res = result.map(line=>{
      if(line.getLong(2) > line.getLong(5))
        {
          Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,(line.getLong(2) - line.getLong(5)).toString+" runs")
        }else
        {
          Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,(10 - line.getInt(6)).toString+" wickets")
        }
    })
   val schemaRDD = StructType(
     List(
       StructField("match_id",StringType,true),
       StructField("team_1",StringType,true),
       StructField("score_team_1",StringType,true),
       StructField("team_2",StringType,true),
       StructField("score_team_2",StringType,true),
       StructField("Win_by",StringType,true)
     )
   )
  val resultDF = sQLContext.createDataFrame(res,schemaRDD)
  resultDF.write.format("com.databricks.spark.csv").option("header","true").save("D://test1.csv")
  }
