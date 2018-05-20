
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
object AnalysisUsingDataFrames extends App{
  case class Team1Data(match_id:Int,innings_id:Int,batting_team_1:String,total_runs:Int,dismissal:String)
  case class Team2Data(match_id:Int,innings_id:Int,batting_team_2:String,total_runs:Int,dismissal:String)
  case class MatchDetails(match_id:Int,team1:String, team2:String, result:String,dl_applied:Int,winner:String,win_by_runs:Int,win_by_wickets:Int)

    val sparkConf = new SparkConf().setMaster("local").setAppName("Ipl-Data-Analysis-Dataframe")
    val sc =  new SparkContext(sparkConf)
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
    val iplTeam3DF = iplDataWithoutHeaderRDD.filter(line=>line.split(",")(1).toInt==3).map(line=>{
      val temp = line.split(",",-1)
      if(temp(19)==""){
        temp(19)="0"
      }else{
        temp(19)="1"
      }
      Team1Data(temp(0).toInt,temp(1).toInt,temp(2),temp(17).toInt,temp(19))
    }).toDF()
    val iplTeam4DF = iplDataWithoutHeaderRDD.filter(line=>line.split(",")(1).toInt==4).map(line=>{
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
    iplTeam3DF.registerTempTable("team1_Super_Over")
    iplTeam4DF.registerTempTable("team2_Super_Over")
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
   val team3Grouped = sQLContext.sql("select match_id, innings_id, batting_team_1, sum(total_runs) as total_runs_team_1, cast(sum(dismissal) as Int) as wickets_team_1 from team1_Super_Over group by match_id,innings_id,batting_team_1")
   val team4Grouped = sQLContext.sql("select match_id, innings_id, batting_team_2, sum(total_runs) as total_runs_team_2, cast(sum(dismissal) as Int) as wickets_team_2 from team2_Super_Over group by match_id,innings_id,batting_team_2")
   val iplTeamJoinedData = team1grouped.join(team2grouped,team1grouped("match_id")===team2grouped("match_id")).select(team1grouped("match_id"),team1grouped("batting_team_1"),team1grouped("total_runs_team_1"),team1grouped("wickets_team_1"),team2grouped("batting_team_2"),team2grouped("total_runs_team_2"),team2grouped("wickets_team_2"))
   val iplTeamJoinedDataForSuperOver = team3Grouped.join(team4Grouped,team4Grouped("match_id")===team3Grouped("match_id")).select(team3Grouped("match_id"),team3Grouped("batting_team_1"),team3Grouped("total_runs_team_1"),team3Grouped("wickets_team_1"),team4Grouped("batting_team_2"),team4Grouped("total_runs_team_2"),team4Grouped("wickets_team_2"))
   val iplDataJoined = iplTeamJoinedDataForSuperOver.unionAll(iplTeamJoinedData)
   val matchDetails = sc.textFile("D://kaggle_ipl_Data/matches.csv")
   val firstlineMatch = matchDetails.first()
   val matchDataWithoutHeaderRDD = matchDetails.filter(line=>line!=firstlineMatch)
    val matchDF = matchDataWithoutHeaderRDD.map(line=>{
      val temp = line.split(",")
      MatchDetails(temp(0).toInt,temp(4),temp(5),temp(8),temp(9).toInt,temp(10),temp(11).toInt,temp(12).toInt)
    }).toDF()
    val iplTeamWithMatchDetailsJoin = iplDataJoined.join(matchDF,iplDataJoined("match_id")===matchDF("match_id"),"right_outer")
    val result = iplTeamWithMatchDetailsJoin.rdd
    val res = result.map(line=>{
      var winningTeam=""
      var winMargin=""
      if(line.getString(10).equals("no result"))
        {
          winningTeam = "no result"
          winMargin = "no result"
          Row(line.getInt(7).toString,line.getString(8),"no result",line.getString(9),"no result",winningTeam,winMargin)
        }
      else if(line.getString(10).equals("tie"))
        {
          if(line.getLong(2) > line.getLong(5))
            {
              winningTeam = line.getString(1)
              winMargin = (line.getLong(2) - line.getLong(5)).toString+" runs"
              Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,winningTeam,winMargin)
            }else if(line.getLong(2) < line.getLong(5))
            {
              winningTeam = line.getString(4)
              winMargin = (10 - line.getInt(6)).toString+" wickets"
              Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,winningTeam,winMargin)
            }else
          {
            winningTeam = "tie"
            winMargin = "super over"
            Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,winningTeam,winMargin)
          }
        }
      else if(line.getInt(11)==1)
        {
          winMargin = if(line.getInt(13)!=0) line.getInt(13)+" runs" else line.getInt(14)+" wickets"
          winningTeam = line.getString(12)
          Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,winningTeam,winMargin)
        }
      else if(line.getLong(2) > line.getLong(5))
        {
          winningTeam = line.getString(1)
          winMargin = (line.getLong(2) - line.getLong(5)).toString+" runs"
          Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,winningTeam,winMargin)
        }else
        {
          winningTeam = line.getString(4)
          winMargin = (10 - line.getInt(6)).toString+" wickets"
          Row(line.getInt(0).toString,line.getString(1),line.getLong(2).toString+"/"+line.getInt(3).toString,line.getString(4),line.getLong(5).toString+"/"+line.getInt(6).toString,winningTeam,winMargin)
        }
    })
   val schemaRDD = StructType(
     List(
       StructField("match_id",StringType,true),
       StructField("team_1",StringType,true),
       StructField("score_team_1",StringType,true),
       StructField("team_2",StringType,true),
       StructField("score_team_2",StringType,true),
       StructField("Winning_Team",StringType,true),
       StructField("Win_by",StringType,true)
     )
   )
  val resultDF = sQLContext.createDataFrame(res,schemaRDD)
  resultDF.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("D://test236")
  }
