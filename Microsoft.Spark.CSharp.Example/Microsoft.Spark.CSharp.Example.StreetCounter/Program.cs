using System;
using Microsoft.Spark.Sql;
namespace Microsoft.Spark.CSharp.Example.StreetCounter
{
    class Program
    { 
        static void Main(string[] args)
        {
            Console.WriteLine("Start SparkSession");
            SparkSession sparkSession = SparkSession.Builder().AppName("Street Counter").GetOrCreate();
            DataFrame dfCsv =
                sparkSession
                    .Read()
                    .Option("delimiter", ";")
                    .Schema("WOJ string ,POW string ,GMI string ,RODZ_GMI string , " +
                            "SYM string , SYM_UL string , " +
                            "CECHA string , NAZWA_1 string ,NAZWA_2 string , " +
                            "STAN_NA string")
                    .Csv("streets.csv");
            DataFrame dataIn = dfCsv
                .WithColumn("STREET", Functions.ConcatWs(" ", dfCsv["CECHA"], dfCsv["NAZWA_1"], dfCsv["NAZWA_2"]));
            DataFrame dataGroup = dataIn
                .Select("STREET")
                .GroupBy("STREET")
                .Count()
                .WithColumnRenamed("count","COUNT");
            DataFrame dataOut = dataGroup
                .OrderBy(dataGroup["COUNT"]
                    .Desc()
                );
            dataOut
                .Coalesce(1)
                .Write()
                .Option("delimiter",";")
                .Csv("result");
            sparkSession.Stop();
            Console.WriteLine("Stop SparkSession");
        }
    }
}