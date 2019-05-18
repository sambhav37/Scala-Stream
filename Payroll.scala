/*
Final Assignment:

Two type of csv files will be given 
* One is a reference csv with. Only one reference file will be given
    1. salary per working day
    2. employeeId
    3. name of each employee
* In/out files will be placed in a directory. Each file will be given 
    1. employeeId 
    2. In Time in format "dd/MM/yyyy hh:mm:ss a"
    3. Out time in format "dd/MM/yyyy hh:mm:ss a"
* Rules for work time and salary calculation
    * Office time = Out time - In time
    * A day is considered full working day if the office time for the employee is greater than or equal to 8 hours
    * A day is considered half working day if the office time for the employee is between 4 and 8 hours
    * A day is not considered if the office time for the employee is less than 4 hours
    * Saturdays and Sundays are mandatory leaves. Ignore in time/out time if it lies in weekends.
    * Calculation needs to be done for the month of June alone. Ignore if the in/out time is not in June.
    * Ignore if any one of In time/ out time is missing
    * Salary of the employee needs to be calculated as below 
        Number of working days * salary per working day
* Calculate the payroll of each employee
* Order the final list by salary of the employee and write to a seperate CSV


Expected behaviour of the JAR 
    * Should take two arguments
        1. Location of the reference file [Path to the single file]
        2. Location of the entry files [Path to the parent directory where the in/out time csv files will be placed]
    * Should calculate the payroll for the month of June and produce the output in the same directory as the argument 2.
    
-----------------------------------------------------------------------------------
Please give the path of your input directories of Data Enteries and SalaryReferenceFile rest all is generalised.
*/
import scala.io.Source
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import collection.breakOut
import java.io.File
import java.io.PrintWriter
import java.io.BufferedReader
import java.io.FileWriter
import java.io.FileReader

object Payroll1
{
    //global map to store the combine dresult of maps from different EntryData Files
    var combined: Map[String,Double] = Map.empty[String,Double]

    //function to filter the invalid entries with missing arguments
    def BadEntry(str: String): Boolean= {
            val strArr = str.split(",").toArray                
            if(strArr.size==3 && (strArr(1).length()!=0))
                return true
            else
                return false
    }

    //function to filter the weekends
    def Weekends(str: String): Boolean= {
            val strArr = str.split(",").toArray
            val str1 = str.substring(2,12)
            val df = DateTimeFormatter.ofPattern("dd/MM/yyyy")
            val dayOfWeek = (LocalDate.parse(str1,df).getDayOfWeek).toString
            if(dayOfWeek == "MONDAY"||dayOfWeek == "TUESDAY"||dayOfWeek=="WEDNESDAY"||dayOfWeek=="THURSDAY"||dayOfWeek=="FRIDAY")
                return true
            else
                return false
     }

     //function to calculate the duration of work of each employee
     def Differ(str: String): String= {
                    val strArr = str.split(",").toArray

                    //Pattern obtained using LocalDateTime class
                    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss a")
                    val localDateTime1 = LocalDateTime.parse(strArr(1), formatter)
                    val localDateTime2 = LocalDateTime.parse(strArr(2), formatter)

                    //Duration calculated using Duration function under LocalDateTime in Minutes
                    val diffInHours = (java.time.Duration.between(localDateTime1, localDateTime2).toMinutes())
                    
                    //empid concatenated with a (,)
                    val abc=(strArr(0).concat(","))
                    
                    //statements for assigning the value for each day work
                    if(diffInHours >= 480)
                        return(abc.concat("1.0"))
                    else if(diffInHours >= 240 && diffInHours < 480)
                        return(abc.concat("0.5"))
                    else
                        return(abc.concat("0.0"))
            }

    //function to filter the days with no work
    def NoWork(str: String): Boolean= {
                    val strArr = str.split(",").toArray                
                    if(strArr(1).equals("1.0") || strArr(1).equals("0.5"))
                    return true
                    else
                    return false
            }

    //function to add the a sequence for a particular key (list[1,2,3,4,5])
    def sum(list: Seq[Double]): Double = list match {
    case Nil => 0
    case head :: tail => head + sum(tail)
        }

    //function to get the list of files in the given directory
    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
        } else {
        List[File]()
            }
        }      

    //function to merge two maps by converting it into a list
    def combineMap(map2:Map[String,Double])
        {
            var list = combined.toList ++ map2.toList
            combined = list.groupBy ( _._1) .map { case (k,v) => k -> v.map(_._2).sum }
        }      

    //driver method   
    def main(args: Array[String]){  

        //path of the directory
        println("Enter the address of the directory containing EntryData files in the following way (c://sac//savb ) : ")
        val direct = scala.io.StdIn.readLine()

        //path for the salaryReference file
        println("Enter the path for the salary file : ")
        val salaryFile = scala.io.StdIn.readLine()

        val outputFile = direct + "/salaryReference1.csv"

        val files = getListOfFiles(direct)
        //streaming the contents of the file
        files.foreach(e1=>{

            //to store the result in a map
             val frequencyMap = 
        Source.fromFile(e1).getLines
        .filter(element=>element.contains("/06/"))    //filter collecting only entries for the month of june                      
        .filter(BadEntry)
        .filter(Weekends)
        .map(Differ)
        .filter(NoWork)

        //map to convert the string into a key ,value tuple
        .map(e=>{
            val split = e.split(",")
            (split(0),split(1).toDouble)
        })

        //pipeline to merge elements having same key into a list
        .foldLeft(Map.empty[String,Seq[Double]]){case(acc,(k,v))=>acc.updated(k,acc.getOrElse(k,Seq.empty[Double])++Seq(v))}

        //map to find sum of the list
        .map(e=>{
            (e._1, sum(e._2))
        })
        .toMap

        //function call for storing the result for each file in the global Map
        combineMap(frequencyMap)
            })
                        //println(combined)
                        
                        //reading the file
                        val file = new File(salaryFile)
                        val br = new BufferedReader(new FileReader(file))
                        
                        //Storing the first line of file containing the headers
                        val st = br.readLine()

                        //new CSV file for writing the output
                        val writer = new PrintWriter(new File(outputFile))

                        //printing the header
                        writer.write(st)
                        writer.flush()
                        writer.write('\n')
                        writer.flush()
                        br.close()
    
        //file stream for salaryReference.csv
        Source.fromFile(salaryFile).getLines.drop(1)
        
        //map to perform salary calculation on the data obtained from the csv files
        .map(e1=>{
            val strArr = e1.split(",").toArray

            //conditional statement to check the availability of the key
            if(combined.contains(strArr(0)))
            {
                //calculating salary by parsing it into double then parsing again into string 
                strArr(2) = (combined(strArr(0))*(strArr(2).toDouble)).toString
            }

            //string return type
            (strArr(0)+","+strArr(1)+","+strArr(2))
        })

        //filter for removing the Empid Entries for which no data is available
        .filter(element=>element.contains("."))

        //writing contents inside the file
        .foreach(x=>{
            writer.write(x)
            writer.flush()
            writer.write('\n')
            writer.flush()
            })
        println("Payroll Calculated !!!")
    }
}