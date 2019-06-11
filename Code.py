import pandas as pd
import os
import datetime as dt
import numpy as np


os.chdir("C:/Users/mfrangos2016/Desktop/Sybil's Project/Advisor Sheets w grades project")
#has grades & stuff
StudentCourseScheduleData = pd.read_csv("StudentCourseScheduleStudy.csv")
Class_RollsData = pd.read_csv("Class Rolls.csv")
GPAsData = pd.read_csv("Gpas.csv")



#class rolls
filteredClassRollData = pd.DataFrame(Class_RollsData[["REGISTRATION_STATUS_DESC"
                                        ,"TITLE_SHORT_DESC"
                                        ,"COURSE_REFERENCE_NUMBER"
                                        ,"NAME"	
                                        ,"ID"
                                        ,"EMAIL_PREFERRED_ADDRESS"
                                        
                                        ,"SUBJECT"
                                        ,"COURSE_NUMBER" #combine with one below
                                        ,"OFFERING_NUMBER"#combine with one above
                                        
                                        ,"ACADEMIC_PERIOD_ADMITTED"
                                        ,"ACADEMIC_PERIOD"
                                        ,"MAJOR"]])
                            




filteredStudentCourseScheduleData = pd.DataFrame(StudentCourseScheduleData[["ID"
                                                               ,"ACADEMIC_PERIOD"
                                                               ,"COURSE_REFERENCE_NUMBER"
                                                               ,"NAME"
                                                               
                                                               ,"SUBJECT"
                                                               ,"COURSE_NUMBER"           #Same as COURSE_NUMBER & OFFERING_NUMBER
                                                               ,"COURSE_SECTION_NUMBER" #Same as COURSE_NUMBER & OFFERING_NUMBER
                                                               
                                                               ,"FINAL_GRADE"]])


def processData(ItemToProcess):
    #Cleans the course identification column
    filteredStudentCourseScheduleData["COURSE_IDENTIFICATION"] = filteredStudentCourseScheduleData["SUBJECT"].astype(str) + filteredStudentCourseScheduleData["COURSE_NUMBER"].astype(str)
    
    #
    GradesClasses =  pd.DataFrame(filteredStudentCourseScheduleData[["ID", "COURSE_IDENTIFICATION",f"{ItemToProcess}"]])
    
    
    #Rotate the data sideways, aka pivot
    GradesClassesPivot = GradesClasses.pivot(columns = "COURSE_IDENTIFICATION",values = f"{ItemToProcess}")

    GradesClassesPivot["ID"] = filteredStudentCourseScheduleData["ID"]
    #Replace NA
    GradesClassesPivot.fillna(value="", method=None, axis=None, inplace=True, limit=None, downcast=None)
    #Group rows by id
    GradesClassesPivot = GradesClassesPivot.groupby("ID")
    #Merge rows by id
    GradesClassesPivot = GradesClassesPivot.agg(lambda x: x.tolist())
    
    #if ItemToProcess == "FINAL_GRADE":
    #Returns only the unique values, aka grades
    for column in GradesClassesPivot.columns:
        GradesClassesPivot.loc[:][column] = GradesClassesPivot.loc[:][column].apply(set)
    
    #Converts the unique set into a list so we can pop the empty ones
    for column in GradesClassesPivot.columns:
        GradesClassesPivot.loc[:][column] = GradesClassesPivot.loc[:][column].apply(list)
    #else:
    #    pass
            
    #Allows popping of the empty list values.
    def PopFunction(cell):
        try:
            if cell[0] == "":
                cell.pop(0)
                return cell.pop(0)
            elif cell[1] == "": 
                cell.pop(1)
                return cell.pop(1)
            elif cell[2] == "": 
                cell.pop(2)
                return cell.pop(2)
        except:
            return cell
        
            
    #Pop the empty list values. Now only grades remain
    for column in GradesClassesPivot.columns:
        GradesClassesPivot.loc[:][column] = GradesClassesPivot.loc[:][column].apply(PopFunction)
        
    #Unused??
    #ClassNames = filteredStudentCourseScheduleData["COURSE_IDENTIFICATION"].unique()
    #ClassesAndGradesData = filteredStudentCourseScheduleData.pivot(values = "FINAL_GRADE", index="ID", columns = "COURSE_IDENTIFICATION")
    
    #Pulls the data
    StudentNames = pd.DataFrame(filteredStudentCourseScheduleData.loc[:,["NAME","ID"]])
    #Gets rid of duplicates
    StudentNames = StudentNames.drop_duplicates(subset = 'ID', keep = "first")
    
    return pd.merge(GradesClassesPivot.reset_index(),pd.DataFrame(filteredStudentCourseScheduleData.drop_duplicates(subset = 'ID', keep = "first").loc[:,["NAME","ID"]]))

#Generates the Tables
GradesTable = processData("FINAL_GRADE")
SemesterTable = processData("ACADEMIC_PERIOD")

        
        
        
#Clean data by removing lists
GradesTable = GradesTable.applymap(lambda y: np.nan if y in [[], np.nan] else str(y))
#Set the index to id
GradesTable.set_index("ID", drop=True, append=False, inplace=True, verify_integrity=False)

#Clean the table by replacing empty lists with nan
SemesterTable = SemesterTable.applymap(lambda y: np.nan if y in [[], np.nan] else str(y))
#set index to id
SemesterTable.set_index("ID", drop=True, append=False, inplace=True, verify_integrity=False)


#BEGIN MERGING THE TABLES
MergedTable = pd.DataFrame()
i=0
for column in GradesTable.columns[:-1]:
    i = i+1 
    #print("column NUMBER: ",i,GradesTable[column])
    print("FIRSTMERGE",GradesTable.loc[:,column])
    print("SECONDMERGE",SemesterTable.loc[:,column])
    MergedTable[column] = GradesTable[column] + "  |  " +  SemesterTable[column]        
    print("SUCCESS",MergedTable.loc[:,column])
    
MergedTable["NAME"] = GradesTable["NAME"]


#GPA REPORT
filteredGPAsData = GPAsData[["ID", "O   GPA"]]
#set index to prepare merging
filteredGPAsData.set_index("ID", inplace = True)
#Merge GPA Data and Grade Data
MergedTable2 = MergedTable.merge(filteredGPAsData, left_index = True, right_index = True)




EmailAddresses = filteredClassRollData.loc[:,["EMAIL_PREFERRED_ADDRESS","ID"]]
#Gets rid of duplicates
EmailAddresses = EmailAddresses.drop_duplicates(subset = 'ID', keep = "first")
EmailAddresses.set_index("ID", inplace = True)

AcademicPeriodAdmitted = filteredClassRollData.loc[:,["ACADEMIC_PERIOD_ADMITTED","ID"]]        
AcademicPeriodAdmitted = AcademicPeriodAdmitted.drop_duplicates(subset = 'ID', keep = "first")
AcademicPeriodAdmitted.set_index("ID", inplace = True)

MAJORCODE = filteredClassRollData.loc[:,["MAJOR","ID"]]   
MAJORCODE = MAJORCODE.drop_duplicates(subset = 'ID', keep = "first")
MAJORCODE.set_index("ID", inplace = True)


#Merge the emails
MergedTable3 = MergedTable2.merge(EmailAddresses, left_index = True, right_index = True)
#Merge the academic period addmitted
MergedTable4 = MergedTable3.merge(AcademicPeriodAdmitted, left_index = True, right_index = True)
MergedTable5 = MergedTable4.merge(MAJORCODE, left_index = True, right_index = True)





#THIS SECTION IS TO REORDER THE COLUMNS
#Send the column names to a list called cols
cols = MergedTable5.columns.tolist()
#set the arangements for reording the columns. We want the last three columns and then the rest.
cols = cols[-5:] + cols[:-5]
#Finally reorder the columns
MergedTable5 = MergedTable5[cols]
print(cols)

#EXPORT CSV
MergedTable5.to_csv("End Product.csv")




    

#########Executable compilation instructions############
#https://medium.com/dreamcatcher-its-blog/making-an-stand-alone-executable-from-a-python-script-using-pyinstaller-d1df9170e263

#conda install -c conda-forge pyinstaller
#conda install -c anaconda pywin32

#Create Executable:
#Now that, you have installed PyInstaller all you have to do is find the python script that you want to convert to an executable. 
#Just navigate to your python script directory. Now, open up your Terminal/Command Prompt in the script directory

#Run the command on anaconda prompt:
#pyinstaller --onefile <your_script_name>.py
