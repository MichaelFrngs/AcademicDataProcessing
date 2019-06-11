import tkinter as tk
import requests
from PIL import Image, ImageTk
from tkinter.filedialog import askopenfilename
import pandas as pd
import sys
import os 

def read_the_csv(entry):
    CSVData = pd.DataFrame(pd.read_csv(f"{entry}"))
    print(CSVData)
    return CSVData
    
def read_the_csv2(entry):
    CSVData = pd.DataFrame(pd.read_csv(f"{entry}"))
    print(CSVData)
    return CSVData

def read_the_csv3(entry):
    CSVData = pd.DataFrame(pd.read_csv(f"{entry}"))
    print(CSVData)
    return CSVData

def read_the_csv4(entry):
    CSVData = pd.DataFrame(pd.read_csv(f"{entry}"))
    print(CSVData)   
    return CSVData

def import_csv_data():
    global v
    csv_file_path = askopenfilename()
    print(csv_file_path)
    v.set(csv_file_path)
    #df = pd.read_csv(csv_file_path)
    
def import_csv_data2():
    global w
    csv_file_path = askopenfilename()
    print(csv_file_path)
    w.set(csv_file_path)
    #df = pd.read_csv(csv_file_path)
    
def import_csv_data3():
    global x
    csv_file_path = askopenfilename()
    print(csv_file_path)
    x.set(csv_file_path)
    #df = pd.read_csv(csv_file_path)

def import_csv_data4():
    global y
    csv_file_path = askopenfilename()
    print(csv_file_path)
    y.set(csv_file_path)
    #df = pd.read_csv(csv_file_path)

#PROCESSING #PROCESSING #PROCESSING #PROCESSING #PROCESSING #PROCESSING 
##########################################################################
##########################################################################
#PROCESSING #PROCESSING #PROCESSING #PROCESSING #PROCESSING #PROCESSING 

def main_processing():
    import numpy as np
    
    os.chdir(os.path.abspath(os.path.curdir)) #Make this variable???
    
    
    #has grades & stuff
    StudentCourseScheduleData = read_the_csv(entry.get())
    Class_RollsData = read_the_csv2(entry2.get())
    GPAsData = read_the_csv3(entry3.get())
    
    
    
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
                                            ,"ACADEMIC_PERIOD"]])
    
    
    
    
    
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
        #Returns only the unique values, namely the grades
        for column in GradesClassesPivot.columns:
            GradesClassesPivot.loc[:][column] = GradesClassesPivot.loc[:][column].apply(set)
        
        #Converts the unique set into a list so we can pop the empty ones
        for column in GradesClassesPivot.columns:
            GradesClassesPivot.loc[:][column] = GradesClassesPivot.loc[:][column].apply(list)
        #else:
        #    pass
                
        #removes the empty list values. Keeps the spreadsheet looking clean
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
    for column in GradesTable.columns:
        i = i+1 
        #print("column NUMBER: ",i,GradesTable[column])
        print("FIRSTMERGE",GradesTable.loc[:,column])
        print("SECONDMERGE",SemesterTable.loc[:,column])
        MergedTable[column] = GradesTable[column] + "  |  " +  SemesterTable[column]        
        print("SUCCESS",MergedTable.loc[:,column])
        
    
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
    
    
    #Merge the emails
    MergedTable3 = MergedTable2.merge(EmailAddresses, left_index = True, right_index = True)
    #Merge the academic period addmitted
    MergedTable4 = MergedTable3.merge(AcademicPeriodAdmitted, left_index = True, right_index = True)
    
    
    
    
    
    #THIS SECTION IS TO REORDER THE COLUMNS
    #Send the column names to a list called cols
    cols = MergedTable4.columns.tolist()
    #set the arangements for reording the columns. We want the last three columns and then the rest.
    cols = cols[-4:] + cols[:-4]
    #Finally reorder the columns
    MergedTable4 = MergedTable4[cols]
    print(cols)
    
    #EXPORT CSV
    MergedTable4.to_csv("End Product.csv")
    root.destroy()
    
#PROCESSING SECTION
##########################################################################
##########################################################################
#PROCESSING SECTION

#LET'S BUILD THE GUI

HEIGHT = 500
WIDTH = 600
root = tk.Tk()


tk.Label(root, text='File Path').grid(row=0, column=1)
v = tk.StringVar()
#enter stuff into this text bocks
entry = tk.Entry(root, textvariable=v)
entry.grid(row=1, column=1)

tk.Button(root, text='Set StudentCourseScheduleData',command=import_csv_data).grid(row=1, column=0)




#Exit button
tk.Button(root, text='Close',command=root.destroy).grid(row=6, column=1)

#ROW TWO OF BUTTONS
###############################################################
###############################################################
###############################################################

w = tk.StringVar()
#enter stuff into this text bocks
entry2 = tk.Entry(root, textvariable=w)
entry2.grid(row=2, column=1)
#Import button
tk.Button(root, text='Set Class_RollsData',command=import_csv_data2).grid(row=2, column=0)



#ROW THREE OF BUTTONS
###############################################################
###############################################################
###############################################################
x = tk.StringVar()
#enter stuff into this text bocks
entry3 = tk.Entry(root, textvariable=x)
entry3.grid(row=3, column=1)

tk.Button(root, text='Set GPAsData',command=import_csv_data3).grid(row=3, column=0)







#ROW FOUR OF BUTTONS
###############################################################
###############################################################
###############################################################
y = tk.StringVar()
#enter stuff into this text bocks
entry4 = tk.Entry(root, textvariable=y)
entry4.grid(row=4, column=1)

tk.Button(root, text='RESERVED',command=import_csv_data4).grid(row=4, column=0)





#ROW FIVE OF BUTTONS
###############################################################
###############################################################
###############################################################

button4 = tk.Button(root, text = "Process Data", bg= "white", fg = "blue", command = lambda: main_processing())
button4.grid(row=5, column=1)



#End of application. 
root.mainloop()



#EXECUTABLE COMPILATION INSTRUCTIONS

#pyinstaller --onefile <your_script_name>.py --exclude-module PyQt5

#conda install -c conda-forge pyinstaller
#conda install -c anaconda pywin32







